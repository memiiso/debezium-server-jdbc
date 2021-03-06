/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.jdbc.batchsizewait.InterfaceBatchSizeWait;
import io.debezium.server.jdbc.jdbi.ArrayListCodec;
import io.debezium.server.jdbc.jdbi.LinkedHashMapCodec;
import io.debezium.server.jdbc.relational.BaseTableWriter;
import io.debezium.server.jdbc.relational.RelationalTable;
import io.debezium.server.jdbc.relational.TableNotFoundException;
import io.debezium.server.jdbc.relational.TableWriterFactory;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.codec.CodecFactory;
import org.jdbi.v3.core.qualifier.QualifiedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages to jdbc database tables.
 *
 * @author Ismail Simsek
 */
@Named("jdbc")
@Dependent
public class JdbcChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  public static final ObjectMapper mapper = new ObjectMapper();
  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeConsumer.class);
  static Deserializer<JsonNode> valDeserializer;
  static Deserializer<JsonNode> keyDeserializer;
  protected final Clock clock = Clock.system();
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  @ConfigProperty(name = "debezium.sink.jdbc.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.jdbc.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  public Jdbi jdbi;
  @ConfigProperty(name = "debezium.sink.jdbc.table-prefix", defaultValue = "")
  String tablePrefix;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;
  @ConfigProperty(name = "debezium.format.value.schemas.enable", defaultValue = "false")
  boolean eventSchemaEnabled;
  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;
  @Inject
  TableWriterFactory tableWriterFactory;
  BaseTableWriter tableWriter;
  @ConfigProperty(name = "debezium.sink.jdbc.database.schema", defaultValue = "debezium")
  String targetSchema;
  @ConfigProperty(name = "debezium.sink.jdbc.database.url")
  String url;
  @ConfigProperty(name = "debezium.sink.jdbc.database.username")
  String username;
  @ConfigProperty(name = "debezium.sink.jdbc.database.password")
  String password;

  @ConfigProperty(name = "debezium.sink.jdbc.upsert", defaultValue = "true")
  boolean upsert;

  @PostConstruct
  void connect() throws Exception {
    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    batchSizeWait = JdbcUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
    batchSizeWait.initizalize();

    Properties properties = new Properties();
    Map<String, String> conf = JdbcUtil.getConfigSubset(ConfigProvider.getConfig(), "debezium.sink.jdbc.database" +
                                                                                    ".param.");
    properties.putAll(conf);
    LOGGER.trace("Jdbc Properties: {}", properties);
    LOGGER.trace("Jdbc url {}", url);
    LOGGER.trace("Jdbc username {}", username);
    BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(properties);
    dataSource.setUrl(url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);

    jdbi = Jdbi.create(dataSource);
    jdbi.registerCodecFactory(
        CodecFactory.forSingleCodec(QualifiedType.of(LinkedHashMap.class), new LinkedHashMapCodec()));
    jdbi.registerCodecFactory(
        CodecFactory.forSingleCodec(QualifiedType.of(ArrayList.class), new ArrayListCodec()));

    tableWriter = tableWriterFactory.get(jdbi);

    // configure and set 
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    // configure and set 
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();
  }

  public RelationalTable getJdbcTable(String tableName, JdbcChangeEvent.Schema schema) throws DebeziumException {
    RelationalTable t;
    try {
      try (Handle handle = jdbi.open()) {
        t = new RelationalTable(targetSchema, tableName, handle.getConnection());
      }
    } catch (TableNotFoundException e) {
      //t = createTable;
      if (!eventSchemaEnabled) {
        throw new RuntimeException("RelationalTable '" + tableName + "' not found! Set `debezium.format.value.schemas.enable` to" +
                                   " true to create tables automatically!");
      }
      // create table
      try (Handle handle = jdbi.open()) {
        LOGGER.debug("Target table not found creating it!");
        JdbcUtil.createTable(targetSchema, tableName, handle.getConnection(), schema, upsert);
      }
      // get table after reading it
      try (Handle handle = jdbi.open()) {
        t = new RelationalTable(targetSchema, tableName, handle.getConnection());
      }
    }

    return t;
  }

  /**
   * @param numUploadedEvents periodically log number of events consumed
   */
  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    //group events by destination
    Map<String, List<JdbcChangeEvent>> result =
        records.stream()
            .map((ChangeEvent<Object, Object> e)
                -> {
              try {
                return new JdbcChangeEvent(e.destination(),
                    valDeserializer.deserialize(e.destination(), getBytes(e.value())),
                    e.key() == null ? null : valDeserializer.deserialize(e.destination(), getBytes(e.key())),
                    mapper.readTree(getBytes(e.value())).get("schema"),
                    e.key() == null ? null : mapper.readTree(getBytes(e.key())).get("schema")
                );
              } catch (IOException ex) {
                throw new DebeziumException(ex);
              }
            })
            .collect(Collectors.groupingBy(JdbcChangeEvent::destination));

    // consume list of events for each destination table
    for (Map.Entry<String, List<JdbcChangeEvent>> tableEvents : result.entrySet()) {
      RelationalTable tbl = this.getJdbcTable(mapDestination(tableEvents.getKey()), tableEvents.getValue().get(0).schema());
      tableWriter.addToTable(tbl, tableEvents.getValue());
    }

    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even it's should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(records.size());

    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());
  }

  public String mapDestination(String destination) {
    final String tableName = destination
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");
    return tablePrefix + tableName;
  }
}
