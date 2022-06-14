package io.debezium.server.jdbc.relational;

import javax.enterprise.context.Dependent;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jdbi.v3.core.Jdbi;

@Dependent
public class TableWriterFactory {
  @ConfigProperty(name = "debezium.sink.jdbc.upsert", defaultValue = "true")
  boolean upsert;
  @ConfigProperty(name = "debezium.sink.jdbc.upsert-keep-deletes", defaultValue = "true")
  boolean upsertKeepDeletes;

  public BaseTableWriter get(final Jdbi jdbi) {
    if (upsert) {
      return new UpsertTableWriter(jdbi, upsertKeepDeletes);
    } else {
      return new AppendTableWriter(jdbi);
    }
  }
}
