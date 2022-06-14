/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc.jdbi;

import io.debezium.DebeziumException;
import io.debezium.server.jdbc.JdbcChangeConsumer;

import java.util.LinkedHashMap;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.codec.Codec;
import org.jdbi.v3.core.mapper.ColumnMapper;

/**
 * Codec to persist a LinkedHashMap to the database and restore it back.
 */
public class LinkedHashMapCodec implements Codec<LinkedHashMap<?, ?>> {

  @Override
  public ColumnMapper<LinkedHashMap<?, ?>> getColumnMapper() {
    return (r, idx, ctx) -> {
      try {
        return JdbcChangeConsumer.mapper.readValue(r.getString(idx), LinkedHashMap.class);
      } catch (JsonProcessingException e) {
        throw new DebeziumException(e);
      }
    };
  }

  @Override
  public Function<LinkedHashMap<?, ?>, Argument> getArgumentFunction() {
    return value -> (idx, stmt, ctx) -> {
      try {
        stmt.setString(idx, JdbcChangeConsumer.mapper.writeValueAsString(value));
      } catch (JsonProcessingException e) {
        throw new DebeziumException(e);
      }
    };
  }
}

