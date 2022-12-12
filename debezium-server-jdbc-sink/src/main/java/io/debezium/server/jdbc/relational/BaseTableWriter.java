/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc.relational;

import io.debezium.server.jdbc.JdbcChangeEvent;

import java.util.Arrays;
import java.util.List;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseTableWriter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTableWriter.class);
  final Jdbi jdbi;
  final String identifierQuoteCharacter;

  public BaseTableWriter(final Jdbi jdbi, String identifierQuoteCharacter) {
    this.jdbi = jdbi;
    this.identifierQuoteCharacter = identifierQuoteCharacter;
  }

  public void addToTable(final RelationalTable table, final List<JdbcChangeEvent> events) {
    final String sql = table.preparedInsertStatement(this.identifierQuoteCharacter);
    int inserts = jdbi.withHandle(handle -> {
      PreparedBatch b = handle.prepareBatch(sql);
      events.forEach(e -> b.add(e.valueAsMap()));
      return Arrays.stream(b.execute()).sum();
    });
  }

}
