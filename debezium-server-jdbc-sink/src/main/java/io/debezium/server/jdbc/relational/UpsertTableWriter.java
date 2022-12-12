/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc.relational;

import io.debezium.server.jdbc.JdbcChangeEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;

public class UpsertTableWriter extends BaseTableWriter {
  static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
  private final AppendTableWriter appendTableWriter;
  final String sourceTsMsColumn = "__source_ts_ms";
  final String opColumn = "__op";
  final boolean upsertKeepDeletes;

  public UpsertTableWriter(Jdbi jdbi, String identifierQuoteCharacter, boolean upsertKeepDeletes) {
    super(jdbi, identifierQuoteCharacter);
    this.upsertKeepDeletes = upsertKeepDeletes;
    appendTableWriter = new AppendTableWriter(jdbi, identifierQuoteCharacter);
  }

  @Override
  public void addToTable(final RelationalTable table, final List<JdbcChangeEvent> events) {
    if (table.hasPK()) {
      this.deleteInsert(table, deduplicateBatch(events));
    } else {
      // log message
      appendTableWriter.addToTable(table, events);
    }
  }

  public void deleteInsert(final RelationalTable table, final List<JdbcChangeEvent> events) {

    int inserts = jdbi.withHandle(handle -> {
      handle.begin(); // USE SINGLE TRANSACTION
      PreparedBatch delete = handle.prepareBatch(table.preparedDeleteStatement(this.identifierQuoteCharacter));
      PreparedBatch insert = handle.prepareBatch(table.preparedInsertStatement(this.identifierQuoteCharacter));

      for (JdbcChangeEvent row : events) {
        // if its deleted row and upsertKeepDeletes = true then add deleted record to target table
        // else deleted records are deleted from target table
        if (upsertKeepDeletes || !(row.operation().equals("d"))) {// anything which not an insert is upsert
          insert.add(row.valueAsMap());
        }

        if (!row.operation().equals("c")) { // anything which not an insert is upsert
          delete.add(row.keyAsMap());
        }
      }

      int[] deleted = delete.execute();
      int[] inserted = insert.execute();
      handle.commit();
      return Arrays.stream(deleted).sum() + Arrays.stream(inserted).sum();
    });
  }

  private List<JdbcChangeEvent> deduplicateBatch(List<JdbcChangeEvent> events) {

    ConcurrentHashMap<JsonNode, JdbcChangeEvent> deduplicated = new ConcurrentHashMap<>();

    for (JdbcChangeEvent e : events) {

      if (deduplicated.containsKey(e.key())) {

        // replace it if it's new
        if (this.compareByTsThenOp(deduplicated.get(e.key()).value(), e.value()) <= 0) {
          deduplicated.put(e.key(), e);
        }

      } else {
        deduplicated.put(e.key(), e);
      }

    }
    return new ArrayList<>(deduplicated.values());
  }

  private int compareByTsThenOp(JsonNode lhs, JsonNode rhs) {

    int result = Long.compare(lhs.get(sourceTsMsColumn).asLong(0), rhs.get(sourceTsMsColumn).asLong(0));

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result = cdcOperations.getOrDefault(lhs.get(opColumn).asText("c"), -1)
          .compareTo(
              cdcOperations.getOrDefault(rhs.get(opColumn).asText("c"), -1)
          );
    }

    return result;
  }

}
