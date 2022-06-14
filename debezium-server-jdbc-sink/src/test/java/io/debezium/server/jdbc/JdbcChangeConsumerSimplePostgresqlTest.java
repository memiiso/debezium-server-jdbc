/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc;

import io.debezium.server.jdbc.testresources.BaseDbTest;
import io.debezium.server.jdbc.testresources.TargetPostgresqlDB;
import io.debezium.server.jdbc.testresources.TestChangeEvent;
import io.debezium.server.jdbc.testresources.TestUtil;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

import org.junit.jupiter.api.Test;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TargetPostgresqlDB.class)
public class JdbcChangeConsumerSimplePostgresqlTest extends BaseDbTest {
  @Inject
  JdbcChangeConsumer consumer;

  @Test
  public void testSimpleUpload() throws Exception {

    String dest = "inventory.customers_append";
    List<io.debezium.engine.ChangeEvent<Object, Object>> records = new ArrayList<>();
    records.add(TestChangeEvent.of(dest, 1, "c"));
    records.add(TestChangeEvent.of(dest, 2, "c"));
    records.add(TestChangeEvent.of(dest, 3, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());
    // check that its consumed!
    // 3 records should be updated 4th one should be inserted
    records.clear();
    records.add(TestChangeEvent.of(dest, 1, "r"));
    records.add(TestChangeEvent.of(dest, 2, "d"));
    records.add(TestChangeEvent.of(dest, 3, "u", "UpdatednameV1"));
    records.add(TestChangeEvent.of(dest, 4, "c"));
    consumer.handleBatch(records, TestUtil.getCommitter());
  }
}
