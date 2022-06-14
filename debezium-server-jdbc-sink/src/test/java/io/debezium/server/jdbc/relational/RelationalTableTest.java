/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc.relational;

import io.debezium.DebeziumException;
import io.debezium.server.jdbc.jdbi.LinkedHashMapCodec;
import io.debezium.server.jdbc.testresources.TargetPostgresqlDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.assertions.Assertions;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.codec.CodecFactory;
import org.jdbi.v3.core.qualifier.QualifiedType;
import org.jdbi.v3.core.statement.Update;
import org.jooq.CreateTableColumnStep;
import org.jooq.CreateTableConstraintStep;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static io.debezium.server.jdbc.JdbcChangeConsumer.mapper;
import static org.jooq.impl.SQLDataType.*;

class RelationalTableTest {
  public static TargetPostgresqlDB db = new TargetPostgresqlDB();
  public static Jdbi jdbi;

  @BeforeAll
  static void beforeAll() throws Exception {
    db.start();
    // Jdbi
    BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(new Properties());
    dataSource.setUrl(db.container.getJdbcUrl());
    dataSource.setUsername(db.container.getUsername());
    dataSource.setPassword(db.container.getPassword());
    jdbi = Jdbi.create(dataSource);
    jdbi.registerCodecFactory(
        CodecFactory.forSingleCodec(QualifiedType.of(LinkedHashMap.class), new LinkedHashMapCodec()));

    // CREATE TES TABLE USING JOOQ
    try (Connection conn = DriverManager.getConnection(
        db.container.getJdbcUrl(),
        db.container.getUsername(),
        db.container.getPassword())) {
      DSLContext create = DSL.using(conn);

      try (CreateTableConstraintStep sql = create.createTable("tbl_with_pk")
          .column("id", BIGINT)
          .column("coll1", LONGNVARCHAR)
          .column("coll2", NUMERIC)
          .column("coll3", FLOAT)
          .column("coll4", DECIMAL)
          .primaryKey("id", "coll1")) {
        sql.execute();
      }

      try (CreateTableColumnStep sql = create.createTable("tbl_without_pk")
          .column("id", BIGINT)
          .column("coll1", LONGNVARCHAR)
          .column("coll2", NUMERIC)
          .column("coll3", DATE)
          .column("coll4", DECIMAL)) {
        sql.execute();
      }
    }
  }

  @Test
  void complexTypeBinding() {
    String withPK = "INSERT INTO tbl_with_pk (id, coll1) values (:id, :coll1) ";
    try (Handle handle = jdbi.open()) {
      LinkedHashMap<Integer, String> testhashmap = new LinkedHashMap<>();
      testhashmap.put(100, "Amit");
      System.out.println("DATA=" + mapper.writeValueAsString(testhashmap));
      Update query = handle.createUpdate(withPK).
          bind("id", 1).
          bind("coll1", testhashmap);
      int rows = query.execute();
      System.out.println("Rows inserted=" + rows);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }


  @Test
  void experiment() {
    try (Handle handle = jdbi.open()) {
      handle.createQuery("select * from tbl_with_pk");
    }
  }

  @Test
  void hasPK() {
    try (Handle handle = jdbi.open()) {
      RelationalTable tbl_without_pk = new RelationalTable("public", "tbl_without_pk", handle.getConnection());
      RelationalTable tbl_with_pk = new RelationalTable("public", "tbl_with_pk", handle.getConnection());
      Assertions.assertTrue(tbl_with_pk.hasPK());
      Assertions.assertFalse(tbl_without_pk.hasPK());
    }
  }

  @Test
  void preparedInsertStatement() {
    String withPK = "INSERT INTO tbl_with_pk \n" +
                    "(coll3 , coll2 , coll1 , id , coll4 ) \n" +
                    "VALUES (:coll3, :coll2, :coll1, :id, :coll4)";
    String withoutPK = "INSERT INTO tbl_without_pk \n" +
                       "(coll3 , coll2 , coll1 , id , coll4 ) \n" +
                       "VALUES (:coll3, :coll2, :coll1, :id, :coll4)";
    try (Handle handle = jdbi.open()) {
      RelationalTable tbl_without_pk = new RelationalTable("public", "tbl_without_pk", handle.getConnection());
      RelationalTable tbl_with_pk = new RelationalTable("public", "tbl_with_pk", handle.getConnection());
      Assert.assertEquals(withPK, tbl_with_pk.preparedInsertStatement());
      Assert.assertEquals(withoutPK, tbl_without_pk.preparedInsertStatement());
    }
  }

  @Test
  void preparedDeleteStatement() {
    String withPK = "DELETE FROM tbl_with_pk \n" +
                    "WHERE coll1 = :coll1 \n" +
                    "    AND id = :id";
    try (Handle handle = jdbi.open()) {
      RelationalTable tbl_without_pk = new RelationalTable("public", "tbl_without_pk", handle.getConnection());
      RelationalTable tbl_with_pk = new RelationalTable("public", "tbl_with_pk", handle.getConnection());
      Assert.assertEquals(withPK, tbl_with_pk.preparedDeleteStatement());
      Assert.assertThrows(DebeziumException.class, tbl_without_pk::preparedDeleteStatement);
    }
  }
}