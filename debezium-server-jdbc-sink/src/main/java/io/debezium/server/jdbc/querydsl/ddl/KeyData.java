/*
 *
 */

package io.debezium.server.jdbc.querydsl.ddl;

import java.util.List;

/**
 * Common interface for ForeignKeyData and InverseForeignKeyData
 *
 * @author tiwe
 */
public interface KeyData {

  String getName();

  String getTable();

  List<String> getForeignColumns();

  List<String> getParentColumns();

}