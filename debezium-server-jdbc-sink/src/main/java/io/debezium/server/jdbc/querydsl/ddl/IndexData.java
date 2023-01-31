/*
 * Copyright (c) 2010 Mysema Ltd.
 * All rights reserved.
 *
 */

package io.debezium.server.jdbc.querydsl.ddl;

/**
 * IndexData
 *
 * @author tiwe
 */
public class IndexData {

  private final String name;

  private final String[] columns;

  private boolean unique;

  public IndexData(String name, String[] columns) {
    this.name = name;
    this.columns = columns.clone();
  }

  public String getName() {
    return name;
  }

  public String[] getColumns() {
    return columns.clone();
  }

  public boolean isUnique() {
    return unique;
  }

  public void setUnique(boolean unique) {
    this.unique = unique;
  }


}
