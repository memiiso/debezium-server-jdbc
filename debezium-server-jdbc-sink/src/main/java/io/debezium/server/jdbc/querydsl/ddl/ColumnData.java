/*
 * Copyright (c) 2010 Mysema Ltd.
 * All rights reserved.
 *
 */

package io.debezium.server.jdbc.querydsl.ddl;

/**
 * ColumnData
 *
 * @author tiwe
 */
public class ColumnData {

  private final String name;

  private final String type;

  private boolean nullAllowed = true;

  private boolean autoIncrement;

  private Integer size;

  public ColumnData(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public boolean isNullAllowed() {
    return nullAllowed;
  }

  public void setNullAllowed(boolean nullAllowed) {
    this.nullAllowed = nullAllowed;
  }

  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  public void setAutoIncrement(boolean autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

}