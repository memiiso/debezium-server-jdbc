/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.jdbc.relational;

import org.jdbi.v3.core.Jdbi;

public class AppendTableWriter extends BaseTableWriter {
  public AppendTableWriter(Jdbi jdbi, String identifierQuoteCharacter) {
    super(jdbi, identifierQuoteCharacter);
  }
}
