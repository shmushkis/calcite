/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDdlNodes {
  private SqlDdlNodes() {}

  /** Creates a CREATE TABLE. */
  public static SqlCreateTable createTable(SqlParserPos pos,
      SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    return new SqlCreateTable(pos, name, columnList, query);
  }

  /** Creates a CREATE VIEW. */
  public static SqlCreateView createView(SqlParserPos pos,
      boolean materialized, boolean replace, SqlIdentifier name,
      SqlNodeList columnList, SqlNode query) {
    return new SqlCreateView(pos, materialized, replace, name, columnList,
        query);
  }

  /** Creates a column declaration. */
  public static SqlNode column(SqlParserPos pos, SqlIdentifier name,
      SqlDataTypeSpec dataType, SqlNode expression, boolean virtual) {
    return new SqlColumnDeclaration(pos, name, dataType, expression, virtual);
  }

  /** Creates a CHECK constraint. */
  public static SqlNode check(SqlParserPos pos, SqlIdentifier name,
      SqlNode expression) {
    return new SqlCheckConstraint(pos, name, expression);
  }

  /** Creates a UNIQUE constraint. */
  public static SqlKeyConstraint unique(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList) {
    return new SqlKeyConstraint(pos, name, columnList);
  }

  /** Creates a PRIMARY KEY constraint. */
  public static SqlKeyConstraint primary(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList) {
    return new SqlKeyConstraint(pos, name, columnList) {
      @Override public SqlOperator getOperator() {
        return PRIMARY;
      }
    };
  }

}

// End SqlDdlNodes.java
