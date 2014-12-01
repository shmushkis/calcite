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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.util.Cursor;

import java.util.List;

/**
 * Command handler for getting various metadata. Should be implemented by each
 * driver.
 *
 * <p>Also holds other abstract methods that are not related to metadata
 * that each provider must implement. This is not ideal.</p>
 */
public interface Meta {
  String getSqlKeywords();

  String getNumericFunctions();

  String getStringFunctions();

  String getSystemFunctions();

  String getTimeDateFunctions();

  MetaResultSet getTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList);

  MetaResultSet getColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  MetaResultSet getSchemas(String catalog, Pat schemaPattern);

  MetaResultSet getCatalogs();

  MetaResultSet getTableTypes();

  MetaResultSet getProcedures(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern);

  MetaResultSet getProcedureColumns(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern);

  MetaResultSet getColumnPrivileges(String catalog,
      String schema,
      String table,
      Pat columnNamePattern);

  MetaResultSet getTablePrivileges(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  MetaResultSet getBestRowIdentifier(String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable);

  MetaResultSet getVersionColumns(String catalog, String schema, String table);

  MetaResultSet getPrimaryKeys(String catalog, String schema, String table);

  MetaResultSet getImportedKeys(String catalog, String schema, String table);

  MetaResultSet getExportedKeys(String catalog, String schema, String table);

  MetaResultSet getCrossReference(String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable);

  MetaResultSet getTypeInfo();

  MetaResultSet getIndexInfo(String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate);

  MetaResultSet getUDTs(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types);

  MetaResultSet getSuperTypes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern);

  MetaResultSet getSuperTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  MetaResultSet getAttributes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern);

  MetaResultSet getClientInfoProperties();

  MetaResultSet getFunctions(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern);

  MetaResultSet getFunctionColumns(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern);

  MetaResultSet getPseudoColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  /** Creates a cursor for a result set. */
  Cursor createCursor(AvaticaResultSet resultSet);

  AvaticaPrepareResult prepare(AvaticaStatement statement, String sql);

  MetaResultSet createResultSet(Iterable elements,
      MetaImpl.NamedFieldGetter namedFieldGetter);

  /** Wrapper to remind API calls that a parameter is a pattern (allows '%' and
   * '_' wildcards, per the JDBC spec) rather than a string to be matched
   * exactly. */
  class Pat {
    public final String s;

    private Pat(String s) {
      this.s = s;
    }

    public static Pat of(String name) {
      return new Pat(name);
    }
  }

  /** Meta data from which a result set can be constructed. */
  class MetaResultSet {
    public final AvaticaStatement statement;
    public final boolean ownStatement;
    public AvaticaPrepareResult prepareResult;

    public MetaResultSet(AvaticaStatement statement, boolean ownStatement,
        AvaticaPrepareResult prepareResult, Cursor cursor) {
      this.prepareResult = prepareResult;
      this.statement = statement;
      this.ownStatement = ownStatement;
    }
  }
}

// End Meta.java
