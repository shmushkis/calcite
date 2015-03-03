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

package org.apache.calcite.jdbc;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.util.Compatible;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * An {@link org.apache.calcite.jdbc.CalciteSchema} implementation that
 * maintains minimal state.
 */
public class SimpleCalciteSchema extends CalciteSchema {
  public SimpleCalciteSchema(CalciteSchema parent, Schema schema, String name) {
    super(parent, schema, name);
  }

  @Override
  public void setCache(boolean cache) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CalciteSchema getSubSchema(String schemaName, boolean caseSensitive) {
    if (caseSensitive) {
      // Check explicit schemas, case-sensitive.
      final CalciteSchema entry = subSchemaMap.get(schemaName);
      if (entry != null) {
        return entry;
      }
    } else {
      // Check explicit schemas, case-insensitive.
      //noinspection LoopStatementThatDoesntLoop
      for (Map.Entry<String, CalciteSchema> entry
          : find(subSchemaMap, schemaName).entrySet()) {
        return entry.getValue();
      }
    }
    // Check implicit schemas.
    Schema s = schema.getSubSchema(schemaName);
    if (s != null) {
      return new SimpleCalciteSchema(this, s, schemaName);
    }

    return null;
  }

  @Override
  public CalciteSchema add(String name, Schema schema) {
    final CalciteSchema calciteSchema =
        new SimpleCalciteSchema(this, schema, name);
    subSchemaMap.put(name, calciteSchema);
    return calciteSchema;
  }

  @Override
  public TableEntry getTable(String tableName, boolean caseSensitive) {
    if (caseSensitive) {
      // Check explicit tables, case-sensitive.
      final TableEntry entry = tableMap.get(tableName);
      if (entry != null) {
        return entry;
      }
    } else {
      // Check explicit tables, case-insensitive.
      //noinspection LoopStatementThatDoesntLoop
      for (Map.Entry<String, TableEntry> entry
          : find(tableMap, tableName).entrySet()) {
        return entry.getValue();
      }
    }
    // Check implicit tables.
    Table table = schema.getTable(tableName);
    if (table != null) {
      return tableEntry(tableName, table);
    }
    return null;
  }

  @Override
  public NavigableMap<String, CalciteSchema> getSubSchemaMap() {
    // Build a map of implicit sub-schemas first, then explicit sub-schemas.
    // If there are implicit and explicit with the same name, explicit wins.
    final ImmutableSortedMap.Builder<String, CalciteSchema> builder =
        new ImmutableSortedMap.Builder<String, CalciteSchema>(COMPARATOR);
    final long now = System.currentTimeMillis();
    builder.putAll(subSchemaMap);
    return Compatible.INSTANCE.navigableMap(builder.build());
  }

  @Override
  public NavigableSet<String> getTableNames() {
    final ImmutableSortedSet.Builder<String> builder =
        new ImmutableSortedSet.Builder<String>(COMPARATOR);
    // Add explicit tables, case-sensitive.
    builder.addAll(tableMap.keySet());
    // Add implicit tables
    builder.addAll(schema.getTableNames());
    return Compatible.INSTANCE.navigableSet(builder.build());
  }

  @Override
  public Collection<Function> getFunctions(String name, boolean caseSensitive) {
    final ImmutableList.Builder<Function> builder = ImmutableList.builder();

    if (caseSensitive) {
      // Add explicit functions, case-sensitive.
      final Collection<FunctionEntry> functionEntries = functionMap.get(name);
      if (functionEntries != null) {
        for (FunctionEntry functionEntry : functionEntries) {
          builder.add(functionEntry.getFunction());
        }
      }
      // Add implicit functions, case-sensitive.
      final Collection<Function> functions = schema.getFunctions(name);
      if (functions != null) {
        builder.addAll(functions);
      }
    } else {
      // Add explicit functions, case-insensitive.
      for (String name2 : find(functionNames, name)) {
        final Collection<FunctionEntry> functionEntries =
            functionMap.get(name2);
        if (functionEntries != null) {
          for (FunctionEntry functionEntry : functionEntries) {
            builder.add(functionEntry.getFunction());
          }
        }
      }
    }
    return builder.build();
  }

  @Override
  public NavigableSet<String> getFunctionNames() {
    final ImmutableSortedSet.Builder<String> builder =
        new ImmutableSortedSet.Builder<String>(COMPARATOR);
    // Add explicit functions, case-sensitive.
    builder.addAll(functionMap.keySet());
    // Add implicit functions, case-sensitive.
    return Compatible.INSTANCE.navigableSet(builder.build());
  }

  @Override
  public NavigableMap<String, Table> getTablesBasedOnNullaryFunctions() {
    ImmutableSortedMap.Builder<String, Table> builder =
        new ImmutableSortedMap.Builder<String, Table>(COMPARATOR);
    for (Map.Entry<String, FunctionEntry> s : nullaryFunctionMap.entrySet()) {
      final Function function = s.getValue().getFunction();
      if (function instanceof TableMacro) {
        assert function.getParameters().isEmpty();
        final Table table = ((TableMacro) function).apply(ImmutableList.of());
        builder.put(s.getKey(), table);
      }
    }
    return Compatible.INSTANCE.navigableMap(builder.build());
  }

  @Override
  public TableEntry getTableBasedOnNullaryFunction(String tableName, boolean caseSensitive) {
    if (caseSensitive) {
      final FunctionEntry functionEntry = nullaryFunctionMap.get(tableName);
      if (functionEntry != null) {
        final Function function = functionEntry.getFunction();
        if (function instanceof TableMacro) {
          assert function.getParameters().isEmpty();
          final Table table = ((TableMacro) function).apply(ImmutableList.of());
          return tableEntry(tableName, table);
        }
      }
      for (Function function : schema.getFunctions(tableName)) {
        if (function instanceof TableMacro
            && function.getParameters().isEmpty()) {
          final Table table = ((TableMacro) function).apply(ImmutableList.of());
          return tableEntry(tableName, table);
        }
      }
    } else {
      for (Map.Entry<String, FunctionEntry> entry
          : find(nullaryFunctionMap, tableName).entrySet()) {
        final Function function = entry.getValue().getFunction();
        if (function instanceof TableMacro) {
          assert function.getParameters().isEmpty();
          final Table table = ((TableMacro) function).apply(ImmutableList.of());
          return tableEntry(tableName, table);
        }
      }
    }
    return null;
  }

  @Override
  protected boolean isCacheEnabled() {
    return false;
  }

  /** Creates a root schema. When <code>addMetadataSchema</code> argument is
   * true a "metadata" schema containing definitions of tables, columns etc. is
   * added to root schema. */
  public static CalciteSchema createRootSchema(boolean addMetadataSchema) {
    SimpleCalciteSchema rootSchema = new SimpleCalciteSchema(null,
        new CalciteConnectionImpl.RootSchema(), "");
    if (addMetadataSchema) {
      rootSchema.add("metadata", MetadataSchema.INSTANCE);
    }
    return rootSchema;
  }

}
