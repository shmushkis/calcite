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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Table based on a Cassandra column family
 */
public class CassandraTable extends AbstractQueryableTable
    implements TranslatableTable {
  RelProtoDataType protoRowType;
  private final CassandraSchema schema;
  private final String columnFamily;

  public CassandraTable(CassandraSchema schema, String columnFamily) {
    super(Object[].class);
    this.schema = schema;
    this.columnFamily = columnFamily;
  }

  public String toString() {
    return "CassandraTable {" + columnFamily + "}";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType == null) {
      protoRowType = schema.getRelDataType(columnFamily);
    }
    return protoRowType.apply(typeFactory);
  }

  public Enumerable<Object> query(final Session session) {
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final ResultSet results = session.execute("SELECT * FROM \"test\"");
        return new CassandraEnumerator(results, protoRowType);
      }
    };
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new CassandraQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CassandraTableScan(cluster, cluster.traitSetOf(CassandraRel.CONVENTION),
        relOptTable, this, null);
  }

  /** Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link org.apache.calcite.adapter.cassandra.CassandraTable}. */
  public static class CassandraQueryable<T> extends AbstractTableQueryable<T> {
    public CassandraQueryable(QueryProvider queryProvider, SchemaPlus schema,
        CassandraTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().query(getSession());
      return enumerable.enumerator();
    }

    private CassandraTable getTable() {
      return (CassandraTable) table;
    }

    private Session getSession() {
      return schema.unwrap(CassandraSchema.class).session;
    }

    /** Called via code-generation.
     *
     * @see org.apache.calcite.adapter.mongodb.CassandraMethod#CASSANDRA_QUERYABLE_QUERY
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query() {
      return getTable().query(getSession());
    }
  }
}

// End CassandraTable.java
