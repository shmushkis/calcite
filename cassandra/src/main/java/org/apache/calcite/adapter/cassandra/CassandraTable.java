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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Table based on a Cassandra column family
 */
public class CassandraTable extends AbstractTable
    implements ScannableTable {
  private RelProtoDataType protoRowType;
  private final CassandraSchema schema;
  private final String columnFamily;

  public CassandraTable(CassandraSchema schema, String columnFamily) {
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

  public Enumerable<Object[]> scan(DataContext root) {
    Session session = schema.session;
    final ResultSet results = session.execute("SELECT * FROM \"test\"");

    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new CassandraEnumerator(results, protoRowType);
      }
    };
  }
}

// End CassandraTable.java
