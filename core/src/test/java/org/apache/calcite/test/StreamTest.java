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
package org.apache.calcite.test;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for streaming queries.
 */
public class StreamTest {
  public static final String STREAM_SCHEMA = "     {\n"
      + "       name: 'STREAMS',\n"
      + "       tables: [ {\n"
      + "         type: 'custom',\n"
      + "         name: 'ORDERS',\n"
      + "         stream: true,\n"
      + "         factory: '" + OrdersStreamTableFactory.class.getName() + "'\n"
      + "       } ]\n"
      + "     }\n";

  public static final String STREAM_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + STREAM_SCHEMA
      + "   ]\n"
      + "}";

  @Test public void testStream() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream * from orders")
        .convertContains("LogicalDelta\n"
            + "  LogicalProject(id=[$0], product=[$1], quantity=[$2])\n"
            + "    EnumerableTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains("EnumerableInterpreter\n"
            + "  BindableTableScan(table=[[]])")
        .returns(
            startsWith("id=1; product=paint; quantity=10",
                "id=2; product=paper; quantity=5"));
  }

  private Function<ResultSet, Void> startsWith(String... rows) {
    final ImmutableList<String> rowList = ImmutableList.copyOf(rows);
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet input) {
        try {
          final StringBuilder buf = new StringBuilder();
          final ResultSetMetaData metaData = input.getMetaData();
          for (String expectedRow : rowList) {
            if (!input.next()) {
              throw new AssertionError("input ended too soon");
            }
            CalciteAssert.rowToString(input, buf, metaData);
            String actualRow = buf.toString();
            buf.setLength(0);
            assertThat(actualRow, equalTo(expectedRow));
          }
          return null;
        } catch (SQLException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /** Mock table that returns a stream of orders from a fixed array. */
  @SuppressWarnings("UnusedDeclaration")
  public static class OrdersStreamTableFactory implements TableFactory<Table> {
    // public constructor, per factory contract
    public OrdersStreamTableFactory() {
    }

    public Table create(SchemaPlus schema, String name,
        Map<String, Object> operand, RelDataType rowType) {
      final RelProtoDataType protoRowType = new RelProtoDataType() {
        public RelDataType apply(RelDataTypeFactory a0) {
          return a0.builder()
              .add("id", SqlTypeName.INTEGER)
              .add("product", SqlTypeName.VARCHAR, 10)
              .add("quantity", SqlTypeName.INTEGER)
              .build();
        }
      };
      final ImmutableList<Object[]> rows = ImmutableList.of(
          new Object[] {1, "paint", 10},
          new Object[] {2, "paper", 5});

      return new StreamableTable() {
        public Table stream() {
          return new OrdersTable(protoRowType, rows);
        }

        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return protoRowType.apply(typeFactory);
        }

        public Statistic getStatistic() {
          return Statistics.UNKNOWN;
        }

        public Schema.TableType getJdbcTableType() {
          return Schema.TableType.TABLE;
        }
      };
    }
  }

  /** Table representing the ORDERS stream. */
  public static class OrdersTable implements ScannableTable {
    private final RelProtoDataType protoRowType;
    private final ImmutableList<Object[]> rows;

    public OrdersTable(RelProtoDataType protoRowType,
        ImmutableList<Object[]> rows) {
      this.protoRowType = protoRowType;
      this.rows = rows;
    }

    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.STREAM;
    }
  }
}

// End StreamTest.java
