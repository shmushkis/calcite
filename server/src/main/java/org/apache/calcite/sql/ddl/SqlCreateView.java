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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateView extends SqlCreate
    implements SqlExecutableStatement {
  private final boolean materialized;
  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNode query;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

  /** Creates a SqlCreateView. */
  SqlCreateView(SqlParserPos pos, boolean materialized, boolean replace,
      SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    super(pos, replace);
    this.materialized = materialized;
    this.name = name;
    this.columnList = columnList; // may be null
    this.query = Preconditions.checkNotNull(query);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (getReplace()) {
      writer.keyword("CREATE OR REPLACE");
    } else {
      writer.keyword("CREATE");
    }
    if (materialized) {
      writer.keyword("MATERIALIZED VIEW");
    } else {
      writer.keyword("VIEW");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("AS");
    writer.newlineAndIndent();
    query.unparse(writer, 0, 0);
  }

  /** Creates a list of (name, type) pairs from {@link #columnList}, in which
   * they alternate. */
  private List<Pair<SqlIdentifier, SqlDataTypeSpec>> nameTypes() {
    final List list = columnList.getList();
    //noinspection unchecked
    return Pair.zip((List<SqlIdentifier>) Util.quotientList(list, 2, 0),
        Util.quotientList((List<SqlDataTypeSpec>) list, 2, 1));
  }

  public void execute(CalcitePrepare.Context context) {
    final List<String> path = context.getDefaultSchemaPath();
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Pair<SqlIdentifier, SqlDataTypeSpec> pair : nameTypes()) {
      builder.add(pair.left.getSimple(),
          pair.right.deriveType(typeFactory, true));
    }
    final RelDataType rowType = builder.build();
    schema.add(name.getSimple(),
        new MutableArrayTable(name.getSimple(),
            RelDataTypeImpl.proto(rowType)));
  }

  /** Abstract base class for implementations of {@link ModifiableTable}. */
  abstract static class AbstractModifiableTable
      extends AbstractTable implements ModifiableTable {
    AbstractModifiableTable(String tableName) {
      super();
    }

    public TableModify toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
      return LogicalTableModify.create(table, catalogReader, child, operation,
          updateColumnList, sourceExpressionList, flattened);
    }
  }

  /** Table backed by a Java list. */
  static class MutableArrayTable extends AbstractModifiableTable {
    final List list = new ArrayList();
    private final RelProtoDataType protoRowType;

    MutableArrayTable(String name, RelProtoDataType protoRowType) {
      super(name);
      this.protoRowType = protoRowType;
    }

    public Collection getModifiableCollection() {
      return list;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        public Enumerator<T> enumerator() {
          //noinspection unchecked
          return (Enumerator<T>) Linq4j.enumerator(list);
        }
      };
    }

    public Type getElementType() {
      return Object[].class;
    }

    public Expression getExpression(SchemaPlus schema, String tableName,
        Class clazz) {
      return Schemas.tableExpression(schema, getElementType(),
          tableName, clazz);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }
  }
}

// End SqlCreateView.java
