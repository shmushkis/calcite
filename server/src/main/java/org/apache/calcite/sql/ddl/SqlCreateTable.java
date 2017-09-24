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
import org.apache.calcite.linq4j.Ord;
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
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableNullableList;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate
    implements SqlExecutableStatement {
  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNode query;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.OTHER_DDL);

  /** Creates a SqlCreateTable. */
  SqlCreateTable(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList, SqlNode query) {
    super(pos, false);
    this.name = name;
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
  }

  public void execute(CalcitePrepare.Context context) {
    final List<String> path = context.getDefaultSchemaPath();
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final Map<Integer, SqlNode> columnExprs = new HashMap<>();
    for (Ord<SqlNode> c : Ord.zip(columnList)) {
      if (c.e instanceof SqlColumnDeclaration) {
        final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
        builder.add(d.name.getSimple(),
            d.dataType.deriveType(typeFactory, true));
        if (d.expression != null) {
          columnExprs.put(c.i, d.expression);
        }
      } else {
        throw new AssertionError(); // TODO:
      }
    }
    final InitializerExpressionFactory ief;
    if (columnExprs.isEmpty()) {
      ief = null;
    } else {
      ief = new NullInitializerExpressionFactory() {
        @Override public Strategy generationStrategy(RelOptTable table,
            int iColumn) {
          final SqlNode c = columnExprs.get(iColumn);
          if (c == null) {
            return Strategy.NULLABLE;
          }
          return Strategy.STORED;
        }
      };
    }
    final RelDataType rowType = builder.build();
    schema.add(name.getSimple(),
        new MutableArrayTable(name.getSimple(),
            RelDataTypeImpl.proto(rowType), ief));
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
  static class MutableArrayTable extends AbstractModifiableTable
      implements Wrapper {
    final List list = new ArrayList();
    private final RelProtoDataType protoRowType;
    private final InitializerExpressionFactory initializerExpressionFactory;

    MutableArrayTable(String name, RelProtoDataType protoRowType,
        InitializerExpressionFactory initializerExpressionFactory) {
      super(name);
      this.protoRowType = protoRowType;
      this.initializerExpressionFactory = initializerExpressionFactory;
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

    public <C> C unwrap(Class<C> aClass) {
      if (aClass.isInstance(this)) {
        return aClass.cast(this);
      }
      if (aClass.isInstance(initializerExpressionFactory)) {
        return aClass.cast(initializerExpressionFactory);
      }
      return null;
    }
  }
}

// End SqlCreateTable.java
