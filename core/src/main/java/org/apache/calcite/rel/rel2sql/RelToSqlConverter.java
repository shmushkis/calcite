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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Utility to convert relational expressions to SQL abstract syntax tree.
 */
public class RelToSqlConverter extends SqlImplementor {

  private static final Logger LOGGER = Logger.getLogger(RelToSqlConverter.class.getName());

  public RelToSqlConverter(SqlDialect dialect) {
    super(dialect);
  }

  public Result visitChild(int i, RelNode e) {
    if (e instanceof Union) {
      return visitUnion((Union) e);
    } else if (e instanceof Join) {
      return visitJoin((Join) e);
    } else if (e instanceof Filter) {
      return visitFilter((Filter) e);
    } else if (e instanceof Project) {
      return visitProject((Project) e);
    } else if (e instanceof Aggregate) {
      return visitAggregate((Aggregate) e);
    } else if (e instanceof TableScan) {
      return visitTableScan((TableScan) e);
    } else if (e instanceof Intersect) {
      return visitIntersect((Intersect) e);
    } else if (e instanceof Minus) {
      return visitMinus((Minus) e);
    } else if (e instanceof Calc) {
      return visitCalc((Calc) e);
    } else if (e instanceof Sort) {
      return visitSort((Sort) e);
    } else if (e instanceof TableModify) {
      return visitTableModify((TableModify) e);
    } else {
      throw new AssertionError("Need to Implement for " + e.getClass().getName()); // TODO:
    }
  }

  public Result visitUnion(Union e) {
    final SqlSetOperator operator = e.all
        ? SqlStdOperatorTable.UNION_ALL
        : SqlStdOperatorTable.UNION;
    return setOpToSql(operator, e);
  }

  public Result visitJoin(Join e) {
    final Result leftResult = visitChild(0, e.getLeft());
    final Result rightResult = visitChild(1, e.getRight());
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext =
        rightResult.qualifiedContext();
    SqlNode sqlCondition = convertConditionToSqlNode(e.getCondition(),
        leftContext,
        rightContext,
        e.getLeft().getRowType().getFieldCount());
    SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType(e.getJoinType()).symbol(POS),
            rightResult.asFrom(),
            JoinConditionType.ON.symbol(POS),
            sqlCondition);
    return result(join, leftResult, rightResult);
  }

  public Result visitFilter(Filter e) {
    Result x = visitChild(0, e.getInput());
    final Builder builder =
        x.builder(e, Clause.WHERE);
    builder.setWhere(builder.context.toSql(null, e.getCondition()));
    return builder.result();
  }

  public Result visitProject(Project e) {
    Result x = visitChild(0, e.getInput());
    if (isStar(e.getChildExps(), e.getInput().getRowType())) {
      return x;
    }
    final Builder builder =
        x.builder(e, Clause.SELECT);
    final List<SqlNode> selectList = new ArrayList<>();
    for (RexNode ref : e.getChildExps()) {
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      addSelect(selectList, sqlExpr, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }


  public Result visitAggregate(Aggregate e) {
    // "select a, b, sum(x) from ( ... ) group by a, b"
    final Result x = visitChild(0, e.getInput());
    final Builder builder;
    if (e.getInput() instanceof Project) {
      builder = x.builder(e);
      builder.clauses.add(Clause.GROUP_BY);
    } else {
      builder = x.builder(e, Clause.GROUP_BY);
    }
    List<SqlNode> groupByList = Expressions.list();
    final List<SqlNode> selectList = new ArrayList<>();
    for (int group : e.getGroupSet()) {
      final SqlNode field = builder.context.field(group);
      addSelect(selectList, field, e.getRowType());
      groupByList.add(field);
    }
    for (AggregateCall aggCall : e.getAggCallList()) {
      SqlNode aggCallSqlNode = builder.context.toSql(aggCall);
      if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        aggCallSqlNode =
            rewriteSingleValueExpr(aggCallSqlNode, dialect);
      }
      addSelect(selectList, aggCallSqlNode, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));
    if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
      // Some databases don't support "GROUP BY ()". We can omit it as long
      // as there is at least one aggregate function.
      builder.setGroupBy(new SqlNodeList(groupByList, POS));
    }
    return builder.result();
  }

  public Result visitTableScan(TableScan e) {
    return result(new SqlIdentifier(e.getTable().getQualifiedName(), SqlParserPos.ZERO),
        Collections.singletonList(Clause.FROM), e);
  }

  public Result visitIntersect(Intersect e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT, e);
  }

  public Result visitMinus(Minus e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT, e);
  }

  public Result visitCalc(Calc e) {
    Result x = visitChild(0, e.getInput());
    final RexProgram program = e.getProgram();
    Builder builder =
        program.getCondition() != null
            ? x.builder(e, Clause.WHERE)
            : x.builder(e);
    if (!isStar(program)) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (RexLocalRef ref : program.getProjectList()) {
        SqlNode sqlExpr = builder.context.toSql(program, ref);
        addSelect(selectList, sqlExpr, e.getRowType());
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
    }

    if (program.getCondition() != null) {
      builder.setWhere(
          builder.context.toSql(program, program.getCondition()));
    }
    return builder.result();
  }

  public Result visitValues(Values e) {
    final List<String> fields = e.getRowType().getFieldNames();
    final List<Clause> clauses = Collections.singletonList(Clause.SELECT);
    final Context context =
        new AliasContext(Collections.<Pair<String, RelDataType>>emptyList(), false);
    final List<SqlSelect> selects = new ArrayList<>();
    for (List<RexLiteral> tuple : e.getTuples()) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (Pair<RexLiteral, String> literal : Pair.zip(tuple, fields)) {
        selectList.add(
            SqlStdOperatorTable.AS.createCall(
                POS,
                context.toSql(null, literal.left),
                new SqlIdentifier(literal.right, POS)));
      }
      selects.add(
          new SqlSelect(POS, SqlNodeList.EMPTY,
              new SqlNodeList(selectList, POS), null, null, null,
              null, null, null, null, null));
    }
    SqlNode query = null;
    for (SqlSelect select : selects) {
      if (query == null) {
        query = select;
      } else {
        query = SqlStdOperatorTable.UNION_ALL.createCall(POS, query,
            select);
      }
    }
    return result(query, clauses, e);
  }

  public Result visitSort(Sort e) {
    final Result x = visitChild(0, e.getInput());
    final Builder builder = x.builder(e, Clause.ORDER_BY);
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }
    builder.setOrderBy(new SqlNodeList(orderByList, POS));
    return builder.result();
  }

  public Result visitTableModify(TableModify e) {
    throw new AssertionError(); // TODO:
  }

  @Override public void addSelect(List<SqlNode> selectList, SqlNode node,
      RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    if (name.toLowerCase().startsWith("expr$")) {
      //Put it in ordinalMap
      ordinalMap.put(name.toLowerCase(), node);
    } else if (alias == null || !alias.equals(name)) {
      node = SqlStdOperatorTable.AS.createCall(
          POS, node, new SqlIdentifier(name, POS));
    }
    selectList.add(node);
  }
}

// End RelToSqlConverter.java
