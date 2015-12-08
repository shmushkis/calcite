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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
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
    for (RelFieldCollation fieldCollation : e.getCollation().getFieldCollations()) {
      if (fieldCollation.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED
          && dialect.getDatabaseProduct() == SqlDialect.DatabaseProduct.MYSQL) {
        orderByList.add(
            ISNULL_FUNCTION.createCall(POS,
                builder.context.field(fieldCollation.getFieldIndex())));
        fieldCollation = new RelFieldCollation(fieldCollation.getFieldIndex(),
            fieldCollation.getDirection(),
            RelFieldCollation.NullDirection.UNSPECIFIED);
      }
      orderByList.add(builder.context.toSql(fieldCollation));
    }
    builder.setOrderBy(new SqlNodeList(orderByList, POS));
    return builder.result();
  }

  public Result visitTableModify(TableModify e) {
    throw new AssertionError(); // TODO:
  }

  /**
   * MySQL specific function.
   */
  private static final SqlFunction ISNULL_FUNCTION =
      new SqlFunction("ISNULL", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN, InferTypes.FIRST_KNOWN,
          OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

  /**
   * Rewrite SINGLE_VALUE into expression based on database variants
   * E.g. HSQLDB, MYSQL, ORACLE, etc
   */
  private SqlNode rewriteSingleValueExpr(SqlNode aggCall,
                                         SqlDialect sqlDialect) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlNode caseOperand;
    final SqlNode elseExpr;
    final SqlNode countCall =
        SqlStdOperatorTable.COUNT.createCall(POS, operand);

    final SqlLiteral nullLiteral = SqlLiteral.createNull(POS);
    final SqlNode wrappedOperand;
    switch (sqlDialect.getDatabaseProduct()) {
    case MYSQL:
    case HSQLDB:
      caseOperand = countCall;
      final SqlNodeList selectList = new SqlNodeList(POS);
      selectList.add(nullLiteral);
      final SqlNode unionOperand;
      switch (sqlDialect.getDatabaseProduct()) {
      case MYSQL:
        wrappedOperand = operand;
        unionOperand = new SqlSelect(POS, SqlNodeList.EMPTY, selectList,
            null, null, null, null, SqlNodeList.EMPTY, null, null, null);
        break;
      default:
        wrappedOperand = SqlStdOperatorTable.MIN.createCall(POS, operand);
        unionOperand = SqlStdOperatorTable.VALUES.createCall(POS,
            SqlLiteral.createApproxNumeric("0", POS));
      }

      SqlCall unionAll = SqlStdOperatorTable.UNION_ALL
          .createCall(POS, unionOperand, unionOperand);

      final SqlNodeList subQuery = new SqlNodeList(POS);
      subQuery.add(unionAll);

      final SqlNodeList selectList2 = new SqlNodeList(POS);
      selectList2.add(nullLiteral);
      elseExpr = SqlStdOperatorTable.SCALAR_QUERY.createCall(POS, subQuery);
      break;

    default:
      LOGGER.info("SINGLE_VALUE rewrite not supported for "
          + sqlDialect.getDatabaseProduct());
      return aggCall;
    }

    final SqlNodeList whenList = new SqlNodeList(POS);
    whenList.add(SqlLiteral.createExactNumeric("0", POS));
    whenList.add(SqlLiteral.createExactNumeric("1", POS));

    final SqlNodeList thenList = new SqlNodeList(POS);
    thenList.add(nullLiteral);
    thenList.add(wrappedOperand);

    SqlNode caseExpr =
        new SqlCase(POS, caseOperand, whenList, thenList, elseExpr);

    LOGGER.info("SINGLE_VALUE rewritten into [" + caseExpr + "]");

    return caseExpr;
  }

  private void addSelect(
      List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
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

  private static boolean isStar(List<RexNode> exps, RelDataType inputRowType) {
    int i = 0;
    for (RexNode ref : exps) {
      if (!(ref instanceof RexInputRef)) {
        return false;
      } else if (((RexInputRef) ref).getIndex() != i++) {
        return false;
      }
    }
    return i == inputRowType.getFieldCount();
  }

  private static boolean isStar(RexProgram program) {
    int i = 0;

    for (RexLocalRef ref : program.getProjectList()) {
      if (ref.getIndex() != i++) {
        return false;
      }
    }
    return i == program.getInputRowType().getFieldCount();
  }

  private Result setOpToSql(SqlSetOperator operator, RelNode rel) {
    List<SqlNode> list = Expressions.list();
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      final Result result = this.visitChild(input.i, input.e);
      list.add(result.asSelect());
    }
    final SqlCall node = operator.createCall(new SqlNodeList(list, POS));
    final List<Clause> clauses =
        Expressions.list(Clause.SET_OP);
    return result(node, clauses, rel);
  }

  /**
   * Convert {@link RexNode} condition into {@link SqlNode}
   *
   * @param node           condition Node
   * @param leftContext    LeftContext
   * @param rightContext   RightContext
   * @param leftFieldCount Number of field on left result
   * @return SqlJoin which represent the condition
   */
  private SqlNode convertConditionToSqlNode(RexNode node,
                                            Context leftContext,
                                            Context rightContext,
                                            int leftFieldCount) {
    if (!(node instanceof RexCall)) {
      throw new AssertionError(node);
    }
    final List<RexNode> operands;
    final SqlOperator op;
    switch (node.getKind()) {
    case AND:
    case OR:
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      SqlNode sqlCondition = null;
      for (RexNode operand : operands) {
        SqlNode x = convertConditionToSqlNode(operand, leftContext,
            rightContext, leftFieldCount);
        if (sqlCondition == null) {
          sqlCondition = x;
        } else {
          sqlCondition = op.createCall(POS, sqlCondition, x);
        }
      }
      return sqlCondition;

    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      if (operands.get(0) instanceof RexInputRef
          && operands.get(1) instanceof RexInputRef) {
        final RexInputRef op0 = (RexInputRef) operands.get(0);
        final RexInputRef op1 = (RexInputRef) operands.get(1);

        if (op0.getIndex() < leftFieldCount
            && op1.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op0 = op1'
          return op.createCall(POS,
              leftContext.field(op0.getIndex()),
              rightContext.field(op1.getIndex() - leftFieldCount));
        }
        if (op1.getIndex() < leftFieldCount
            && op0.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op1 = op0'
          return reverseOperatorDirection(op).createCall(POS,
              leftContext.field(op1.getIndex()),
              rightContext.field(op0.getIndex() - leftFieldCount));
        }
      }
    }
    throw new AssertionError(node);
  }

  private static SqlOperator reverseOperatorDirection(SqlOperator op) {
    switch (op.kind) {
    case GREATER_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
      return op;
    default:
      throw new AssertionError(op);
    }
  }

  private static JoinType joinType(JoinRelType joinType) {
    switch (joinType) {
    case LEFT:
      return JoinType.LEFT;
    case RIGHT:
      return JoinType.RIGHT;
    case INNER:
      return JoinType.INNER;
    case FULL:
      return JoinType.FULL;
    default:
      throw new AssertionError(joinType);
    }
  }
}

// End RelToSqlConverter.java
