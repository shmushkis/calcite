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
package org.apache.calcite.rex;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Visitor that converts 3-valued logic into two-valued logic. */
class NullSafeVisitor {
  final RexBuilder rexBuilder;
  private final RelDataTypeFactory typeFactory;

  /** Expressions that are known to be not null. */
  private final Deque<RexNode> notNullNodes = new ArrayDeque<>();

  public NullSafeVisitor(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    this.typeFactory = rexBuilder.typeFactory;
  }

  private RexNode reuse(RexNode original, RexNode new_) {
    return new_.equals(original) ? original : new_;
  }

  public RexNode value(RexNode e) {
    final RexCall call;
    final List<RexNode> isUnknowns;
    final List<RexNode> isFalses;
    final List<RexNode> isTrues;
    final List<RexNode> newOperands;
    switch (e.getKind()) {
    case IS_TRUE:
      assert e instanceof RexCall;
      call = (RexCall) e;
      return reuse(e, isTrue(call.getOperands().get(0)));

    case IS_FALSE:
      assert e instanceof RexCall;
      call = (RexCall) e;
      return reuse(e, isFalse(call.getOperands().get(0)));

    case IS_NOT_TRUE:
      assert e instanceof RexCall;
      call = (RexCall) e;
      return reuse(e, isNotTrue(call.getOperands().get(0)));

    case IS_NOT_FALSE:
      assert e instanceof RexCall;
      call = (RexCall) e;
      return reuse(e, isNotFalse(call.getOperands().get(0)));

    case IS_NOT_NULL:
      assert e instanceof RexCall;
      call = (RexCall) e;
      return reuse(e, isNotNull(call.getOperands().get(0)));

    case AND:
      assert e instanceof RexCall;
      call = (RexCall) e;
      // Generate:
      //   CASE
      //   WHEN a0 IS UNKNOWN OR a1 IS UNKNOWN OR a2 IS UNKNOWN
      //   THEN CASE
      //        WHEN a0 IS FALSE OR a1 IS FALSE OR a2 IS FALSE
      //        THEN FALSE
      //        ELSE UNKNOWN
      //        END
      //   ELSE a0 IS TRUE AND a1 IS TRUE AND a2 IS TRUE
      //   END
      newOperands = valueList(call.getOperands());
      isUnknowns = new ArrayList<>();
      isFalses = new ArrayList<>();
      isTrues = new ArrayList<>();
      for (RexNode operand : newOperands) {
        if (mayBeNull(operand)) {
          isUnknowns.add(isNull(operand));
        }
        isFalses.add(isFalse(operand));
      }
      try (Mark ignore = mark()) {
        for (RexNode operand : newOperands) {
          registerNotNull(operand);
        }
        for (RexNode operand : newOperands) {
          isTrues.add(isTrue(operand));
        }
      }
      return case_(RexUtil.composeDisjunction(rexBuilder, isUnknowns),
          case_(RexUtil.composeDisjunction(rexBuilder, isFalses),
              rexBuilder.makeLiteral(false),
              rexBuilder.makeNullLiteral(SqlTypeName.BOOLEAN)),
          RexUtil.composeConjunction(rexBuilder, isTrues));

    case OR:
      assert e instanceof RexCall;
      call = (RexCall) e;
      // Generate:
      //   CASE
      //   WHEN a0 IS UNKNOWN
      //   THEN CASE
      //        WHEN a1 IS UNKNOWN
      //        THEN UNKNOWN
      //        WHEN CAST(a1 AS BOOLEAN)
      //        THEN TRUE
      //        ELSE UNKNOWN
      //        END
      //   WHEN CAST(a0 AS BOOLEAN)
      //   THEN TRUE
      //   ELSE a1
      //   END
      final List<RexNode> operands = call.getOperands();
      final RexNode left = operands.get(0);
      final RexNode right;
      if (call.getOperands().size() > 2) {
        right = rexBuilder.makeCall(SqlStdOperatorTable.OR, Util.skip(operands));
      } else {
        right = operands.get(1);
      }
      return case_(isNull(left),
          case_(isNull(right), rexBuilder.constantNull(),
              ensureNotNull(right), rexBuilder.makeLiteral(true),
              rexBuilder.constantNull()),
          ensureNotNull(left), rexBuilder.makeLiteral(true),
          value(right));

    case CASE:
      assert e instanceof RexCall;
      call = (RexCall) e;
      newOperands = new ArrayList<>();
      for (Ord<RexNode> operand : Ord.zip(call.getOperands())) {
        if (RexUtil.isCasePredicate(call, operand.i)) {
          newOperands.add(isTrue(operand.e));
        } else {
          newOperands.add(cast(operand.e, call.type));
        }
      }
      return case_(newOperands);

    case CAST:
      if (((RexCall) e).type.equals(((RexCall) e).operands.get(0).getType())) {
        return ((RexCall) e).operands.get(0);
      }
      // fall through

    default:
      if (e instanceof RexCall) {
        call = (RexCall) e;
        if (RexUtil.isStrict(call.getOperator())) {
          final List<RexNode> isNotNulls = new ArrayList<>();
          newOperands = new ArrayList<>();
          try (Mark ignore = mark()) {
            gatherIsNotNulls(isNotNulls, e);
            for (RexNode operand : values(call.getOperands())) {
              if (RexUtil.isNull(operand)) {
                return rexBuilder.constantNull();
              }
              newOperands.add(ensureNotNull(operand));
            }
          }
          if (isNotNulls.isEmpty()) {
            if (newOperands.equals(call.getOperands())) {
              return e;
            } else {
              return call.clone(notNullable(call.type), newOperands);
            }
          }
          // Generate:
          //   CASE
          //   WHEN a0 IS NULL THEN NULL
          //   WHEN a2 IS NULL THEN NULL
          //   ELSE op(CAST_NOT_NULL(a0), arg1, CAST_NOT_NULL(a2))
          //   END
          final List<RexNode> list = new ArrayList<>();
          for (RexNode e2 : isNotNulls) {
            list.add(isFalse(e2));
            list.add(rexBuilder.constantNull());
          }
          list.add(
              call.clone(notNullable(call.type), newOperands));
          return reuse(e,
              rexBuilder.makeCall(call.type, SqlStdOperatorTable.CASE,
                  list));
        } else {
          return call.clone(call.type, valueList(call.operands));
        }
      }
    }
    return e;
  }

  private RexNode case_(RexNode... operands) {
    return case_(ImmutableList.copyOf(operands));
  }

  private RexNode case_(List<RexNode> operands) {
    return RexUtil.simplify(rexBuilder,
        rexBuilder.makeCall(SqlStdOperatorTable.CASE, operands));
  }

  private void registerNotNull(RexNode operand) {
    notNullNodes.push(operand);
  }

  /** Returns a marker that will allow us to restore the stack to its previous
   * state. */
  private Mark mark() {
    return new Mark(notNullNodes.size());
  }

  /** Simplifies a list of expressions. */
  private List<RexNode> valueList(List<RexNode> operands) {
    final List<RexNode> list = new ArrayList<>();
    for (RexNode operand : operands) {
      list.add(value(operand));
    }
    return list.equals(operands) ? operands : list;
  }

  private Iterable<RexNode> values(Iterable<RexNode> operands) {
    return Iterables.transform(operands,
        new Function<RexNode, RexNode>() {
          public RexNode apply(RexNode e) {
            return value(e);
          }
        });
  }

  private RexNode cast(RexNode e, RelDataType type) {
    if (e.getType().equals(type)) {
      return value(e);
    } else if (!type.isNullable()
        && typeFactory.createTypeWithNullability(type, true)
          .equals(e.getType())) {
      return castNotNull(e);
    } else {
      return value(rexBuilder.makeCast(type, e, true));
    }
  }

  /** Returns "CAST(e AS t NOT NULL)", where t is e's type.
   *
   * <p>You must have previously checked that it is not null (e.g. by
   * generating "e IS NOT NULL AND ...") and registered it as not null. */
  public RexNode castNotNull(RexNode e) {
    if (e instanceof RexCall) {
      final RexCall call = (RexCall) e;
      if (RexUtil.isCoStrict(call.getOperator())) {
        final List<RexNode> newOperands = new ArrayList<>();
        try (Mark ignore = mark()) {
          for (RexNode operand : values(call.getOperands())) {
            newOperands.add(ensureNotNull(operand));
          }
        }
        return reuse(e, call.clone(notNullable(call.type), newOperands));
      }
    }
    return rexBuilder.makeNotNullCast(e);
  }

  public RexNode ensureNotNull(RexNode e) {
    if (!e.getType().isNullable()) {
      return e;
    }
    return castNotNull(e);
  }

  private RexNode isTrue(RexNode e) {
    if (e.isAlwaysTrue()) {
      return rexBuilder.makeLiteral(true);
    }
    if (e.isAlwaysFalse()) {
      return rexBuilder.makeLiteral(false);
    }
    if (!mayBeNull(e)) {
      return ensureNotNull(value(e));
    }
    final RexCall call;
    final List<RexNode> operands;
    switch (e.getKind()) {
    case AND:
      operands = new ArrayList<>();
      call = (RexCall) e;
      try (Mark ignore = mark()) {
        for (RexNode operand : call.getOperands()) {
          operands.add(isTrue(operand));
          registerNotNull(operand);
        }
      }
      return RexUtil.simplifyAnds(rexBuilder, operands);

    case OR:
      operands = new ArrayList<>();
      call = (RexCall) e;
      try (Mark ignore = mark()) {
        for (RexNode operand : call.getOperands()) {
          final RexNode operand1 = isTrue(operand);
          operands.add(operand1);
          switch (operand1.getKind()) {
          case IS_NULL:
            // After we've generated "x IS NULL OR ..." we know that x is not
            // null.
            registerNotNull(((RexCall) operand1).getOperands().get(0));
          }
          registerNotNull(operand);
        }
      }
      final RexNode e2 = RexUtil.simplifyOrs(rexBuilder, operands);
      return RexUtil.pullFactors(rexBuilder, e2);

    case NOT:
      // Rewrite: (NOT e) IS TRUE  ==>  e IS FALSE
      call = (RexCall) e;
      return isFalse(call.getOperands().get(0));

    case CASE:
      // Rewrite: (CASE WHEN c1 THEN b1 ... ELSE b END) IS TRUE
      //    ==>  CASE WHEN c1 THEN b1 IS TRUE ... ELSE b IS TRUE END
      operands = new ArrayList<>();
      call = (RexCall) e;
      try (Mark ignore = mark()) {
        for (Ord<RexNode> operand : Ord.zip(call.getOperands())) {
          operands.add(isTrue(operand.e));
        }
      }
      return call.clone(notNullable(call.type), operands);

    default:
      // Rewrite: e IS TRUE  ==>  e IS NOT NULL AND CAST_NOT_NULL(e)
      try (Mark ignore = mark()) {
        operands = new ArrayList<>();
        gatherIsNotNulls(operands, e);
        operands.add(ensureNotNull(e));
        return RexUtil.simplifyAnds(rexBuilder, operands);
      }
    }
  }

  private RexNode isNotTrue(RexNode e) {
    // Rewrite: "x IS NOT TRUE"  ==> "x IS FALSE"
    if (!mayBeNull(e)) {
      return isFalse(e);
    }
    // Rewrite: "x IS NOT TRUE" ==> "x IS NULL OR x IS FALSE"
    return isTrue(
        rexBuilder.makeCall(SqlStdOperatorTable.OR, isNull(e), isFalse(e)));
  }

  private RexNode isNotFalse(RexNode e) {
    // Rewrite: "x IS NOT FALSE"  ==> "x IS TRUE"
    if (!mayBeNull(e)) {
      return isFalse(e);
    }
    // Rewrite: "x IS NOT FALSE" ==> "x IS NULL OR x IS TRUE"
    return isTrue(
        rexBuilder.makeCall(SqlStdOperatorTable.OR, isNull(e), isTrue(e)));
  }

  private RexNode isFalse(RexNode e) {
    if (e.isAlwaysFalse()) {
      return rexBuilder.makeLiteral(true);
    }
    if (e.isAlwaysTrue()) {
      return rexBuilder.makeLiteral(false);
    }
    if (!mayBeNull(e)) {
      switch (e.getKind()) {
      case IS_NOT_NULL:
      case IS_NULL:
        return isTrue(negate((RexCall) e));
      default:
        return RexUtil.not(ensureNotNull(e));
      }
    }
    final RexCall call;
    final List<RexNode> operands;
    switch (e.getKind()) {
    case AND:
      // Rewrite: (x AND y) IS FALSE  ==>  (x IS FALSE) OR (y IS FALSE)
      operands = new ArrayList<>();
      call = (RexCall) e;
      try (Mark ignore = mark()) {
        for (RexNode operand : call.getOperands()) {
          operands.add(isFalse(operand));
          registerNotNull(operand);
        }
      }
      return RexUtil.simplifyOrs(rexBuilder, operands);

    case OR:
      // Rewrite: (x OR y) IS FALSE  ==>  (x IS FALSE) AND (y IS FALSE)
      operands = new ArrayList<>();
      call = (RexCall) e;
      try (Mark ignore = mark()) {
        for (RexNode operand : call.getOperands()) {
          operands.add(isFalse(operand));
          registerNotNull(operand);
        }
      }
      return RexUtil.simplifyAnds(rexBuilder, operands);

    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case EQUALS:
    case NOT_EQUALS:
    case IS_DISTINCT_FROM:
    case IS_NOT_DISTINCT_FROM:
      call = (RexCall) e;
      return isTrue(negate(call));

    case NOT:
      // Rewrite: (NOT e) IS FALSE  ==>  e IS TRUE
      call = (RexCall) e;
      return isTrue(call.getOperands().get(0));

    case CASE:
      // Rewrite: (CASE WHEN c1 THEN b1 ... ELSE b END) IS FALSE
      //    ==>  CASE WHEN c1 THEN b1 IS FALSE ... ELSE b IS FALSE END
      operands = new ArrayList<>();
      call = (RexCall) e;
      try (Mark ignore = mark()) {
        for (Ord<RexNode> operand : Ord.zip(call.getOperands())) {
          operands.add(
              RexUtil.isCasePredicate(call, operand.i) ? isTrue(operand.e)
                  : isFalse(operand.e));
        }
      }
      return call.clone(notNullable(call.type), operands);

    default:
      // Rewrite: e IS FALSE  ==>  e IS NOT NULL AND NOT(CAST_NOT_NULL(e))
      try (Mark ignore = mark()) {
        operands = new ArrayList<>();
        gatherIsNotNulls(operands, e);
        operands.add(RexUtil.not(castNotNull(e)));
        return RexUtil.simplifyAnds(rexBuilder, operands);
      }
    }
  }

  private RexNode negate(RexCall call) {
    return rexBuilder.makeCall(call.type, RexUtil.op(call.getKind().negate()),
        call.getOperands());
  }

  private boolean mayBeNull(RexNode e) {
    return e.getType().isNullable()
        && !notNullNodes.contains(e);
  }

  /** Efficiently translates "operand IS [NOT] NULL". */
  private RexNode isXxxNull(RexNode e, boolean negate) {
    if (!mayBeNull(e)) {
      // If x is not nullable, rewrite
      //   "x IS NOT NULL" ==> "TRUE"
      //   "x IS NULL" ==> "FALSE"
      return rexBuilder.makeLiteral(negate);
    }
  strict:
    if (e instanceof RexCall) {
      final RexCall call = (RexCall) e;
      final List<RexNode> list = new ArrayList<>();
      if (RexUtil.isCoStrict(call.getOperator())) {
        for (RexNode operand : call.getOperands()) {
          list.add(isXxxNull(operand, negate));
        }
      } else {
        break strict;
      }
      // If f is co-strict, rewrite
      //   "f(x, y) IS NOT NULL" ==> "x IS NOT NULL AND y IS NOT NULL"
      //   "f(x, y) IS NULL" ==> "x IS NULL OR y IS NULL"
      return negate
          ? RexUtil.simplifyAnds(rexBuilder, list)
          : RexUtil.simplifyOrs(rexBuilder, list);
    }
    final SqlPostfixOperator op = negate
        ? SqlStdOperatorTable.IS_NOT_NULL
        : SqlStdOperatorTable.IS_NULL;
    return rexBuilder.makeCall(op, e);
  }

  /** Efficiently translates "operand IS NOT NULL". */
  private RexNode isNotNull(RexNode e) {
    return isXxxNull(e, true);
  }

  /** Efficiently translates "operand IS NULL". */
  private RexNode isNull(RexNode e) {
    return isXxxNull(e, false);
  }

  /** Populates a list of predicates checking that an expression is not null.
   *
   * <p>If e is a call to a strict operator Foo(x, y), then we populate
   * {@code x IS NOT NULL AND y IS NOT NULL} rather than
   * {@code Foo(x, y) IS NOT NULL}.
   *
   * <p>If e is
   * {@code CASE WHEN c THEN NULL WHEN c2 THEN notNull ELSE NULL END}
   * then we populate {@code c OR }
   * </p>*/
  private void gatherIsNotNulls(List<RexNode> list, RexNode e) {
    if (mayBeNull(e)) {
      if (e instanceof RexCall) {
        final RexCall call = (RexCall) e;
        if (RexUtil.isCoStrict(call.getOperator())) {
          for (RexNode operand : call.getOperands()) {
            gatherIsNotNulls(list, operand);
          }
        } else {
          list.add(isNotNull(e));
        }
      } else {
        list.add(isNotNull(e));
      }
      registerNotNull(e);
    }
  }

  RelDataType notNullable(RelDataType type) {
    return typeFactory.createTypeWithNullability(type, false);
  }

  /** Marker that allows you to restore the stack of known-not-null
   * expressions. */
  private class Mark implements AutoCloseable {
    private final int originalSize;

    public Mark(int originalSize) {
      this.originalSize = originalSize;
    }

    public void close() {
      while (notNullNodes.size() > originalSize) {
        notNullNodes.pop();
      }
    }
  }
}

// End NullSafeVisitor.java
