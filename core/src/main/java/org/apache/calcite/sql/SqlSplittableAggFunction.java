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
package org.apache.calcite.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate function that can be split into partial aggregates.
 *
 * <p>For example, {@code COUNT(x)} can be split into {@code COUNT(x)} on
 * subsets followed by {@code SUM} to combine those counts.
 */
public interface SqlSplittableAggFunction {
  AggregateCall split(AggregateCall aggregateCall,
      Mappings.TargetMapping offset);

  /** Generates an aggregate call to merge sub-totals.
   *
   * <p>Most implementations will add a single aggregate call to
   * {@code aggCalls}, and return a {@link RexInputRef} that points to it.
   *
   * @param rexBuilder Rex builder
   * @param extra Place to define extra input expressions
   * @param offset Offset due to grouping columns (and indicator columns if
   *     applicable)
   * @param aggregateCall Source aggregate call
   * @param leftSubTotal Ordinal of the sub-total coming from the left side of
   *     the join, or -1 if there is no such sub-total
   * @param rightSubTotal Ordinal of the sub-total coming from the right side
   *     of the join, or -1 if there is no such sub-total
   * @return Aggregate call
   */
  AggregateCall topSplit(RexBuilder rexBuilder, Registry<RexNode> extra,
      int offset, AggregateCall aggregateCall, int leftSubTotal,
      int rightSubTotal);

  RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
      AggregateCall aggregateCall);

  /** Splitting strategy where a function splits into two copies of itself.
   *
   * <p>SUM, MIN, and MAX work this way.
   * COUNT does not (it splits into a COUNT followed by a SUM).
   */
  SqlSplittableAggFunction SELF =
      new SqlSplittableAggFunction() {
        public RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
            AggregateCall aggregateCall) {
          final int arg = aggregateCall.getArgList().get(0);
          return rexBuilder.makeInputRef(inputRowType.getFieldList().get(arg).getType(), arg);
        }

        public AggregateCall split(AggregateCall aggregateCall,
            Mappings.TargetMapping offset) {
          return aggregateCall.transform(offset);
        }

        public AggregateCall topSplit(RexBuilder rexBuilder,
            Registry<RexNode> extra, int offset, AggregateCall aggregateCall,
            int leftSubTotal, int rightSubTotal) {
          assert (leftSubTotal >= 0) != (rightSubTotal >= 0);
          final int arg = leftSubTotal >= 0 ? leftSubTotal : rightSubTotal;
          return aggregateCall.copy(ImmutableIntList.of(arg), -1);
        }
      };

  /** Splitting strategy where a function is split into itself followed by
   * a given function.
   *
   * <p>COUNT, for example, is split into itself followed by SUM. (Actually
   * SUM0, because the total needs to be 0, not null, if there are 0 rows.)
   * This rule works for any number of arguments to COUNT, including COUNT(*).
   */
  SqlSplittableAggFunction COUNT = new CountSplitter();

  /** Splitting strategy where a function is split into itself followed by
   * a given function.
   *
   * <p>COUNT, for example, is split into itself followed by SUM. (Actually
   * SUM0, because the total needs to be 0, not null, if there are 0 rows.)
   * This rule works for any number of arguments to COUNT, including COUNT(*).
   */
  abstract class MergeableAggFunction implements SqlSplittableAggFunction {
    private final SqlAggFunction merge;

    public MergeableAggFunction(SqlAggFunction merge) {
      this.merge = merge;
    }

    public AggregateCall split(AggregateCall aggregateCall,
        Mappings.TargetMapping offset) {
      return aggregateCall.transform(offset);
    }

    public AggregateCall topSplit(RexBuilder rexBuilder,
        Registry<RexNode> extra, int offset, AggregateCall aggregateCall,
        int leftSubTotal, int rightSubTotal) {
      final List<RexNode> merges = new ArrayList<>();
      if (leftSubTotal >= 0) {
        merges.add(
            rexBuilder.makeInputRef(aggregateCall.type, leftSubTotal));
      }
      if (rightSubTotal >= 0) {
        merges.add(
            rexBuilder.makeInputRef(aggregateCall.type, rightSubTotal));
      }
      RexNode node;
      switch (merges.size()) {
      case 1:
        node = merges.get(0);
        break;
      case 2:
        node = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges);
        break;
      default:
        throw new AssertionError("unexpected count " + merges);
      }
      int ordinal = extra.register(node);
      return AggregateCall.create(merge, false, ImmutableList.of(ordinal),
          -1, aggregateCall.type, aggregateCall.name);
    }
  }

  /** Collection in which one can register an element. Registering may return
   * a reference to an existing element. */
  interface Registry<E> {
    int register(E e);
  }

  /** Splitting strategy for {@code COUNT}. */
  class CountSplitter extends MergeableAggFunction {
    public CountSplitter() {
      super(SqlStdOperatorTable.SUM0);
    }

    /**
     * {@inheritDoc}
     *
     * COUNT(*) and COUNT applied to all NOT NULL arguments become {@code 1};
     * otherwise {@code CASE WHEN arg0 IS NOT NULL THEN 1 ELSE 0 END}.
     */
    public RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
        AggregateCall aggregateCall) {
      final List<RexNode> predicates = new ArrayList<>();
      for (Integer arg : aggregateCall.getArgList()) {
        final RelDataType type = inputRowType.getFieldList().get(arg).getType();
        if (type.isNullable()) {
          predicates.add(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
                  rexBuilder.makeInputRef(type, arg)));
        }
      }
      final RexNode predicate =
          RexUtil.composeConjunction(rexBuilder, predicates, true);
      if (predicate == null) {
        return rexBuilder.makeExactLiteral(BigDecimal.ONE);
      } else {
        return rexBuilder.makeCall(SqlStdOperatorTable.CASE, predicate,
            rexBuilder.makeExactLiteral(BigDecimal.ONE),
            rexBuilder.makeExactLiteral(BigDecimal.ZERO));
      }
    }
  }
}

// End SqlSplittableAggFunction.java
