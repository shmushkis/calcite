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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 *
 * <p>Sub-queries are represented by {@link RexSubQuery} expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped {@link RelNode} will contain a {@link RexCorrelVariable} before
 * the rewrite, and the product of the rewrite will be a {@link Correlate}.
 * The Correlate can be removed using {@link RelDecorrelator}.
 */
public abstract class SubQueryRemoveRule extends RelOptRule {
  public static final SubQueryRemoveRule PROJECT =
      new SubQueryRemoveRule(
          operand(Project.class, null, RexUtil.SubQueryFinder.PROJECT_PREDICATE,
              any()),
          RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Project") {
        public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          apply(call, project);
        }
      };

  public static final SubQueryRemoveRule FILTER =
      new SubQueryRemoveRule(
          operand(Filter.class, null, RexUtil.SubQueryFinder.FILTER_PREDICATE,
              any()),
          RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Filter") {
        public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          apply(call, project);
        }
      };

  public static final SubQueryRemoveRule JOIN =
      new SubQueryRemoveRule(
          operand(Join.class, null, RexUtil.SubQueryFinder.JOIN_PREDICATE,
              any()), RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Join") {
        public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          apply(call, project);
        }
      };

  private SubQueryRemoveRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory,
      String description) {
    super(operand, relBuilderFactory, description);
  }

  protected void apply(final RelOptRuleCall call, Project project) {
    final RelBuilder builder = call.builder();
    final RexSubQuery e = RexUtil.SubQueryFinder.find(project.getProjects());
    assert e != null;
    builder.push(project.getInput());
    final int leftCount = builder.peek().getRowType().getFieldCount();
    final RexNode target;
    switch (e.getKind()) {
    case SCALAR_QUERY:
      builder.push(e.rel);
      final Boolean unique = RelMetadataQuery.areColumnsUnique(builder.peek(),
          ImmutableBitSet.of());
      if (unique == null || !unique) {
        builder.aggregate(builder.groupKey(),
            builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, false, null,
                null, builder.field(0)));
      }
      builder.join(JoinRelType.LEFT);
      target = builder.field(leftCount);
      break;

    case IN:
      // Most general case, where the left and right keys might have nulls, and
      // caller requires 3-valued logic return.
      //
      // select e.deptno, e.deptno in (select deptno from emp)
      //
      // becomes
      //
      // select e.deptno,
      //   case
      //   when ct.c = 0 then false
      //   when dt.i is not null then true
      //   when e.deptno is null then null
      //   when ct.ck < ct.c then null
      //   else false
      //   end
      // from e
      // cross join (select count(*) as c, count(deptno) as ck from emp) as ct
      // left join (select distinct deptno, true as i from emp) as dt
      //   on e.deptno = dt.deptno
      //
      // If keys are not null we can remove "ct" and simplify to
      //
      // select e.deptno,
      //   case
      //   when dt.i is not null then true
      //   else false
      //   end
      // from e
      // left join (select distinct deptno, true as i from emp) as dt
      //   on e.deptno = dt.deptno
      //
      // We could further simplify to
      //
      // select e.deptno,
      //   dt.i is not null
      // from e
      // left join (select distinct deptno, true as i from emp) as dt
      //   on e.deptno = dt.deptno
      //
      // but have not yet.

      // First, the cross join
      if (e.getType().isNullable()) {
        builder.push(e.rel);
        builder.aggregate(builder.groupKey(),
            builder.count(false, "c"),
            builder.aggregateCall(SqlStdOperatorTable.COUNT, false, null, "ck",
                builder.fields()));
        builder.as("ct");
        builder.join(JoinRelType.INNER);
      }

      // Now the left join
      builder.push(e.rel);
      final List<RexNode> fields = new ArrayList<>(builder.fields());
      fields.add(builder.alias(builder.literal(true), "i"));
      builder.project(fields);
      builder.distinct();
      builder.as("dt");
      final List<RexNode> conditions = new ArrayList<>();
      for (Pair<RexNode, RexNode> pair
          : Pair.zip(e.getOperands(), builder.fields())) {
        conditions.add(
            builder.equals(pair.left, RexUtil.shift(pair.right, leftCount)));
      }
      builder.join(JoinRelType.LEFT, builder.and(conditions));

      final List<RexNode> keyIsNulls = new ArrayList<>();
      for (RexNode operand : e.getOperands()) {
        if (operand.getType().isNullable()) {
          keyIsNulls.add(builder.isNull(operand));
        }
      }
      final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
      if (e.getType().isNullable()) {
        operands.add(
            builder.equals(builder.field("ct", "c"), builder.literal(0)),
            builder.literal(false));
      }
      operands.add(builder.isNotNull(builder.field("dt", "i")),
          builder.literal(true));
      if (!keyIsNulls.isEmpty()) {
        operands.add(builder.or(keyIsNulls), builder.literal(null));
      }
      if (e.getType().isNullable()) {
        operands.add(
            builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field("ct", "ck"), builder.field("ct", "c")),
            builder.literal(null));
      }
      operands.add(builder.literal(false));
      target = builder.call(SqlStdOperatorTable.CASE, operands.build());
      break;

    default:
      throw new AssertionError(e.getKind());
    }
    final RexShuttle shuttle = new RexShuttle() {
      @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
        return target;
      }
    };
    builder.project(shuttle.apply(project.getProjects()),
        project.getRowType().getFieldNames());
    call.transformTo(builder.build());
  }
}

// End SubQueryRemoveRule.java
