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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

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
  protected final RelBuilderFactory protoBuilder =
      RelBuilder.proto(Frameworks.newConfigBuilder().build().getContext());

  public static final SubQueryRemoveRule PROJECT =
      new SubQueryRemoveRule(
          operand(Project.class, null, RexUtil.SubQueryFinder.PROJECT_PREDICATE,
              any()), "SubQueryRemoveRule:Project") {
        public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          apply(call, project);
        }
      };

  public static final SubQueryRemoveRule FILTER =
      new SubQueryRemoveRule(
          operand(Filter.class, null, RexUtil.SubQueryFinder.FILTER_PREDICATE,
              any()), "SubQueryRemoveRule:Filter") {
        public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          apply(call, project);
        }
      };

  public static final SubQueryRemoveRule JOIN =
      new SubQueryRemoveRule(
          operand(Join.class, null, RexUtil.SubQueryFinder.JOIN_PREDICATE,
              any()), "SubQueryRemoveRule:Join") {
        public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          apply(call, project);
        }
      };

  private SubQueryRemoveRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  protected void apply(final RelOptRuleCall call, Project project) {
    final RelBuilder builder = call.builder();
    final RexSubQuery e = RexUtil.SubQueryFinder.find(project.getProjects());
    assert e != null;
    final RexShuttle shuttle;
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
      builder.push(project.getInput());
      final int leftCount = builder.peek().getRowType().getFieldCount();
      builder.join(JoinRelType.LEFT);
      shuttle = new RexShuttle() {
        @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
          return builder.field(leftCount);
        }
      };
      break;

    case IN:
      shuttle = null;
      break;

    default:
      throw new AssertionError(e.getKind());
    }
    builder.project(shuttle.apply(project.getProjects()),
        project.getRowType().getFieldNames());
    call.transformTo(builder.build());
  }
}

// End SubQueryRemoveRule.java
