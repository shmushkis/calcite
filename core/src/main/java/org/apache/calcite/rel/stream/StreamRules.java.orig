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
package org.apache.calcite.rel.stream;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;

/**
 * Rules and relational operators for streaming relational expressions.
 */
public class StreamRules {
  private StreamRules() {}

  public static final ImmutableList<RelOptRule> RULES =
      ImmutableList.of(
          new DeltaProjectTransposeRule(),
          new DeltaFilterTransposeRule(),
          new DeltaTableScanRule());

  /** Planner rule that pushes a {@link Delta} through a {@link Project}. */
  public static class DeltaProjectTransposeRule extends RelOptRule {
    public DeltaProjectTransposeRule() {
      super(
          operand(Delta.class,
              operand(Project.class, any())));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      final Project project = call.rel(1);
      final RelOptCluster cluster = delta.getCluster();
      final LogicalDelta newDelta =
          new LogicalDelta(cluster, delta.getTraitSet(), project.getInput());
      final LogicalProject newProject =
          new LogicalProject(cluster, newDelta, project.getProjects(),
              project.getRowType().getFieldNames(), project.getFlags());
      call.transformTo(newProject);
    }
  }

  /** Planner rule that pushes a {@link Delta} through a {@link Filter}. */
  public static class DeltaFilterTransposeRule extends RelOptRule {
    public DeltaFilterTransposeRule() {
      super(
          operand(Delta.class,
              operand(Filter.class, any())));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      final Filter filter = call.rel(1);
      final RelOptCluster cluster = delta.getCluster();
      final LogicalDelta newDelta =
          new LogicalDelta(cluster, delta.getTraitSet(), filter.getInput());
      final LogicalFilter newFilter =
          new LogicalFilter(cluster, newDelta, filter.getCondition());
      call.transformTo(newFilter);
    }
  }

  /** Planner rule that pushes a {@link Delta} into a {@link TableScan} of a
   * streamable table.
   *
   * <p>Very likely, the stream was only represented as a table for uniformity
   * with the other relations in the system. The Delta disappears and the stream
   * can be implemented directly. */
  public static class DeltaTableScanRule extends RelOptRule {
    public DeltaTableScanRule() {
      super(
          operand(Delta.class,
              operand(TableScan.class, none())));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Delta delta = call.rel(0);
      final TableScan scan = call.rel(1);
      final RelOptCluster cluster = delta.getCluster();
      final RelOptTable relOptTable = scan.getTable();
      final StreamableTable streamableTable =
          relOptTable.unwrap(StreamableTable.class);
      if (streamableTable != null) {
        final Table table1 = streamableTable.stream();
        final RelOptTable relOptTable2 =
            RelOptTableImpl.create(relOptTable.getRelOptSchema(),
                relOptTable.getRowType(), table1);
        final LogicalTableScan newScan =
            LogicalTableScan.create(cluster, relOptTable2);
        call.transformTo(newScan);
      }
    }
  }
}

// End StreamRules.java
