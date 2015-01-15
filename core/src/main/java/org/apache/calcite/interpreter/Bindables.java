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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;

/**
 * Utilities pertaining to {@link BindableRel} and {@link BindableConvention}.
 */
public class Bindables {
  private Bindables() {}

  public static final RelOptRule BINDABLE_TABLE_RULE =
      new BindableTableScanRule();

  public static final RelOptRule BINDABLE_FILTER_RULE =
      new BindableFilterRule();

  /** Rule that converts a {@link ScannableTable} to bindable convention. */
  private static class BindableTableScanRule extends RelOptRule {
    public BindableTableScanRule() {
      super(operand(TableScan.class, none()));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final TableScan scan = call.rel(0);
      call.transformTo(
          new BindableTableScan(scan.getCluster(),
              scan.getTraitSet().replace(BindableConvention.INSTANCE),
              scan.getTable()));
    }
  }

  /** Scan of a table that implements {@link ScannableTable} and therefore can
   * be converted into an {@link Enumerable}. */
  private static class BindableTableScan
      extends TableScan implements BindableRel {
    BindableTableScan(RelOptCluster cluster, RelTraitSet traits,
        RelOptTable table) {
      super(cluster, traits, table);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      return table.unwrap(ScannableTable.class).scan(dataContext);
    }
  }

  /** Rule that converts a {@link Filter} to bindable convention. */
  private static class BindableFilterRule extends ConverterRule {
    private BindableFilterRule() {
      super(LogicalFilter.class, RelOptUtil.FILTER_PREDICATE, Convention.NONE,
          BindableConvention.INSTANCE, "BindableFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      return new BindableFilter(rel.getCluster(),
          rel.getTraitSet().replace(BindableConvention.INSTANCE),
          convert(filter.getInput(),
              filter.getInput().getTraitSet()
                  .replace(BindableConvention.INSTANCE)),
          filter.getCondition());
    }
  }

  /** Implementation of {@link org.apache.calcite.rel.core.Filter} in bindable
   * convention. */
  public static class BindableFilter extends Filter implements BindableRel {
    public BindableFilter(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode child, RexNode condition) {
      super(cluster, traitSet, child, condition);
      assert getConvention() instanceof BindableConvention;
    }

    public BindableFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new BindableFilter(getCluster(), traitSet, input, condition);
    }

    public Class<Object[]> getElementType() {
      return Object[].class;
    }

    public Enumerable<Object[]> bind(DataContext dataContext) {
      throw new UnsupportedOperationException(); // TODO:
    }
  }
}

// End Bindables.java
