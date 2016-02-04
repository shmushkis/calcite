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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.util.AbstractList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Rules and relational operators for
 * {@link CassandraRel#CONVENTION}
 * calling convention.
 */
public class CassandraRules {
  private CassandraRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final RelOptRule[] RULES = {
    CassandraFilterRule.INSTANCE,
    CassandraProjectRule.INSTANCE
  };

  static List<String> cassandraFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override public String get(int index) {
            return rowType.getFieldList().get(index).getName();
          }

          @Override public int size() {
            return rowType.getFieldCount();
          }
        });
  }

  /** Translator from {@link RexNode} to strings in Cassandra's expression
   * language. */
  static class RexToCassandraTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    protected RexToCassandraTranslator(JavaTypeFactory typeFactory,
        List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /** Base class for planner rules that convert a relational expression to
   * Cassandra calling convention. */
  abstract static class CassandraConverterRule extends ConverterRule {
    protected final Convention out;

    public CassandraConverterRule(
        Class<? extends RelNode> clazz,
        String description) {
      this(clazz, Predicates.<RelNode>alwaysTrue(), description);
    }

    public <R extends RelNode> CassandraConverterRule(
        Class<R> clazz,
        Predicate<? super R> predicate,
        String description) {
      super(clazz, predicate, Convention.NONE, CassandraRel.CONVENTION, description);
      this.out = CassandraRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link CassandraFilter}.
   */
  private static class CassandraFilterRule extends RelOptRule {
    private static final Predicate<LogicalFilter> PREDICATE =
        new Predicate<LogicalFilter>() {
          public boolean apply(LogicalFilter input) {
            // TODO: Check for an equality predicate on the partition key
            // Right now this just checks if we have a single top-level AND
            return RelOptUtil.disjunctions(input.getCondition()).size() == 1;
          }
        };

    private static final CassandraFilterRule INSTANCE = new CassandraFilterRule();

    private CassandraFilterRule() {
      super(operand(LogicalFilter.class, operand(CassandraTableScan.class, none())),
          "CassandraFilterRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();

      CassandraTableScan scan = call.rel(1);
      List<String> keyFields = scan.cassandraTable.getKeyFields();
      List<String> fieldNames = CassandraRules.cassandraFieldNames(filter.getInput().getRowType());

      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() != 1) {
        return false;
      } else {
        condition = disjunctions.get(0);
        for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
          if (!isEqualityOnKey(predicate, fieldNames, keyFields)) {
            return false;
          }
        }
      }

      return true;
    }

    private boolean isEqualityOnKey(RexNode node, List<String> fieldNames,
        List<String> keyFields) {
      if (node.getKind() != SqlKind.EQUALS) {
        return false;
      }

      RexCall call = (RexCall) node;
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      return isCompareKeyFieldWithLiteral(left, right, fieldNames, keyFields)
          || isCompareKeyFieldWithLiteral(right, left, fieldNames, keyFields);
    }

    private boolean isCompareKeyFieldWithLiteral(RexNode left, RexNode right,
        List<String> fieldNames, List<String> keyFields) {
      if (left.getKind() == SqlKind.INPUT_REF && right.getKind() == SqlKind.LITERAL) {
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        return keyFields.contains(name);
      } else {
        return false;
      }
    }

    /* @see org.apache.calcite.rel.convert.ConverterRule */
    public void onMatch(RelOptRuleCall call) {
      RelNode rel = call.rel(0);
      if (rel.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(rel);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(CassandraRel.CONVENTION);
      return new CassandraFilter(
          rel.getCluster(),
          traitSet,
          convert(filter.getInput(), CassandraRel.CONVENTION),
          filter.getCondition());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link CassandraProject}.
   */
  private static class CassandraProjectRule extends CassandraConverterRule {
    private static final CassandraProjectRule INSTANCE = new CassandraProjectRule();

    private CassandraProjectRule() {
      super(LogicalProject.class, "CassandraProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new CassandraProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(),
          project.getRowType());
    }
  }
}

// End CassandraRules.java
