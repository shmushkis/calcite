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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Cassandra.
 */
public class CassandraFilter extends Filter implements CassandraRel {
  private final List<String> partitionKeys;
  private Boolean singlePartition;

  public CassandraFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition,
      List<String> partitionKeys) {
    super(cluster, traitSet, child, condition);

    this.partitionKeys = partitionKeys;
    this.singlePartition = false;

    assert getConvention() == CassandraRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public CassandraFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new CassandraFilter(getCluster(), traitSet, input, condition, partitionKeys);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    Translator translator =
        new Translator(CassandraRules.cassandraFieldNames(getRowType()), partitionKeys);
    String match = translator.translateMatch(condition);
    singlePartition = translator.isSinglePartition();
    implementor.add(null, Collections.singletonList(match));
  }

  /** Check if the filter restricts to a single partition.
   *
   * @return True if the filter will restrict the underlying to a single partition
   */
  public boolean isSinglePartition() {
    return singlePartition;
  }

  /** Translates {@link RexNode} expressions into Cassandra expression strings. */
  static class Translator {
    private final List<String> fieldNames;
    private final Set<String> partitionKeys;

    Translator(List<String> fieldNames, List<String> partitionKeys) {
      this.fieldNames = fieldNames;
      this.partitionKeys = new HashSet<String>(partitionKeys);
    }

    /** Check if the query spans only one partition.
     *
     * @return True if the matches translated so far have resulted in a single partition
     */
    public boolean isSinglePartition() {
      return partitionKeys.isEmpty();
    }

    /** Produce the CQL predicate string for the given condition.
     *
     * @param condition Condition to translate
     * @return CQL predicate string
     */
    private String translateMatch(RexNode condition) {
      // CQL does not support disjunctions
      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() == 1) {
        return translateAnd(disjunctions.get(0));
      } else {
        throw new AssertionError("cannot translate " + condition);
      }
    }

    /** Conver the value of a literal to a string.
     *
     * @param literal Literal to translate
     * @return String representation of the literal
     */
    private static String literalValue(RexLiteral literal) {
      Object value = literal.getValue2();
      StringBuilder buf = new StringBuilder();
      buf.append(value);
      return buf.toString();
    }

    /** Translate a conjunctive predicate to a CQL string.
     *
     * @param condition A conjunctive predicate
     * @return CQL string for the predicate
     */
    private String translateAnd(RexNode condition) {
      List<String> predicates = new ArrayList<String>();
      for (RexNode node : RelOptUtil.conjunctions(condition)) {
        predicates.add(translateMatch2(node));
      }

      return Util.toString(predicates, "", " AND ", "");
    }

    /** Translate a binary relation. */
    private String translateMatch2(RexNode node) {
      // We currently only use equality, but inequalities on clustering keys
      // should be possible in the future
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary("=", "=", (RexCall) node);
      case LESS_THAN:
        return translateBinary("<", ">", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("<=", ">=", (RexCall) node);
      case GREATER_THAN:
        return translateBinary(">", "<", (RexCall) node);
      case GREATER_THAN_OR_EQUAL:
        return translateBinary(">=", "<=", (RexCall) node);
      default:
        throw new AssertionError("cannot translate " + node);
      }
    }

    /** Translates a call to a binary operator, reversing arguments if
     * necessary. */
    private String translateBinary(String op, String rop, RexCall call) {
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);
      String expression = translateBinary2(op, left, right);
      if (expression != null) {
        return expression;
      }
      expression = translateBinary2(rop, right, left);
      if (expression != null) {
        return expression;
      }
      throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /** Translates a call to a binary operator. Returns null on failure. */
    private String translateBinary2(String op, RexNode left, RexNode right) {
      switch (right.getKind()) {
      case LITERAL:
        break;
      default:
        return null;
      }
      final RexLiteral rightLiteral = (RexLiteral) right;
      switch (left.getKind()) {
      case INPUT_REF:
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        return translateOp2(op, name, rightLiteral);
      case CAST:
        // XXX: This might not actually work, needs testing
        return translateBinary2(op, ((RexCall) left).operands.get(0), right);
      default:
        return null;
      }
    }

    /** Combines a field name, operator, and literal to produce a predicate string. */
    private String translateOp2(String op, String name, RexLiteral right) {
      // In case this is a partition key, record that it is now restricted
      if (op.equals("=")) {
        partitionKeys.remove(name);
      }
      return name + " " + op + " " + literalValue(right);
    }
  }
}

// End CassandraFilter.java
