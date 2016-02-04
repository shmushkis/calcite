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
import java.util.List;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Cassandra.
 */
public class CassandraFilter extends Filter implements CassandraRel {
  public CassandraFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == CassandraRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public CassandraFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new CassandraFilter(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    Translator translator =
        new Translator(CassandraRules.cassandraFieldNames(getRowType()));
    String match = translator.translateOr(condition);
    implementor.add(null, Collections.singletonList(match));
  }

  /** Translates {@link RexNode} expressions into Cassandra expression strings. */
  static class Translator {
    private final List<String> fieldNames;

    Translator(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    private String translateMatch(RexNode condition) {
      return translateOr(condition);
    }

    private String translateOr(RexNode condition) {
      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() == 1) {
        return translateAnd(disjunctions.get(0));
      } else {
        throw new AssertionError("cannot translate " + condition);
      }
    }

    private static String literalValue(RexLiteral literal) {
      Object value = literal.getValue2();
      StringBuilder buf = new StringBuilder();
      buf.append(value);
      return buf.toString();
    }

    private String translateAnd(RexNode condition) {
      List<String> predicates = new ArrayList<String>();
      for (RexNode node : RelOptUtil.conjunctions(condition)) {
        predicates.add(translateMatch2(node));
      }

      return Util.toString(predicates, "", " AND ", "");
    }

    private String translateMatch2(RexNode node) {
      switch (node.getKind()) {
      case EQUALS:
        return translateBinary("=", "=", (RexCall) node);
      case LESS_THAN:
        return translateBinary("<", ">", (RexCall) node);
      case LESS_THAN_OR_EQUAL:
        return translateBinary("<=", ">=", (RexCall) node);
      case NOT_EQUALS:
        return translateBinary("!=", "!=", (RexCall) node);
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
        return translateBinary2(op, ((RexCall) left).operands.get(0), right);
      default:
        return null;
      }
    }

    private String translateOp2(String op, String name, RexLiteral right) {
      return name + " " + op + " " + literalValue(right);
    }
  }
}

// End CassandraFilter.java
