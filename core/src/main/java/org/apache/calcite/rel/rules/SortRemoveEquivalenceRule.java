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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Planner rule that removes from a
 * a {@link org.apache.calcite.rel.core.Sort} any keys that are constant.
 */
public class SortRemoveEquivalenceRule extends RelOptRule {
  public static final SortRemoveEquivalenceRule INSTANCE =
      new SortRemoveEquivalenceRule();

  private SortRemoveEquivalenceRule() {
    super(
        operand(Sort.class, any()),
        "SortRemoveEquivalenceRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);

    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelOptPredicateList predicates =
        mq.getPulledUpPredicates(sort.getInput());
    if (predicates == null) {
      return;
    }

    final ImmutableMap<RexNode, RexNode> constants = predicates.constantMap;
    final BitSet constantInputs = new BitSet();
    for (RexNode constant : constants.keySet()) {
      if (constant instanceof RexInputRef) {
        constantInputs.set(((RexInputRef) constant).getIndex());
      }
    }
    final List<RelFieldCollation> fieldCollations = new ArrayList<>();
    for (RelFieldCollation fc : sort.collation.getFieldCollations()) {
      if (!constantInputs.get(fc.getFieldIndex())) {
        fieldCollations.add(fc);
      }
    }
    if (fieldCollations.size() == sort.collation.getFieldCollations().size()) {
      return;
    }
    if (fieldCollations.isEmpty()
        && sort.fetch == null
        && sort.offset == null) {
      // Sort is trival. Remove it.
      call.transformTo(sort.getInput());
    } else {
      call.transformTo(
          sort.copy(sort.getTraitSet(), sort.getInput(),
              RelCollations.of(fieldCollations)));
    }
  }
}

// End SortRemoveEquivalenceRule.java
