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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that removes
 * a {@link org.apache.calcite.rel.core.Sort} if its input is already sorted.
 *
 * <p>Requires {@link RelCollationTraitDef}.
 */
public class SortRemoveRedundant extends RelOptRule {
  public static final SortRemoveRedundant INSTANCE = new SortRemoveRedundant();

  private SortRemoveRedundant() {
    super(
        operand(Sort.class, any()),
        "SortRemoveRedundant");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPredicateList predicates =
            mq.getPulledUpPredicates(sort.getInput());
    if (predicates == null) {
      return;
    }

    final RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
    final List<RexNode> inputList = sort.getChildExps();

    final List<RelFieldCollation> collationsList = new ArrayList<>();
    for (RexNode currentNode : sort.getChildExps()) {
      //TODO: Figure if there is a safer way of getting RexInputRef
      RexInputRef ref = (RexInputRef) currentNode;

      if (!(predicates.constantMap.containsKey(ref))) {
        collationsList.add(new RelFieldCollation(ref.getIndex()));
      }
    }

    if (sort.offset != null || sort.fetch != null) {
      // Don't remove sort if would also remove OFFSET or LIMIT.
      return;
    }

    // None of the group expressions are constant. Nothing to do.
    if (collationsList.isEmpty()) {
      final RelCollation collation = sort.getCollation();
      final RelTraitSet traits = sort.getInput().getTraitSet().replace(collation);

      call.transformTo(convert(sort.getInput(), traits));

      return;

    }

    final RelCollation newCollation =  RelCollations.of(collationsList);

    final RelTraitSet traits = sort.getInput().getTraitSet().replace(newCollation);
    Sort result = sort.copy(sort.getTraitSet(), sort.getInput(), newCollation,
            sort.offset, sort.fetch);

    call.transformTo(result);
  }
}

// End SortRemoveRedundant.java
