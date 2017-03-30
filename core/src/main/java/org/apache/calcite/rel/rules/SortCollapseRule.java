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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

/**
 * Planner rule that combines two adjacent {@link Sort} operations.
 *
 * <p>Useful for queries that have an {@code ORDER BY} in a sub-query and
 * a {@code LIMIT} in an outer query, for example
 *
 * <blockquote>
 * <tt>SELECT *<br>
 * FROM (<br>
 * &nbsp;&nbsp;SELECT DISTINCT dim2<br>
 * &nbsp;&nbsp;FROM foo<br>
 * &nbsp;&nbsp;ORDER BY dim2)<br>
 * LIMIT 10</tt>
 * </blockquote>
 *
 * <p>If the inner or outer {@code Sort} has a limit or offset, the rule can
 * fire only if the outer {@code Sort}'s key is a prefix of the inner
 * {@code Sort}'s key. (This includes the case where the outer {@code Sort}'s
 * key is empty, as in the example.)
 */
public class SortCollapseRule extends RelOptRule {
  public static final SortCollapseRule INSTANCE =
      new SortCollapseRule(RelFactories.LOGICAL_BUILDER);

  /** Creates a SortCollapseRule. */
  public SortCollapseRule(RelBuilderFactory relBuilderFactory) {
    super(operand(Sort.class, operand(Sort.class, any())),
        relBuilderFactory, null);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    // First is the inner sort, second is the outer sort.
    final Sort innerSort = call.rel(1);
    final Sort outerSort = call.rel(0);

    if (Util.startsWith(innerSort.collation.getFieldCollations(),
        outerSort.collation.getFieldCollations())) {
      // Add up the offsets.
      final int firstOffset = innerSort.offset != null
          ? RexLiteral.intValue(innerSort.offset) : 0;
      final int secondOffset = outerSort.offset != null
          ? RexLiteral.intValue(outerSort.offset) : 0;

      final int fetch;

      if (innerSort.fetch == null && outerSort.fetch == null) {
        // Neither has a limit => no limit overall.
        fetch = -1;
      } else if (innerSort.fetch == null) {
        // Outer limit only.
        fetch = RexLiteral.intValue(outerSort.fetch);
      } else if (outerSort.fetch == null) {
        // Inner limit only.
        fetch = Math.max(0,
            RexLiteral.intValue(innerSort.fetch) - secondOffset);
      } else {
        fetch = Math.max(0,
            Math.min(RexLiteral.intValue(innerSort.fetch) - secondOffset,
                RexLiteral.intValue(outerSort.fetch)));
      }

      final RelNode combined = call
          .builder()
          .push(innerSort.getInput())
          .sortLimit(firstOffset + secondOffset, fetch,
              innerSort.getChildExps())
          .build();

      call.transformTo(combined);
    }
  }
}

// End SortCollapseRule.java
