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

import org.apache.calcite.rel.core.RelFactories;

/**
 * Planner rule that introduces redundant
 * {@link org.apache.calcite.rel.core.SemiJoin}s
 * where they would reduce the overall cost of the plan.
 *
 * <p>It is triggered by the pattern
 * {@link org.apache.calcite.rel.logical.LogicalProject}
 * ({@link org.apache.calcite.rel.rules.MultiJoin}).
 */
public class SemiJoinReductionRule extends LoptOptimizeJoinRule {
  public static final SemiJoinReductionRule INSTANCE =
      new SemiJoinReductionRule(
          RelFactories.DEFAULT_JOIN_FACTORY,
          RelFactories.DEFAULT_SEMI_JOIN_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY,
          RelFactories.DEFAULT_FILTER_FACTORY);

  /** Creates a SemiJoinReductionRule. */
  public SemiJoinReductionRule(
      RelFactories.JoinFactory joinFactory,
      RelFactories.SemiJoinFactory semiJoinFactory,
      RelFactories.ProjectFactory projectFactory,
      RelFactories.FilterFactory filterFactory) {
    super(joinFactory, semiJoinFactory, projectFactory, filterFactory, true);
  }
}

// End SemiJoinReductionRule.java
