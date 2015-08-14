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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Root;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link Root} in
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableRoot extends Root implements EnumerableRel {
  /**
   * Creates an EnumerableRoot.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traitSet  Traits of this relational expression
   * @param input     Input relational expression
   * @param fields    Field references and names
   * @param collation Fields of underlying relational expression sorted on
   */
  public EnumerableRoot(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<Pair<Integer, String>> fields,
      RelCollation collation) {
    super(cluster, traitSet, input, fields, collation);
    assert getConvention() instanceof EnumerableConvention;
  }

  /**
   * Creates an EnumerableRoot by parsing serialized output.
   */
  public EnumerableRoot(RelInput input) {
    super(input);
  }

  /** Creates an EnumerableRoot. */
  public static EnumerableRoot create(RelNode input,
      List<Pair<Integer, String>> fields, RelCollation collation) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.getPlanner().emptyTraitSet()
        .plus(EnumerableConvention.INSTANCE);
    return new EnumerableRoot(cluster, traitSet, input, fields, collation);
  }

  @Override public Root copy(RelTraitSet traitSet, RelNode input) {
    return new EnumerableRoot(getCluster(), traitSet, input, fields, collation);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final EnumerableRel input = (EnumerableRel) getInput();
    if (isRefTrivial()) {
      return input.implement(implementor, pref);
    }
    final List<RexNode> projects = new ArrayList<>();
    final RexBuilder rexBuilder = getCluster().getRexBuilder();
    for (Pair<Integer, String> field : fields) {
      projects.add(rexBuilder.makeInputRef(input, field.left));
    }
    RexProgram program = RexProgram.create(input.getRowType(),
        projects, null, rowType, rexBuilder);
    return EnumerableCalc.create(input, program).implement(implementor, pref);
  }
}

// End EnumerableRoot.java
