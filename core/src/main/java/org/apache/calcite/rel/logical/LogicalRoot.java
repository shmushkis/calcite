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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Root;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Root} not
 * targeted at any particular engine or calling convention.
 */
public class LogicalRoot extends Root {
  /**
   * Creates a LogicalRoot.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traitSet  Traits of this relational expression
   * @param input     Input relational expression
   * @param fields    Field references and names
   * @param collation Fields of underlying relational expression sorted on
   */
  protected LogicalRoot(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<Pair<Integer, String>> fields,
      RelCollation collation) {
    super(cluster, traitSet, input, fields, collation);
  }

  /**
   * Creates a LogicalRoot by parsing serialized output.
   */
  public LogicalRoot(RelInput input) {
    super(input);
  }

  /** Creates a LogicalRoot. */
  public static LogicalRoot create(RelNode input,
      List<Pair<Integer, String>> fields, RelCollation collation) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.getPlanner().emptyTraitSet()
        .plus(Convention.NONE);
    return new LogicalRoot(cluster, traitSet, input, fields, collation);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalRoot copy(RelTraitSet traitSet, RelNode input) {
    return new LogicalRoot(input.getCluster(), traitSet, input, fields,
        collation);
  }
}

// End LogicalRoot.java
