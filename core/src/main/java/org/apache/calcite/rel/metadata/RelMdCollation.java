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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;

/**
 * RelMdCollation supplies a default implementation of
 * {@link org.apache.calcite.rel.metadata.RelMetadataQuery#collations}
 * for the standard logical algebra.
 */
public class RelMdCollation {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.COLLATIONS.method, new RelMdCollation());

  //~ Constructors -----------------------------------------------------------

  private RelMdCollation() {}

  //~ Methods ----------------------------------------------------------------

  public ImmutableList<RelCollation> collations(RelNode rel) {
    return ImmutableList.of();
  }

  public ImmutableList<RelCollation> collations(Filter rel) {
    return RelMetadataQuery.collations(rel.getInput());
  }

  public ImmutableList<RelCollation> collations(TableScan scan) {
    return ImmutableList.copyOf(table(scan.getTable()));
  }

  public ImmutableList<RelCollation> collations(EnumerableMergeJoin join) {
    // In general a join is not sorted. But a merge join preserves the sort
    // order of the left and right sides.
    return ImmutableList.copyOf(
        RelMdCollation.mergeJoin(join.getLeft(), join.getRight(),
            join.getLeftKeys(), join.getRightKeys()));
  }

  public ImmutableList<RelCollation> collations(Sort sort) {
    return ImmutableList.copyOf(
        RelMdCollation.sort(sort.getCollation()));
  }

  public ImmutableList<RelCollation> collations(Project project) {
    return ImmutableList.copyOf(
        RelMdCollation.project(project.getInput(), project.getProjects()));
  }

  public ImmutableList<RelCollation> collations(HepRelVertex rel) {
    return RelMetadataQuery.collations(rel.getCurrentRel());
  }

  // Helper methods

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.TableScan}'s collation. */
  public static List<RelCollation> table(RelOptTable table) {
    return table.getCollationList();
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Sort}'s collation. */
  public static List<RelCollation> sort(RelCollation collation) {
    return ImmutableList.of(collation);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Filter}'s collation. */
  public static List<RelCollation> filter(RelNode input) {
    return RelMetadataQuery.collations(input);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Calc}'s collation. */
  public static List<RelCollation> calc(RelNode input,
      RexProgram program) {
    return program.getCollations(RelMetadataQuery.collations(input));
  }

  /** Helper method to determine a {@link Project}'s collation. */
  public static List<RelCollation> project(RelNode input,
      List<? extends RexNode> projects) {
    // TODO: also monotonic expressions
    final ImmutableList.Builder<RelCollation> collations =
        ImmutableList.builder();
    final List<RelCollation> inputCollations =
        input.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
    if (inputCollations == null || inputCollations.isEmpty()) {
      return ImmutableList.of();
    }
    final Multimap<Integer, Integer> targets = LinkedListMultimap.create();
    for (Ord<RexNode> project : Ord.zip(projects)) {
      if (project.e instanceof RexInputRef) {
        targets.put(((RexInputRef) project.e).getIndex(), project.i);
      }
    }
    final List<RelFieldCollation> fieldCollations = Lists.newArrayList();
  loop:
    for (RelCollation ic : inputCollations) {
      fieldCollations.clear();
      for (RelFieldCollation ifc : ic.getFieldCollations()) {
        final Collection<Integer> integers = targets.get(ifc.getFieldIndex());
        if (integers.isEmpty()) {
          continue loop; // cannot do this collation
        }
        fieldCollations.add(ifc.copy(integers.iterator().next()));
      }
      collations.add(RelCollations.of(fieldCollations));
    }
    return collations.build();
  }

  /** Helper method to determine a {@link Join}'s collation assuming that it
   * uses a merge-join algorithm.
   *
   * <p>If the inputs are sorted on other keys <em>in addition to</em> the join
   * key, the result preserves those collations too. */
  public static List<RelCollation> mergeJoin(RelNode left, RelNode right,
      ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
    final ImmutableList.Builder<RelCollation> builder = ImmutableList.builder();

    final ImmutableList<RelCollation> leftCollations =
        RelMetadataQuery.collations(left);
    assert RelCollations.contains(leftCollations, leftKeys)
        : "cannot merge join: left input is not sorted on left keys";
    builder.addAll(leftCollations);

    final ImmutableList<RelCollation> rightCollations =
        RelMetadataQuery.collations(right);
    assert RelCollations.contains(rightCollations, rightKeys)
        : "cannot merge join: right input is not sorted on right keys";
    final int leftFieldCount = left.getRowType().getFieldCount();
    for (RelCollation collation : rightCollations) {
      builder.add(RelCollations.shift(collation, leftFieldCount));
    }
    return builder.build();
  }
}

// End RelMdCollation.java
