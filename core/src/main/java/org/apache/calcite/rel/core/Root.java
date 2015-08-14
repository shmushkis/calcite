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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Relational expression that describes how the result of a query should be
 * presented.
 *
 * <p>It only appears at the root of the tree of relational expressions, hence
 * the name.
 *
 * <p>Aspects of presentation:
 * <ul>
 *   <li>Field names (duplicates allowed)</li>
 *   <li>Sort order (may reference fields that are not projected)</li>
 * </ul>
 *
 * <p>TODO:
 * <ul>
 *   <li>Remove {@link org.apache.calcite.prepare.Prepare#collations}
 *   <li>Look at uses of
 *   {@link org.apache.calcite.sql2rel.SqlToRelConverter#isUnordered}
 *   <li>Remove {@link org.apache.calcite.rel.RelCollations#PRESERVE}
 *   <li>Add a test like InterpreterTest.testInterpretProjectFilterValues
 *   where an interpretable needs a root
 * </ul>
 */
public abstract class Root extends SingleRel {
  public final ImmutableList<Pair<Integer, String>> fields;
  public final RelCollation collation;

  /**
   * Creates a Root.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traitSet  Traits of this relational expression
   * @param input     Input relational expression
   * @param fields    Field references and names
   * @param collation Fields of underlying relational expression sorted on
   */
  protected Root(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      List<Pair<Integer, String>> fields, RelCollation collation) {
    super(cluster, traitSet, input);
    this.fields = ImmutableList.copyOf(fields);
    this.collation = Preconditions.checkNotNull(collation);
  }

  /**
   * Creates a Root by parsing serialized output.
   */
  protected Root(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        Pair.zip(input.getIntegerList("fields"), input.getStringList("names")),
        RelCollationTraitDef.INSTANCE.canonize(input.getCollation()));
  }

  //~ Methods ----------------------------------------------------------------

  public static RelNode strip(RelNode rel) {
    return rel instanceof Root ? rel.getInput(0) : rel;
  }

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  /**
   * Copies a Root.
   *
   * @param traitSet Traits
   * @param input Input
   *
   * @see #copy(RelTraitSet, List)
   */
  public abstract Root copy(RelTraitSet traitSet, RelNode input);

  @Override protected RelDataType deriveRowType() {
    final RelDataTypeFactory.FieldInfoBuilder builder =
        getCluster().getTypeFactory().builder();
    final List<RelDataTypeField> inputFields =
        getInput().getRowType().getFieldList();
    for (Pair<Integer, String> field : fields) {
      builder.add(field.right, inputFields.get(field.left).getType());
    }
    return builder.build();
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return planner.getCostFactory().makeTinyCost();
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    final List<String> exps = Lists.transform(fields,
        new Function<Pair<Integer, String>, String>() {
          public String apply(Pair<Integer, String> input) {
            return "$" + input.left;
          }
        });
    if (!isNameTrivial() || !isRefTrivial()) {
      if (pw.nest()) {
        pw.itemIf("fields", Pair.right(fields), !isNameTrivial());
        pw.itemIf("exprs", exps, !isRefTrivial());
      } else {
        for (Ord<String> field : Ord.zip(Pair.right(fields))) {
          pw.item(field.e, exps.get(field.i));
        }
      }
    }
    return pw.itemIf("collation", collation, !isCollationTrivial());
  }

  protected boolean isNameTrivial() {
    final RelDataType inputRowType = input.getRowType();
    return Pair.right(fields).equals(inputRowType.getFieldNames());
  }

  protected boolean isRefTrivial() {
    final RelDataType inputRowType = input.getRowType();
    final List<Integer> list =
        ImmutableIntList.identity(inputRowType.getFieldCount());
    return list.equals(Pair.left(fields));
  }

  protected boolean isCollationTrivial() {
    final List<RelCollation> collations = input.getTraitSet()
        .getTraits(RelCollationTraitDef.INSTANCE);
    return collations != null
        && collations.size() == 1
        && collations.get(0).equals(collation);
  }

  public boolean isTrivial() {
    return isNameTrivial() && isRefTrivial() && isCollationTrivial();
  }
}

// End Root.java
