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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Project} not
 * targeted at any particular engine or calling convention.
 */
public final class LogicalProject extends Project {
  private final Set<CorrelationId> variablesSet;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalProject.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet Traits of this relational expression
   * @param input    Input relational expression
   * @param projects List of expressions for the input columns
   * @param rowType  Output row type
   */
  public LogicalProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType,
      Set<CorrelationId> variablesSet) {
    super(cluster, traitSet, input, projects, rowType);
    this.variablesSet = ImmutableSet.copyOf(variablesSet);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  @Deprecated // to be removed before 2.0
  public LogicalProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    this(cluster, traitSet, input, projects, rowType,
        ImmutableSet.<CorrelationId>of());
  }

  @Deprecated // to be removed before 2.0
  public LogicalProject(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType,
      int flags) {
    this(cluster, traitSet, input, projects, rowType,
        ImmutableSet.<CorrelationId>of());
    Util.discard(flags);
  }

  @Deprecated // to be removed before 2.0
  public LogicalProject(RelOptCluster cluster, RelNode input,
      List<RexNode> projects, List<String> fieldNames, int flags) {
    this(cluster, cluster.traitSetOf(RelCollations.EMPTY), input, projects,
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames), ImmutableSet.<CorrelationId>of());
    Util.discard(flags);
  }

  /**
   * Creates a LogicalProject by parsing serialized output.
   */
  public LogicalProject(RelInput input) {
    super(input);
    this.variablesSet = ImmutableSet.of();
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, List<String> fieldNames) {
    return create(input, projects, fieldNames,
        ImmutableSet.<CorrelationId>of());
  }

  /** Creates a LogicalProject. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, List<String> fieldNames,
      Set<CorrelationId> variablesSet) {
    final RelOptCluster cluster = input.getCluster();
    final List<String> fieldNames2 =
        fieldNames == null
            ? null
            : SqlValidatorUtil.uniquify(fieldNames,
                SqlValidatorUtil.F_SUGGESTER);
    final RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames2);
    return create(input, projects, rowType, variablesSet);
  }

  @Deprecated // to be removed before 2.0
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    return create(input, projects, rowType, ImmutableSet.<CorrelationId>of());
  }

  /** Creates a LogicalProject, specifying row type rather than field names. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType,
      Set<CorrelationId> variablesSet) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(Convention.NONE)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                new Supplier<List<RelCollation>>() {
                  public List<RelCollation> get() {
                    return RelMdCollation.project(mq, input, projects);
                  }
                });
    return new LogicalProject(cluster, traitSet, input, projects, rowType,
        variablesSet);
  }

  @Override public LogicalProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new LogicalProject(getCluster(), traitSet, input, projects, rowType,
        variablesSet);
  }

  @Override public Set<CorrelationId> getVariablesSet() {
    return variablesSet;
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalProject.java
