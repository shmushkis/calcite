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

package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.plan.volcano.VolcanoPlannerTest.TestLeafRel;
import org.apache.calcite.plan.volcano.VolcanoPlannerTest.TestSingleRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.junit.Test;

import java.util.List;

import static org.apache.calcite.plan.volcano.VolcanoPlannerTest.PHYS_CALLING_CONVENTION;
import static org.apache.calcite.plan.volcano.VolcanoPlannerTest.newCluster;

/** Test for converting rel distribution */
public class TraitConversionTest {

  final ConvertRelDistributionTraitDef newTraitDefInstance = new ConvertRelDistributionTraitDef();
  public SimpleDistribution simpleDistributionAny = new SimpleDistribution("ANY");
  public SimpleDistribution simpleDistributionRandom = new SimpleDistribution("RANDOM");
  public SimpleDistribution simpleDistributionSingleton = new SimpleDistribution("SINGLETON");

  @Test
  public void testTraitConversion() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(newTraitDefInstance);

    planner.addRule(new RandomSingleTraitRule());
    planner.addRule(new SingleLeafTraitRule());
    planner.addRule(ExpandConversionRule.INSTANCE);

    RelOptCluster cluster = newCluster(planner);
    NoneLeafRel leafRel =
            new NoneLeafRel(cluster, "a");
    NoneSingleRel singleRel =
            new NoneSingleRel(cluster, leafRel);
    RelNode convertedRel =
            planner.changeTraits(singleRel,
                    cluster.traitSetOf(PHYS_CALLING_CONVENTION));
    planner.setRoot(convertedRel);
    RelNode result = planner.chooseDelegate().findBestExp();
  }

  /** Converts a NoneSingleRel -> RandomSingleRel */
  class RandomSingleTraitRule extends RelOptRule {
    RandomSingleTraitRule() {
      super(operand(NoneSingleRel.class, any()));
    }

    // implement RelOptRule
    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
      NoneSingleRel singleRel = call.rel(0);
      RelNode childRel = singleRel.getInput();
      RelNode physInput =
              convert(childRel,
                      singleRel.getTraitSet()
                              .replace(PHYS_CALLING_CONVENTION)
                              .plus(simpleDistributionRandom));
      call.transformTo(
              new RandomSingleRel(
                      singleRel.getCluster(),
                      physInput));
    }
  }

  /** RandomSingleRel */
  class RandomSingleRel extends TestSingleRel {
    RandomSingleRel(
            RelOptCluster cluster,
            RelNode child) {
      super(
         cluster,
         cluster.traitSetOf(PHYS_CALLING_CONVENTION).plus(simpleDistributionRandom),
         child);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(
            RelOptPlanner planner,
            RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet,
                        List<RelNode> inputs) {
      return new RandomSingleRel(getCluster(), sole(inputs));
    }
  }

  /** Converts NoneLeafRel -> SingletonLeafRel  */
  class SingleLeafTraitRule extends RelOptRule {
    SingleLeafTraitRule() {
      super(operand(NoneLeafRel.class, any()));
    }

    // implement RelOptRule
    public Convention getOutConvention() {
      return PHYS_CALLING_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
      NoneLeafRel leafRel = call.rel(0);
      call.transformTo(
              new SingletonLeafRel(leafRel.getCluster(), leafRel.getLabel()));
    }
  }

  /** SingletonLeafRel */
  class SingletonLeafRel extends TestLeafRel {
    SingletonLeafRel(
            RelOptCluster cluster,
            String label) {
      super(cluster, cluster.traitSetOf(PHYS_CALLING_CONVENTION)
              .plus(simpleDistributionSingleton), label);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new SingletonLeafRel(getCluster(), getLabel());
    }
  }

  /** Bridges the SimpleDistribution difference between SingletonLeafRel and RandomSingleRel */
  class BridgeRel extends TestSingleRel {
    BridgeRel(
            RelOptCluster cluster,
            RelNode child) {
      super(cluster, cluster.traitSetOf(PHYS_CALLING_CONVENTION)
              .plus(simpleDistributionRandom), child);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
      return planner.getCostFactory().makeTinyCost();
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new BridgeRel(getCluster(), sole(inputs));
    }
  }

  /** Distribution (simplified version of RelDistribution) */
  public class SimpleDistribution implements RelTrait {

    private final String name;

    public SimpleDistribution(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override public String toString() {
      return getName();
    }

    @Override public RelTraitDef getTraitDef() {
      return newTraitDefInstance;
    }

    @Override public boolean satisfies(RelTrait trait) {
      if (trait == this || trait == simpleDistributionAny) {
        return true;
      }

      return false;
    }

    @Override public void register(RelOptPlanner planner) {}
  }

  /** Handles conversion of SimpleDistribution */
  public class ConvertRelDistributionTraitDef extends RelTraitDef<SimpleDistribution> {

    private ConvertRelDistributionTraitDef() {}

    @Override public Class<SimpleDistribution> getTraitClass() {
      return SimpleDistribution.class;
    }

    @Override public String toString() {
      return getSimpleName();
    }

    @Override public String getSimpleName() {
      return "ConvertRelDistributionTraitDef";
    }

    @Override public RelNode convert(RelOptPlanner planner,
                                     RelNode rel,
                                     SimpleDistribution toTrait,
                                     boolean allowInfiniteCostConverters) {
      if (toTrait == simpleDistributionAny) {
        return rel;
      }

      return new BridgeRel(rel.getCluster(), rel);
    }

    @Override public boolean canConvert(RelOptPlanner planner,
                                        SimpleDistribution fromTrait,
                                        SimpleDistribution toTrait) {

      if ((fromTrait == toTrait)
              || (toTrait == simpleDistributionAny)
              || (fromTrait == simpleDistributionSingleton
                && toTrait == simpleDistributionRandom)) {
        return true;
      }

      return false;
    }

    @Override public SimpleDistribution getDefault() {
      return simpleDistributionAny;
    }
  }


  /** NoneLeafRel (has simple distribution of ANY) */
  class NoneLeafRel extends TestLeafRel {
    protected NoneLeafRel(
            RelOptCluster cluster,
            String label) {
      super(
              cluster,
              cluster.traitSetOf(Convention.NONE),
              label);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, simpleDistributionAny);
      assert inputs.isEmpty();
      return this;
    }
  }

  /** NoneSingleRel (has simple distribution of ANY) */
  class NoneSingleRel extends TestSingleRel {
    protected NoneSingleRel(
            RelOptCluster cluster,
            RelNode child) {
      super(
              cluster,
              cluster.traitSetOf(Convention.NONE),
              child);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert traitSet.comprises(Convention.NONE, simpleDistributionAny);
      return new NoneSingleRel(
              getCluster(),
              sole(inputs));
    }
  }

}
// End TraitConversionTest.java
