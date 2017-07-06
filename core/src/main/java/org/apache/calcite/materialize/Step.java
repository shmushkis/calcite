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
package org.apache.calcite.materialize;

import org.apache.calcite.util.graph.AttributedDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** Edge in the join graph.
 *
 * <p>It is directed: the "parent" must be the "many" side containing the
 * foreign key, and the "target" is the "one" side containing the primary
 * key. For example, EMP &rarr; DEPT.
 *
 * <p>When created via
 * {@link LatticeSpace#addEdge(LatticeTable, LatticeTable, List)}
 * it is unique within the {@link LatticeSpace}. */
class Step extends DefaultEdge {
  final List<IntPair> keys;

  Step(LatticeTable source, LatticeTable target, List<IntPair> keys) {
    super(source, target);
    this.keys = ImmutableList.copyOf(keys);
    assert IntPair.ORDERING.isStrictlyOrdered(keys); // ordered and unique
  }

  @Override public int hashCode() {
    return Objects.hash(source, target, keys);
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof Step
        && ((Step) obj).source.equals(source)
        && ((Step) obj).target.equals(target)
        && ((Step) obj).keys.equals(keys);
  }

  @Override public String toString() {
    final StringBuilder b = new StringBuilder()
        .append("Step(")
        .append(source)
        .append(", ")
        .append(target)
        .append(",");
    for (IntPair key : keys) {
      b.append(' ')
          .append(source().field(key.source).getName())
          .append(':')
          .append(target().field(key.target).getName());
    }
    return b.append(")")
        .toString();
  }

  LatticeTable source() {
    return (LatticeTable) source;
  }

  LatticeTable target() {
    return (LatticeTable) target;
  }

  boolean isBackwards() {
    return source == target
        ? keys.get(0).source < keys.get(0).target // want PK on the right
        : cardinality(source()) < cardinality(target());
  }

  /** Temporary method. We should use (inferred) primary keys to figure out
   * the direction of steps. */
  private double cardinality(LatticeTable table) {
    switch (table.t.getQualifiedName().toString()) {
    case "[foodmart, agg_c_14_sales_fact_1997]":
      return 86_805;
    case "[foodmart, customer]":
      return 10_281;
    case "[foodmart, employee]":
      return 1_155;
    case "[foodmart, employee_closure]":
      return 7_179;
    case "[foodmart, department]":
      return 10_281;
    case "[foodmart, inventory_fact_1997]":
      return 4_070;
    case "[foodmart, position]":
      return 18;
    case "[foodmart, product]":
      return 1560;
    case "[foodmart, product_class]":
      return 110;
    case "[foodmart, promotion]":
      return 1_864;
    case "[foodmart, region]":
      return 24; // should be 110; made it smaller than store to trick FK
    case "[foodmart, salary]":
      return 21_252;
    case "[foodmart, sales_fact_1997]":
      return 86_837;
    case "[foodmart, store]":
      return 25;
    case "[foodmart, store_ragged]":
      return 25;
    case "[foodmart, time_by_day]":
      return 730;
    case "[foodmart, warehouse]":
      return 24;
    case "[foodmart, warehouse_class]":
      return 6;
    case "[scott, EMP]":
      return 10;
    case "[scott, DEPT]":
      return 4;
    default:
      throw new AssertionError(table.t.getQualifiedName());
    }
  }

  /** Creates {@link Step} instances. */
  static class Factory implements AttributedDirectedGraph.AttributedEdgeFactory<
      LatticeTable, Step> {
    public Step createEdge(LatticeTable source, LatticeTable target) {
      throw new UnsupportedOperationException();
    }

    public Step createEdge(LatticeTable source, LatticeTable target,
        Object... attributes) {
      @SuppressWarnings("unchecked") final List<IntPair> keys =
          (List) attributes[0];
      return new Step(source, target, keys);
    }
  }
}

// End Step.java
