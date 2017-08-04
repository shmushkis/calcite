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
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
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
  private static final Map<String, Double> CARDINALITY_MAP =
      ImmutableMap.<String, Double>builder()
          .put("[foodmart, agg_c_14_sales_fact_1997]", 86_805d)
          .put("[foodmart, customer]", 10_281d)
          .put("[foodmart, employee]", 1_155d)
          .put("[foodmart, employee_closure]", 7_179d)
          .put("[foodmart, department]", 10_281d)
          .put("[foodmart, inventory_fact_1997]", 4_070d)
          .put("[foodmart, position]", 18d)
          .put("[foodmart, product]", 1560d)
          .put("[foodmart, product_class]", 110d)
          .put("[foodmart, promotion]", 1_864d)
          // region really has 110 rows; made it smaller than store to trick FK
          .put("[foodmart, region]", 24d)
          .put("[foodmart, salary]", 21_252d)
          .put("[foodmart, sales_fact_1997]", 86_837d)
          .put("[foodmart, store]", 25d)
          .put("[foodmart, store_ragged]", 25d)
          .put("[foodmart, time_by_day]", 730d)
          .put("[foodmart, warehouse]", 24d)
          .put("[foodmart, warehouse_class]", 6d)
          .put("[scott, EMP]", 10d)
          .put("[scott, DEPT]", 4d)
          .put("[tpcds, CALL_CENTER]", 8d)
          .put("[tpcds, CATALOG_PAGE]", 11_718d)
          .put("[tpcds, CATALOG_RETURNS]", 144_067d)
          .put("[tpcds, CATALOG_SALES]", 1_441_548d)
          .put("[tpcds, CUSTOMER]", 100_000d)
          .put("[tpcds, CUSTOMER_ADDRESS]", 50_000d)
          .put("[tpcds, CUSTOMER_DEMOGRAPHICS]", 1_920_800d)
          .put("[tpcds, DATE_DIM]", 73049d)
          .put("[tpcds, DBGEN_VERSION]", 1d)
          .put("[tpcds, HOUSEHOLD_DEMOGRAPHICS]", 7200d)
          .put("[tpcds, INCOME_BAND]", 20d)
          .put("[tpcds, INVENTORY]", 11_745_000d)
          .put("[tpcds, ITEM]", 18_000d)
          .put("[tpcds, PROMOTION]", 300d)
          .put("[tpcds, REASON]", 35d)
          .put("[tpcds, SHIP_MODE]", 20d)
          .put("[tpcds, STORE]", 12d)
          .put("[tpcds, STORE_RETURNS]", 287_514d)
          .put("[tpcds, STORE_SALES]", 2_880_404d)
          .put("[tpcds, TIME_DIM]", 86_400d)
          .put("[tpcds, WAREHOUSE]", 5d)
          .put("[tpcds, WEB_PAGE]", 60d)
          .put("[tpcds, WEB_RETURNS]", 71_763d)
          .put("[tpcds, WEB_SALES]", 719_384d)
          .put("[tpcds, WEB_SITE]", 1d)
          .build();

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
    return CARDINALITY_MAP.get(table.t.getQualifiedName().toString());
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
