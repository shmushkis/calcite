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

import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import net.hydromatic.tpcds.query.Query;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link LatticeSuggester}.
 */
public class TpcdsLatticeSuggesterTest {

  private String number(String s) {
    final StringBuilder b = new StringBuilder();
    int i = 0;
    for (String line : s.split("\n")) {
      b.append(++i).append(' ').append(line).append("\n");
    }
    return b.toString();
  }

  private void checkFoodMartAll(boolean evolve) throws Exception {
    final Tester t = new Tester().tpcds().withEvolve(evolve);
    for (Query query : Query.values()) {
      final String sql = query.sql(new Random(0))
          .replaceAll("as returns", "as \"returns\"")
          .replaceAll("sum\\(returns\\)", "sum(\"returns\")")
          .replaceAll(", returns", ", \"returns\"")
          .replaceAll("14 days", "interval '14' day")
          .replaceAll("substr\\(([^,]*),([^,]*),([^)]*)\\)",
              "substring($1 from $2 for $3)");
      System.out.println("Query #" + query.id + "\n" + number(sql)); // TODO:
      switch (query.id) {
      case 6:
        continue; // NPE
      }
      t.addQuery(sql);
    }

    // The graph of all tables and hops
    final String expected = "graph("
        + "vertices: ["
        + "[foodmart, agg_c_10_sales_fact_1997], "
        + "[foodmart, agg_c_14_sales_fact_1997], "
        + "[foodmart, agg_c_special_sales_fact_1997], "
        + "[foodmart, agg_g_ms_pcat_sales_fact_1997], "
        + "[foodmart, agg_l_03_sales_fact_1997], "
        + "[foodmart, agg_l_04_sales_fact_1997], "
        + "[foodmart, agg_l_05_sales_fact_1997], "
        + "[foodmart, agg_lc_06_sales_fact_1997], "
        + "[foodmart, agg_lc_100_sales_fact_1997], "
        + "[foodmart, agg_ll_01_sales_fact_1997], "
        + "[foodmart, agg_pl_01_sales_fact_1997], "
        + "[foodmart, customer], "
        + "[foodmart, department], "
        + "[foodmart, employee], "
        + "[foodmart, employee_closure], "
        + "[foodmart, inventory_fact_1997], "
        + "[foodmart, position], "
        + "[foodmart, product], "
        + "[foodmart, product_class], "
        + "[foodmart, promotion], "
        + "[foodmart, region], "
        + "[foodmart, salary], "
        + "[foodmart, sales_fact_1997], "
        + "[foodmart, store], "
        + "[foodmart, store_ragged], "
        + "[foodmart, time_by_day], "
        + "[foodmart, warehouse], "
        + "[foodmart, warehouse_class]], "
        + "edges: ["
        + "Step([foodmart, agg_c_14_sales_fact_1997], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, agg_c_14_sales_fact_1997], [foodmart, time_by_day],"
        + " month_of_year:month_of_year), "
        + "Step([foodmart, customer], [foodmart, region], customer_region_id:region_id), "
        + "Step([foodmart, customer], [foodmart, store], state_province:store_state), "
        + "Step([foodmart, employee], [foodmart, employee], supervisor_id:employee_id), "
        + "Step([foodmart, employee], [foodmart, position], position_id:position_id), "
        + "Step([foodmart, employee], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, employee], product_id:employee_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, employee], time_id:employee_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, product], product_id:product_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, store], warehouse_id:store_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, time_by_day], time_id:time_id), "
        + "Step([foodmart, inventory_fact_1997], [foodmart, warehouse],"
        + " warehouse_id:warehouse_id), "
        + "Step([foodmart, product], [foodmart, product_class],"
        + " product_class_id:product_class_id), "
        + "Step([foodmart, product], [foodmart, store], product_class_id:store_id), "
        + "Step([foodmart, product_class], [foodmart, store], product_class_id:region_id), "
        + "Step([foodmart, salary], [foodmart, department], department_id:department_id), "
        + "Step([foodmart, salary], [foodmart, employee], employee_id:employee_id), "
        + "Step([foodmart, salary], [foodmart, employee_closure], employee_id:employee_id), "
        + "Step([foodmart, salary], [foodmart, time_by_day], pay_date:the_date), "
        + "Step([foodmart, sales_fact_1997], [foodmart, customer], customer_id:customer_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, customer], product_id:customer_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, customer], store_id:customer_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, product], product_id:product_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, promotion], promotion_id:promotion_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, store], product_id:store_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, store], store_id:store_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, store_ragged], store_id:store_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, time_by_day], product_id:time_id), "
        + "Step([foodmart, sales_fact_1997], [foodmart, time_by_day], time_id:time_id), "
        + "Step([foodmart, store], [foodmart, region], region_id:region_id), "
        + "Step([foodmart, store], [foodmart, warehouse], store_id:stores_id), "
        + "Step([foodmart, warehouse], [foodmart, warehouse_class],"
        + " warehouse_class_id:warehouse_class_id)])";
    assertThat(t.s.space.g.toString(), is(expected));
    if (evolve) {
      // compared to evolve=false, there are a few more nodes (133 vs 117),
      // the same number of paths, and a lot fewer lattices (27 vs 376)
      assertThat(t.s.space.nodeMap.size(), is(133));
      assertThat(t.s.latticeMap.size(), is(27));
      assertThat(t.s.space.pathMap.size(), is(42));
    } else {
      assertThat(t.s.space.nodeMap.size(), is(117));
      assertThat(t.s.latticeMap.size(), is(376));
      assertThat(t.s.space.pathMap.size(), is(42));
    }
  }

  @Test public void testTpcdsAll() throws Exception {
    checkFoodMartAll(false);
  }

  @Test public void testTpcdsAllEvolve() throws Exception {
    checkFoodMartAll(true);
  }

  /** Test helper. */
  private static class Tester {
    final LatticeSuggester s = new LatticeSuggester();
    private final FrameworkConfig config;

    Tester() {
      this(config(CalciteAssert.SchemaSpec.BLANK).build());
    }

    private Tester(FrameworkConfig config) {
      this.config = config;
    }

    Tester tpcds() {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      final double scaleFactor = 0.01d;
      final SchemaPlus schema =
          rootSchema.add("tpcds", new TpcdsSchema(scaleFactor));
      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.Config.DEFAULT)
          .context(
              Contexts.of(
                  new CalciteConnectionConfigImpl(new Properties())
                      .set(CalciteConnectionProperty.CONFORMANCE,
                          SqlConformanceEnum.LENIENT.name())))
          .defaultSchema(schema)
          .build();
      return withConfig(config);
    }

    Tester withConfig(FrameworkConfig config) {
      return new Tester(config);
    }

    List<Lattice> addQuery(String q) throws SqlParseException,
        ValidationException, RelConversionException {
      final Planner planner = new PlannerImpl(config);
      final SqlNode node = planner.parse(q);
      final SqlNode node2 = planner.validate(node);
      final RelRoot root = planner.rel(node2);
      return s.addQuery(root.project());
    }

    /** Parses a query returns its graph. */
    LatticeRootNode node(String q) throws SqlParseException,
        ValidationException, RelConversionException {
      final List<Lattice> list = addQuery(q);
      assertThat(list.size(), is(1));
      return list.get(0).rootNode;
    }

    static Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec spec) {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, spec);
      return Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.Config.DEFAULT)
          .defaultSchema(schema);
    }

    Tester withEvolve(boolean evolve) {
      s.setEvolve(evolve);
      return this;
    }
  }
}

// End TpcdsLatticeSuggesterTest.java
