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
package org.apache.calcite.test;

import org.apache.calcite.test.CalciteAssert.AssertThat;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.notNullValue;

/**
 * Tests for the {@code org.apache.calcite.adapter.jdbc} package,
 * assuming the the TPC-DS data set is loaded.
 *
 * <p>This test was added with {@link CalciteAssert.DatabaseInstance#JETHRO} in
 * mind. Its sample Docker VM has TPC-DS data set but not Scott or Foodmart.
 */
public class JdbcAdapterTpcdsTest {

  /** Ensure that this test is only run for databases that have a popated TPC-DS
   * data set. */
  @BeforeClass public static void beforeClass() {
    Assume.assumeThat(CalciteAssert.DB.tpcDs, notNullValue());
  }

  private AssertThat model() {
    return CalciteAssert.model(JdbcTest.tpcdsModel());
  }

  @Test public void testCount() {
    final String sql1 = "select count(*) as c from TPCDS.\"call_center\"";
    model().query(sql1)
        .returnsUnordered("C=6");
  }

  @Test public void testHavingCountDistinct() {
    final String sql2 = ""
        + "select \"cc_manager\", count(*) as c from TPCDS.\"call_center\"\n"
        + "group by \"cc_manager\""
        + "having count(distinct \"cc_call_center_sk\") > 2";
    model().query(sql2)
        .returnsUnordered("cc_manager=, C=3");
  }
}

// End JdbcAdapterTpcdsTest.java
