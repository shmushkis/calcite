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

import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

/**
 * Unit tests for server and DDL.
 */
public class ServerTest {

  private static final String URL = "jdbc:calcite:"
      + "ParserFactory=" + SqlDdlParserImpl.class.getName() + "#FACTORY";

  @Test public void testStatement() throws Exception {
    try (Connection c = DriverManager.getConnection(URL);
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("values 1, 2")) {
      assertThat(r.next(), is(true));
      assertThat(r.getString(1), notNullValue());
      assertThat(r.next(), is(true));
      assertThat(r.next(), is(false));
    }
  }

  @Test public void testCreateTable() throws Exception {
    try (Connection c = DriverManager.getConnection(URL);
         Statement s = c.createStatement()) {
      boolean b = s.execute("create table t (i int not null)");
      assertThat(b, is(true));
      int x = s.executeUpdate("insert into t values 1");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t values 3");
      assertThat(x, is(1));
      try (ResultSet r = s.executeQuery("select sum(i) from t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(4));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test public void testStoredGeneratedColumn() throws Exception {
    try (Connection c = DriverManager.getConnection(URL);
         Statement s = c.createStatement()) {
      boolean b = s.execute("create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) stored)");
      assertThat(b, is(true));

      int x;

      // A successful row.
      x = s.executeUpdate("insert into t (h, i) values (3, 4)");
      assertThat(x, is(1));

      try (ResultSet r = s.executeQuery("explain plan for\n"
          + "insert into t (h, i) values (3, 4)")) {
        assertThat(r.next(), is(true));
        final String plan = ""
            + "EnumerableTableModify(table=[[T]], operation=[INSERT], flattened=[false])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], proj#0..1=[{exprs}], J=[$t3])\n"
            + "    EnumerableValues(tuples=[[{ 3, 4 }]])\n";
        assertThat(r.getString(1), is(plan));
        assertThat(r.next(), is(false));
      }

      try (ResultSet r = s.executeQuery("select * from t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt("H"), is(3));
        assertThat(r.wasNull(), is(false));
        assertThat(r.getInt("I"), is(4));
        assertThat(r.getInt("J"), is(5)); // j = i + 1
        assertThat(r.next(), is(false));
      }

      // No target column list; too few values provided
      try {
        x = s.executeUpdate("insert into t values (2, 3)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Number of INSERT target columns (3) does not equal "
                + "number of source items (2)"));
      }

      // No target column list; too many values provided
      try {
        x = s.executeUpdate("insert into t values (3, 4, 5, 6)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Number of INSERT target columns (3) does not equal "
                + "number of source items (4)"));
      }

      // No target column list;
      // source count = target count;
      // but one of the target columns is virtual.
      try {
        x = s.executeUpdate("insert into t values (3, 4, 5)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT into generated column 'J'"));
      }

      // Explicit target column list, omits virtual column
      x = s.executeUpdate("insert into t (h, i) values (1, 2)");
      assertThat(x, is(1));

      // Explicit target column list, includes virtual column but assigns
      // DEFAULT.
      x = s.executeUpdate("insert into t (h, i, j) values (1, 2, DEFAULT)");
      assertThat(x, is(1));

      // As previous, re-order columns.
      x = s.executeUpdate("insert into t (h, j, i) values (1, DEFAULT, 3)");
      assertThat(x, is(1));

      // Target column list exists,
      // target column count equals the number of non-virtual columns;
      // but one of the target columns is virtual.
      try {
        x = s.executeUpdate("insert into t (h, j) values (1, 3)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT into generated column 'J'"));
      }

      // Target column list exists and contains all columns,
      // expression for virtual column is not DEFAULT.
      try {
        x = s.executeUpdate("insert into t (h, i, j) values (2, 3, 3 + 1)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT into generated column 'J'"));
      }
      x = s.executeUpdate("insert into t (h, i) values (0, 1)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t (h, i, j) values (0, 1, DEFAULT)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t (j, i, h) values (DEFAULT, NULL, 7)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t (h, i) values (6, 5), (7, 4)");
      assertThat(x, is(2));
      try (ResultSet r = s.executeQuery("select sum(i), count(*) from t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(19));
        assertThat(r.getInt(2), is(9));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test public void testStoredGeneratedColumn2() throws Exception {
    try (Connection c = DriverManager.getConnection(URL);
         Statement s = c.createStatement()) {
      boolean b = s.execute("create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) stored)");
      assertThat(b, is(true));

      // Planner uses constraint to optimize away condition.
      final String sql = "explain plan for\n"
          + "select * from t where j = i + 1";
      final String plan = "xx";
      try (ResultSet r = s.executeQuery(sql)) {
        assertThat(r.next(), is(true));
        assertThat(r.getString(1), is(plan));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test public void testVirtualColumn() throws Exception {
    try (Connection c = DriverManager.getConnection(URL);
         Statement s = c.createStatement()) {
      boolean b = s.execute("create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) virtual)");
      assertThat(b, is(true));

      int x = s.executeUpdate("insert into t (h, i) values (1, 2)");
      assertThat(x, is(1));

      // In plan, "j" is replaced by "i + 1".
      final String sql = "select * from t";
      try (ResultSet r = s.executeQuery(sql)) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(1));
        assertThat(r.getInt(2), is(2));
        assertThat(r.getInt(3), is(3));
        assertThat(r.next(), is(false));
      }

      final String plan = "xxx";
      try (ResultSet r = s.executeQuery("explain plan for " + sql)) {
        assertThat(r.next(), is(true));
        assertThat(r.getString(1), is(plan));
      }
    }
  }
}

// End ServerTest.java
