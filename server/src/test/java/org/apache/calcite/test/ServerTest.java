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
      boolean b = s.execute("create table foo (i int not null)");
      assertThat(b, is(true));
      int x = s.executeUpdate("insert into foo values 1");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into foo values 3");
      assertThat(x, is(1));
      try (ResultSet r = s.executeQuery("select sum(i) from foo")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(4));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test public void testCreateTable2() throws Exception {
    try (Connection c = DriverManager.getConnection(URL);
         Statement s = c.createStatement()) {
      boolean b = s.execute("create table foo (\n"
          + " i int not null,\n"
          + " j int as (i + 1) stored)");
      assertThat(b, is(true));

      int x;
      // No target column list; too few values provided
      try {
        x = s.executeUpdate("insert into foo values (3)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Number of INSERT target columns (2) does not equal "
                + "number of source items (1)"));
      }

      // No target column list; too many values provided
      try {
        x = s.executeUpdate("insert into foo values (3, 4, 5)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Number of INSERT target columns (2) does not equal "
                + "number of source items (3)"));
      }

      // No target column list;
      // source count = target count;
      // but one of the target columns is virtual.
      try {
        x = s.executeUpdate("insert into foo values (3, 4)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT into generated column 'J'"));
      }

      // Explicit target column list, omits virtual column
      x = s.executeUpdate("insert into foo (i) values (1)");
      assertThat(x, is(1));

      // Explicit target column list, includes virtual column but assigns
      // DEFAULT.
      x = s.executeUpdate("insert into foo (i, j) values (2, DEFAULT)");
      assertThat(x, is(1));

      // As previous, re-order columns.
      x = s.executeUpdate("insert into foo (j, i) values (DEFAULT, 3)");
      assertThat(x, is(1));

      // Target column list exists,
      // target column count equals the number of non-virtual columns;
      // but one of the target columns is virtual.
      try {
        x = s.executeUpdate("insert into foo (j) values (3)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT virtual column 'J'"));
      }

      // Target column list exists and contains all columns,
      // expression for virtual column is not DEFAULT.
      try {
        x = s.executeUpdate("insert into foo (i, j) values (3, 3 + 1)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT virtual column 'J'"));
      }
      x = s.executeUpdate("insert into foo (i) values (1)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into foo (i, j) values (1, DEFAULT)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into foo (j, i) values (DEFAULT, 1)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into foo values (5), (4)");
      assertThat(x, is(2));
      try (ResultSet r = s.executeQuery("select sum(i) from foo")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(13));
        assertThat(r.next(), is(false));
      }
    }
  }
}

// End ServerTest.java
