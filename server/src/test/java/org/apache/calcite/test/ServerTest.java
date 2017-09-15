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

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Unit tests for server and DDL.
 */
public class ServerTest {

  private static final String URL = "jdbc:calcite:"
      + "ParserFactory=org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY";

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
}

// End ServerTest.java
