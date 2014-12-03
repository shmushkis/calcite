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
package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.test.CalciteAssert;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for Calcite's remote JDBC driver.
 */
public class CalciteRemoteDriverTest {
  public static final String LJS =
      LocalJsonService.Factory.class.getName();
  private Connection connect;

  @Before public void setUp() throws Exception {
    connect = CalciteAssert.that().connect();
    LocalJsonService.THREAD_SERVICE.set(
        new LocalJsonService(
            new LocalService(
                CalciteConnectionImpl.TROJAN
                    .getMeta((CalciteConnectionImpl) connect))));
  }

  @After public void tearDown() throws Exception {
    if (connect != null) {
      connect.close();
      connect = null;
    }
    LocalJsonService.THREAD_SERVICE.remove();
  }

  @Test public void testCatalogsLocal() throws Exception {
    final Connection connection = DriverManager.getConnection(
        "jdbc:avatica:remote:factory=" + LJS);
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getCatalogs();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(1, metaData.getColumnCount());
    assertEquals("TABLE_CATALOG", metaData.getColumnName(1));
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testSchemasLocal() throws Exception {
    final Connection connection = DriverManager.getConnection(
        "jdbc:avatica:remote:factory=" + LJS);
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    assertTrue(resultSet.next());
    assertThat(resultSet.getString(1), equalTo("POST"));
    assertThat(resultSet.getString(2), CoreMatchers.nullValue());
    assertTrue(resultSet.next());
    assertThat(resultSet.getString(1), equalTo("foodmart"));
    assertThat(resultSet.getString(2), CoreMatchers.nullValue());
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testRemote() throws Exception {
    final HttpServer start = Main.start(new String[]{Factory.class.getName()});
    try {
      int port = start.getPort();
      final Connection connection =
          DriverManager.getConnection(
              "jdbc:avatica:remote:url=http://localhost:" + port);
      final ResultSet resultSet = connection.getMetaData().getSchemas();
      while (resultSet.next()) {
        System.out.println(resultSet.getString(1));
      }
    } finally {
      start.stop();
    }
  }

  /** Creates a {@link Meta} that can see the test databases. */
  public static class Factory implements Meta.Factory {
    public Meta create(List<String> args) {
      try {
        final Connection connection = CalciteAssert.that().connect();
        return new CalciteMetaImpl((CalciteConnectionImpl) connection);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

// End CalciteRemoteDriverTest.java
