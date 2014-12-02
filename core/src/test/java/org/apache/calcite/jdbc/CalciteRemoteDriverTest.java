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

import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.test.CalciteAssert;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Test for Calcite's remote JDBC driver.
 */
public class CalciteRemoteDriverTest {
  public static final String LJS =
      LocalJsonService.Factory.class.getName();

  @Test public void testCatalogsLocal() throws Exception {
    final Connection connect = CalciteAssert.that().connect();
    try {
      LocalJsonService.THREAD_SERVICE.set(
          new LocalJsonService(
              new LocalService(
                  CalciteConnectionImpl.TROJAN
                      .getMeta((CalciteConnectionImpl) connect))));
      final Connection connection = DriverManager.getConnection(
          "jdbc:avatica:remote:factory=" + LJS);
      assertThat(connection.isClosed(), is(false));
      final ResultSet resultSet = connection.getMetaData().getSchemas();
      assertFalse(resultSet.next());
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals(2, metaData.getColumnCount());
      assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
      assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
      resultSet.close();
      connection.close();
      assertThat(connection.isClosed(), is(true));
    } finally {
      connect.close();
      LocalJsonService.THREAD_SERVICE.remove();
    }
  }
}

// End CalciteRemoteDriverTest.java
