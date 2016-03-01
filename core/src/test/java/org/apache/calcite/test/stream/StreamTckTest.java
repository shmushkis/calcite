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
package org.apache.calcite.test.stream;

import org.apache.calcite.avatica.util.DateTimeUtils;

import org.junit.Test;

/**
 * Tests that exercise a streaming SQL engine and test whether it is
 * compatible with standard behavior.
 */
public abstract class StreamTckTest {
  final Object[][] orderRows = {
    {ts(10, 15, 0), 1, "paint", 10},
    {ts(10, 24, 15), 2, "paper", 5},
    {ts(10, 24, 45), 3, "brush", 12},
    {ts(10, 58, 0), 4, "paint", 3},
    {ts(11, 10, 0), 5, "paint", 3}
  };

  protected abstract StreamAssert getTester();

  private static Object ts(int h, int m, int s) {
    return DateTimeUtils.unixTimestamp(2015, 2, 15, h, m, s);
  }

  @Test public void testScan() {
    final String sql = "select stream * from Orders";
    getTester()
        .sql(sql)
        .input(0, "Orders")
        .send(0, ts(10, 15, 0), 1, "paint", 10)
        .expect(ts(10, 15, 0), 1, "paint", 10)
        .send(0, ts(10, 24, 15), 2, "paper", 5)
        .expect(ts(10, 24, 15), 2, "paper", 5)
        .end();
  }

  @Test public void testWhere() {
    final String sql = "select stream * from Orders where units < 6";
    getTester()
        .sql(sql)
        .input(0, "Orders")
        .send(0, ts(10, 15, 0), 1, "paint", 10)
        .expect(ts(10, 15, 0), 1, "paint", 10)
        .send(0, ts(10, 24, 15), 2, "paper", 5)
        .send(0, ts(10, 24, 45), 3, "brush", 12)
        .expect(ts(10, 24, 45), 3, "brush", 12)
        .end();
  }
}

// End StreamTckTest.java
