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
package org.apache.calcite.profile;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.Matchers;
import org.apache.calcite.util.JsonBuilder;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Basic implementation of {@link Profiler}.
 */
public class ProfilerTest {
  @Test public void testProfileScott() throws Exception {
    final String sql = "select * from \"scott\".emp\n"
        + "join \"scott\".dept using (deptno)";
    checkProfile(sql,
        "{type:distribution,columns:[COMM,DEPTNO0],cardinality:4.0}",
        "{type:distribution,columns:[COMM,DEPTNO],cardinality:4.0}",
        "{type:distribution,columns:[COMM,DNAME],cardinality:4.0}",
        "{type:distribution,columns:[COMM],values:[0.00,300.00,500.00,1400.00],cardinality:4.0,nullCount:10}",
        "{type:distribution,columns:[DEPTNO,DEPTNO0],cardinality:3.0}",
        "{type:distribution,columns:[DEPTNO,DNAME],cardinality:3.0}",
        "{type:distribution,columns:[DEPTNO0,DNAME],cardinality:3.0}",
        "{type:distribution,columns:[DEPTNO0],values:[10,20,30],cardinality:3.0}",
        "{type:distribution,columns:[DEPTNO],values:[10,20,30],cardinality:3.0}",
        "{type:distribution,columns:[DNAME],values:[ACCOUNTING,RESEARCH,SALES],cardinality:3.0}",
        "{type:distribution,columns:[HIREDATE,COMM],cardinality:4.0}",
        "{type:distribution,columns:[HIREDATE],values:[1980-12-17,1981-01-05,1981-02-04,1981-02-20,1981-02-22,1981-06-09,1981-09-08,1981-09-28,1981-11-17,1981-12-03,1982-01-23,1987-04-19,1987-05-23],cardinality:13.0}",
        "{type:distribution,columns:[JOB,COMM],cardinality:4.0}",
        "{type:distribution,columns:[JOB,DEPTNO0],cardinality:9.0}",
        "{type:distribution,columns:[JOB,DEPTNO],cardinality:9.0}",
        "{type:distribution,columns:[JOB,DNAME],cardinality:9.0}",
        "{type:distribution,columns:[JOB,MGR,DEPTNO0],cardinality:9.0}",
        "{type:distribution,columns:[JOB,MGR,DEPTNO],cardinality:9.0}",
        "{type:distribution,columns:[JOB,MGR,DNAME],cardinality:9.0}",
        "{type:distribution,columns:[JOB,MGR],cardinality:7.0}",
        "{type:distribution,columns:[JOB,SAL],cardinality:12.0}",
        "{type:distribution,columns:[JOB],values:[ANALYST,CLERK,MANAGER,PRESIDENT,SALESMAN],cardinality:5.0}",
        "{type:distribution,columns:[MGR,COMM],cardinality:4.0}",
        "{type:distribution,columns:[MGR,DEPTNO0],cardinality:8.0}",
        "{type:distribution,columns:[MGR,DEPTNO],cardinality:8.0}",
        "{type:distribution,columns:[MGR,DNAME],cardinality:8.0}",
        "{type:distribution,columns:[MGR,HIREDATE],cardinality:13.0}",
        "{type:distribution,columns:[MGR,SAL],cardinality:11.0}",
        "{type:distribution,columns:[MGR],values:[7566,7698,7782,7788,7839,7902],cardinality:6.0,nullCount:1}",
        "{type:distribution,columns:[SAL,COMM],cardinality:4.0}",
        "{type:distribution,columns:[SAL,DEPTNO0],cardinality:12.0}",
        "{type:distribution,columns:[SAL,DEPTNO],cardinality:12.0}",
        "{type:distribution,columns:[SAL,DNAME],cardinality:12.0}",
        "{type:distribution,columns:[SAL],values:[800.00,950.00,1100.00,1250.00,1300.00,1500.00,1600.00,2450.00,2850.00,2975.00,3000.00,5000.00],cardinality:12.0}",
        "{type:distribution,columns:[],cardinality:1.0}",
        "{type:fd,columns:[COMM],dependentColumn:DEPTNO0}",
        "{type:fd,columns:[COMM],dependentColumn:DEPTNO}",
        "{type:fd,columns:[COMM],dependentColumn:DNAME}",
        "{type:fd,columns:[COMM],dependentColumn:HIREDATE}",
        "{type:fd,columns:[COMM],dependentColumn:JOB}",
        "{type:fd,columns:[COMM],dependentColumn:MGR}",
        "{type:fd,columns:[COMM],dependentColumn:SAL}",
        "{type:fd,columns:[DEPTNO0],dependentColumn:DEPTNO}",
        "{type:fd,columns:[DEPTNO0],dependentColumn:DNAME}",
        "{type:fd,columns:[DEPTNO],dependentColumn:DEPTNO0}",
        "{type:fd,columns:[DEPTNO],dependentColumn:DNAME}",
        "{type:fd,columns:[DNAME],dependentColumn:DEPTNO0}",
        "{type:fd,columns:[DNAME],dependentColumn:DEPTNO}",
        "{type:fd,columns:[HIREDATE],dependentColumn:MGR}",
        "{type:fd,columns:[JOB,DEPTNO0],dependentColumn:MGR}",
        "{type:fd,columns:[JOB,DEPTNO],dependentColumn:MGR}",
        "{type:fd,columns:[JOB,DNAME],dependentColumn:MGR}",
        "{type:fd,columns:[SAL],dependentColumn:DEPTNO0}",
        "{type:fd,columns:[SAL],dependentColumn:DEPTNO}",
        "{type:fd,columns:[SAL],dependentColumn:DNAME}",
        "{type:fd,columns:[SAL],dependentColumn:JOB}",
        "{type:rowCount,rowCount:14}",
        "{type:unique,columns:[EMPNO]}",
        "{type:unique,columns:[ENAME]}",
        "{type:unique,columns:[HIREDATE,DEPTNO0]}",
        "{type:unique,columns:[HIREDATE,DEPTNO]}",
        "{type:unique,columns:[HIREDATE,DNAME]}",
        "{type:unique,columns:[HIREDATE,SAL]}",
        "{type:unique,columns:[JOB,HIREDATE]}");
  }

  @Test public void testProfileZeroRows() throws Exception {
    final String sql = "select * from \"scott\".dept where false";
    checkProfile(sql,
        "{type:rowCount,rowCount:0}",
        "{type:unique,columns:[]}");
  }

  @Test public void testProfileOneRow() throws Exception {
    final String sql = "select * from \"scott\".dept where deptno = 10";
    checkProfile(sql,
        "{type:rowCount,rowCount:1}",
        "{type:unique,columns:[]}");
  }

  @Test public void testProfileTwoRows() throws Exception {
    final String sql = "select * from \"scott\".dept where deptno in (10, 20)";
    checkProfile(sql,
        "{type:distribution,columns:[],cardinality:1.0}",
        "{type:rowCount,rowCount:2}",
        "{type:unique,columns:[DEPTNO]}",
        "{type:unique,columns:[DNAME]}");
  }

  private void checkProfile(String sql, String... lines) throws Exception {
    checkProfile(sql, Matchers.equalsUnordered(lines));
  }

  private void checkProfile(final String sql,
      final Matcher<Iterable<String>> matcher) throws Exception {
    CalciteAssert.that(CalciteAssert.Config.SCOTT)
        .doWithConnection(new Function<CalciteConnection, Void>() {
          public Void apply(CalciteConnection c) {
            try (PreparedStatement s = c.prepareStatement(sql)) {
              final ResultSetMetaData m = s.getMetaData();
              final List<Profiler.Column> columns = new ArrayList<>();
              final int columnCount = m.getColumnCount();
              for (int i = 1; i < columnCount; i++) {
                columns.add(new Profiler.Column(i - 1, m.getColumnLabel(i)));
              }
              final Profiler p = new SimpleProfiler();
              final Enumerable<List<Comparable>> rows = getRows(s);
              final List<Profiler.Statistic> statistics =
                  p.profile(rows, columns);
              final List<String> strings = new ArrayList<>();
              final JsonBuilder jb = new JsonBuilder();
              for (Profiler.Statistic statistic : statistics) {
                final String json = jb.toJsonString(statistic.toMap(jb));
                strings.add(json.replaceAll("\n", "").replaceAll(" ", "")
                    .replaceAll("\"", ""));
              }
              Collections.sort(strings);
              Assert.assertThat(strings, matcher);
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
            return null;
          }
        });
  }

  private Enumerable<List<Comparable>> getRows(final PreparedStatement s) {
    return new AbstractEnumerable<List<Comparable>>() {
      public Enumerator<List<Comparable>> enumerator() {
        try {
          final ResultSet r = s.executeQuery();
          return getListEnumerator(r, r.getMetaData().getColumnCount());
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private Enumerator<List<Comparable>> getListEnumerator(
      final ResultSet r, final int columnCount) {
    return new Enumerator<List<Comparable>>() {
      final Comparable[] values = new Comparable[columnCount];

      public List<Comparable> current() {
        for (int i = 0; i < columnCount; i++) {
          try {
            final Comparable value = (Comparable) r.getObject(i + 1);
            values[i] = NullSentinel.mask(value);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
        return ImmutableList.copyOf(values);
      }

      public boolean moveNext() {
        try {
          return r.next();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      public void reset() {
      }

      public void close() {
        try {
          r.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}

// End ProfilerTest.java
