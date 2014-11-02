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
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Function2;
import net.hydromatic.linq4j.function.Functions;

import net.hydromatic.linq4j.function.Predicate2;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link net.hydromatic.optiq.runtime.Enumerables}.
 */
public class EnumerablesTest {
  private static final Enumerable<Emp> EMPS = Linq4j.asEnumerable(
      Arrays.asList(
          new Emp(10, "Fred"),
          new Emp(20, "Theodore"),
          new Emp(20, "Sebastian"),
          new Emp(30, "Joe")));

  private static final Enumerable<Dept> DEPTS = Linq4j.asEnumerable(
      Arrays.asList(
          new Dept(20, "Sales"),
          new Dept(15, "Marketing")));

  private static final Function2<Emp, Dept, String> EMP_DEPT_TO_STRING =
      new Function2<Emp, Dept, String>() {
        public String apply(Emp v0, Dept v1) {
          return "{" + (v0 == null ? null : v0.name)
              + ", " + (v0 == null ? null : v0.deptno)
              + ", " + (v1 == null ? null : v1.deptno)
              + ", " + (v1 == null ? null : v1.name)
              + "}";
        }
      };

  private static final Predicate2<Emp, Dept> EQUAL_DEPTNO =
      new Predicate2<Emp, Dept>() {
        public boolean apply(Emp v0, Dept v1) {
          return v0.deptno == v1.deptno;
        }
      };

  @Test public void testSemiJoin() {
    assertThat(
        Enumerables.semiJoin(
            EMPS,
            DEPTS,
            new Function1<Emp, Integer>() {
              public Integer apply(Emp a0) {
                return a0.deptno;
              }
            },
            new Function1<Dept, Integer>() {
              public Integer apply(Dept a0) {
                return a0.deptno;
              }
            },
            Functions.<Integer>identityComparer()).toList().toString(),
        equalTo("[Emp(20, Theodore), Emp(20, Sebastian)]"));
  }

  @Test public void testThetaJoin() {
    assertThat(
        Enumerables.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO, EMP_DEPT_TO_STRING,
            false, false).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}]"));
  }

  @Test public void testThetaLeftJoin() {
    assertThat(
        Enumerables.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO, EMP_DEPT_TO_STRING,
            false, true).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}]"));
  }

  @Test public void testThetaRightJoin() {
    assertThat(
        Enumerables.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO, EMP_DEPT_TO_STRING,
            true, false).toList().toString(),
        equalTo("[{Theodore, 20, 20, Sales}, {Sebastian, 20, 20, Sales}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test public void testThetaFullJoin() {
    assertThat(
        Enumerables.thetaJoin(EMPS, DEPTS, EQUAL_DEPTNO, EMP_DEPT_TO_STRING,
            true, true).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, 20, Sales}, "
            + "{Sebastian, 20, 20, Sales}, {Joe, 30, null, null}, "
            + "{null, null, 15, Marketing}]"));
  }

  @Test public void testThetaFullJoinLeftEmpty() {
    assertThat(
        Enumerables.thetaJoin(EMPS.take(0), DEPTS, EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, true).toList().toString(),
        equalTo("[{null, null, 20, Sales}, {null, null, 15, Marketing}]"));
  }

  @Test public void testThetaFullJoinRightEmpty() {
    assertThat(
        Enumerables.thetaJoin(EMPS, DEPTS.take(0), EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, true).toList().toString(),
        equalTo("[{Fred, 10, null, null}, {Theodore, 20, null, null}, "
            + "{Sebastian, 20, null, null}, {Joe, 30, null, null}]"));
  }

  @Test public void testThetaFullJoinBothEmpty() {
    assertThat(
        Enumerables.thetaJoin(EMPS.take(0), DEPTS.take(0), EQUAL_DEPTNO,
            EMP_DEPT_TO_STRING, true, true).toList().toString(),
        equalTo("[]"));
  }

  /** Employee record. */
  private static class Emp {
    final int deptno;
    final String name;

    Emp(int deptno, String name) {
      this.deptno = deptno;
      this.name = name;
    }

    @Override public String toString() {
      return "Emp(" + deptno + ", " + name + ")";
    }
  }

  /** Department record. */
  private static class Dept {
    final int deptno;
    final String name;

    Dept(int deptno, String name) {
      this.deptno = deptno;
      this.name = name;
    }

    @Override public String toString() {
      return "Dept(" + deptno + ", " + name + ")";
    }
  }
}

// End EnumerablesTest.java
