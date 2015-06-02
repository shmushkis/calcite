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
package org.apache.calcite.examples;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/**
 * Example that uses {@link org.apache.calcite.tools.RelBuilder}
 * to create various relational expressions.
 */
public class RelBuilderExample {
  // private constructor to prevent instantiation
  private RelBuilderExample() {}

  public static void main(String[] args) {
    // Create a builder. The config contains a schema mapped
    // to the SCOTT database, with tables EMP and DEPT.
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    example1(builder);
    example2(builder);
    example3(builder);
  }

  /**
   * Creates a relational expression for a table scan.
   * It is equivalent to
   *
   * <pre>
   * SELECT *
   * FROM emp</pre>
   */
  private static void example1(RelBuilder builder) {
    final RelNode node = builder
        .scan("EMP")
        .build();
    System.out.println(RelOptUtil.toString(node));
  }

  /**
   * Creates a relational expression for a table scan and project.
   * It is equivalent to
   *
   * <pre>
   * SELECT deptno, ename
   * FROM emp</pre>
   */
  private static void example2(RelBuilder builder) {
    final RelNode node = builder
        .scan("EMP")
        .project(builder.field("DEPTNO"), builder.field("ENAME"))
        .build();
    System.out.println(RelOptUtil.toString(node));
  }

  /**
   * Creates a relational expression for a table scan, aggregate, filter.
   * It is equivalent to
   *
   * <pre>
   * SELECT deptno, count(*) AS c, sum(sal) AS s
   * FROM emp
   * GROUP BY deptno
   * HAVING count(*) > 10</pre>
   */
  private static void example3(RelBuilder builder) {
    final RelNode node = builder
        .scan("EMP")
        .aggregate(builder.groupKey(builder.field("DEPTNO")),
            builder.aggregateCall(SqlStdOperatorTable.COUNT,
                false, "C"),
            builder.aggregateCall(SqlStdOperatorTable.SUM,
                false, "S", builder.field("SAL")))
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN,
                builder.field("C"), builder.literal(10)))
        .build();
    System.out.println(RelOptUtil.toString(node));
  }
}

// End RelBuilderExample.java
