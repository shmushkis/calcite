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

import org.apache.calcite.piglet.Ast;
import org.apache.calcite.piglet.Handler;
import org.apache.calcite.piglet.parser.ParseException;
import org.apache.calcite.piglet.parser.PigletParser;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.tools.PigRelBuilder;

import org.junit.Ignore;
import org.junit.Test;

import java.io.StringReader;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Unit tests for Piglet. */
public class PigletTest {
  private Ast.Program parseProgram(String s) throws ParseException {
    return new PigletParser(new StringReader(s)).stmtListEof();
  }

  @Test public void testParseLoad() throws ParseException {
    final String s = "A = LOAD 'Emp';";
    Ast.Program x = parseProgram(s);
    final String expected = "{op: PROGRAM, stmts: [\n"
        + "  {op: LOAD, target: A, name: Emp}]}";
    assertThat(Ast.toString(x), is(expected));
  }

  /** Tests parsing and un-parsing all kinds of operators. */
  @Test public void testParse2() throws ParseException {
    final String s = "A = LOAD 'Emp';\n"
        + "DESCRIBE A;\n"
        + "DUMP A;\n"
        + "B = FOREACH A GENERATE 1, name;\n"
        + "B1 = FOREACH A {\n"
        + "  X = DISTINCT A;\n"
        + "  Y = FILTER X BY foo;\n"
        + "  Z = LIMIT Z 3;\n"
        + "  GENERATE 1, name;\n"
        + "}\n"
        + "C = FILTER B BY name;\n"
        + "D = DISTINCT C;\n"
        + "E = ORDER D BY $1 DESC, $2 ASC, $3;\n"
        + "F = ORDER E BY * DESC;\n"
        + "G = LIMIT F -10;\n"
        + "H = GROUP G ALL;\n"
        + "I = GROUP H BY e;\n"
        + "J = GROUP I BY (e1, e2);\n";
    Ast.Program x = parseProgram(s);
    final String expected = "{op: PROGRAM, stmts: [\n"
        + "  {op: LOAD, target: A, name: Emp},\n"
        + "  {op: DESCRIBE, relation: A},\n"
        + "  {op: DUMP, relation: A},\n"
        + "  {op: FOREACH, target: B, source: A, expList: [\n"
        + "    1,\n"
        + "    name]},\n"
        + "  {op: FOREACH, target: B1, source: A, nestedOps: [\n"
        + "    {op: DISTINCT, target: X, source: A},\n"
        + "    {op: FILTER, target: Y, source: X, condition: foo},\n"
        + "    {op: LIMIT, target: Z, source: Z, count: 3}], expList: [\n"
        + "    1,\n"
        + "    name]},\n"
        + "  {op: FILTER, target: C, source: B, condition: name},\n"
        + "  {op: DISTINCT, target: D, source: C},\n"
        + "  {op: ORDER, target: E, source: D},\n"
        + "  {op: ORDER, target: F, source: E},\n"
        + "  {op: LIMIT, target: G, source: F, count: -10},\n"
        + "  {op: GROUP, target: H, source: G},\n"
        + "  {op: GROUP, target: I, source: H, keys: [\n"
        + "    e]},\n"
        + "  {op: GROUP, target: J, source: I, keys: [\n"
        + "    e1,\n"
        + "    e2]}]}";
    assertThat(Ast.toString(x), is(expected));
  }

  @Test public void testScan() throws ParseException {
    final String s = "A = LOAD 'EMP';";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = "LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Test public void testDump() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "DUMP A;";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = "LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Test public void testForeach() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = FOREACH A GENERATE DNAME, $2;";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = "LogicalProject(DNAME=[$1], LOC=[$2])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Ignore // foreach nested not implemented yet
  @Test public void testForeachNested() throws ParseException {
    final String s = "A = LOAD 'EMP';\n"
        + "B = GROUP A BY DEPTNO;\n"
        + "C = FOREACH B {\n"
        + "  D = ORDER A BY SAL DESC;\n"
        + "  E = LIMIT D 3;\n"
        + "  GENERATE E.DEPTNO, E.EMPNO;\n"
        + "}";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = "LogicalProject(DNAME=[$1], LOC=[$2])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Test public void testGroup() throws ParseException {
    final String s = "A = LOAD 'EMP';\n"
        + "B = GROUP A BY DEPTNO;";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = ""
        + "LogicalAggregate(group=[{7}], A=[COLLECT($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Test public void testFilter() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = FILTER A BY DEPTNO;";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = "LogicalFilter(condition=[$0])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Test public void testLimit() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = LIMIT A 3;";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = "LogicalSort(fetch=[3])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Test public void testOrder() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = ORDER A BY DEPTNO DESC, DNAME;";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = ""
        + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[DESC], dir1=[ASC])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }

  @Test public void testOrderStar() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = ORDER A BY * DESC;";
    Ast.Program x = parseProgram(s);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(x);
    final String expected = ""
        + "LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[DESC], dir1=[DESC], dir2=[DESC])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(RelOptUtil.toString(builder.peek()), is(expected));
  }
}

// End PigletTest.java
