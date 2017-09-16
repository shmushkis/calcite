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

import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import org.junit.Test;

/**
 * Tests SQL parser extensions for DDL.
 *
 * <p>Todo:
 * <ul>
 *
 * <li>"create table x (a int) as values 1, 2" should fail validation;
 * data type not allowed in "create table ... as".
 *
 * <li>"create table x (a int, b int as (a + 1)) stored"
 * should not allow b to be specified in insert;
 * should generate check constraint on b;
 * should populate b in insert as if it had a default
 *
 * <li>test that "create table as select" works
 *
 * <li>"create table as select" should store constraints
 * deduced by planner
 *
 * <li>add "create schema" (both local and foreign)
 *
 * <li>add "drop table"
 *
 * <li>add "drop schema"
 *
 * <li>add "drop view"
 *
 * <li>add Quidem test(s)
 *
 * </ul>
 */
public class ServerParserTest extends SqlParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return SqlDdlParserImpl.FACTORY;
  }

  @Test public void testCreateTable() {
    sql("create table x (i int not null, j varchar(5) null)")
        .ok("CREATE TABLE `X` (`I` INTEGER NOT NULL, `J` VARCHAR(5))");
  }

  @Test public void testCreateTableAsSelect() {
    final String expected = "CREATE TABLE `X` AS\n"
        + "SELECT *\n"
        + "FROM `EMP`";
    sql("create table x as select * from emp")
        .ok(expected);
  }

  @Test public void testCreateTableAsValues() {
    final String expected = "CREATE TABLE `X` AS\n"
        + "VALUES (ROW(1)),\n"
        + "(ROW(2))";
    sql("create table x as values 1, 2")
        .ok(expected);
  }

  @Test public void testCreateTableAsSelectColumnList() {
    final String expected = "CREATE TABLE `X` (`A`, `B`) AS\n"
        + "SELECT *\n"
        + "FROM `EMP`";
    sql("create table x (a, b) as select * from emp")
        .ok(expected);
  }

  @Test public void testCreateTableCheck() {
    final String expected = "CREATE TABLE `X` (`I` INTEGER NOT NULL,"
        + " CONSTRAINT `C1` CHECK (`I` < 10), `J` INTEGER)";
    sql("create table x (i int not null, constraint c1 check (i < 10), j int)")
        .ok(expected);
  }

  @Test public void testCreateTableVirtualColumn() {
    final String sql = "create table x (\n"
        + " i int not null,\n"
        + " j int generated always as (i + 1) stored,\n"
        + " k int as (j + 1) virtual,\n"
        + " m int as (k + 1))";
    final String expected = "CREATE TABLE `X` (`I` INTEGER NOT NULL,"
        + " `J` INTEGER AS (`I` + 1),"
        + " `K` INTEGER AS (`J` + 1) STORED,"
        + " `M` INTEGER AS (`K` + 1) STORED)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateView() {
    final String sql = "create or replace view v as\n"
        + "select * from (values (1, '2'), (3, '45')) as t (x, y)";
    final String expected = "CREATE OR REPLACE VIEW `V` AS\n"
        + "SELECT *\n"
        + "FROM (VALUES (ROW(1, '2')),\n"
        + "(ROW(3, '45'))) AS `T` (`X`, `Y`)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateMaterializedView() {
    final String sql = "create or replace materialized view mv (d, v) as\n"
        + "select deptno, count(*) from emp\n"
        + "group by deptno order by deptno desc";
    final String expected = "CREATE OR REPLACE"
        + " MATERIALIZED VIEW `MV` (`D`, `V`) AS\n"
        + "SELECT `DEPTNO`, COUNT(*)\n"
        + "FROM `EMP`\n"
        + "GROUP BY `DEPTNO`\n"
        + "ORDER BY `DEPTNO` DESC";
    sql(sql).ok(expected);
  }

}

// End ServerParserTest.java
