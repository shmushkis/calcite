---
layout: docs
title: Algebra
permalink: /docs/algebra.html
---
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}

{% assign sourceRoot = "http://github.com/apache/incubator-calcite/blob/master" %}

Relational algebra is at the heart of Calcite. Every query is
represented as a tree of relational operators. You can translate from
SQL to relational algebra, or you can build the tree directly.

Planner rules transform expression trees using mathematical identities
that preserve semantics. For example, it is valid to push a filter
into an input of an inner join if the filter does not reference
columns from the other input.

Calcite optimizes queries by repeatedly applying planner rules to a
relational expression. A cost model guides the process, and the
planner engine generates an alternative expression that has the same
semantics as the original but a lower cost.

The planning process is extensible. You can add your own relational
operators, planner rules, cost model, and statistics.

## Algebra builder

The simplest way to build a relational expression is to use the algebra
builder. Here is an example (you can find the full code in
[RelBuilderExample.java]({{ sourceRoot }}/src/test/java/org/apache/calcite/examples/RelBuilderExample.java)):

### TableScan

{% highlight java %}
final FrameworkConfig config;
final RelBuilder builder = RelBuilder.create(config);
final RelNode node = builder
  .scan("EMP")
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

The code prints

{% highlight text %}
LogicalTableScan(table=[[scott, EMP]])
{% endhighlight %}

It has created a scan of the `EMP` table; equivalent to the SQL

{% highlight sql %}
SELECT *
FROM scott.EMP;
{% endhighlight %}

### Adding a Project

Now, let's add a Project, the equivalent of

{% highlight sql %}
SELECT ename, deptno
FROM scott.EMP;
{% endhighlight %}

We just add a call to the `project` method before calling
`build`:

{% highlight java %}
final RelNode node = builder
  .scan("EMP")
  .project(builder.field("DEPTNO"), builder.field("ENAME"))
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

and the output is

{% highlight text %}
LogicalProject(DEPTNO=[$7], ENAME=[$1])
  LogicalTableScan(table=[[scott, EMP]])
{% endhighlight %}

The two calls to `builder.field` create simple expressions
that return the fields from the input relational expression,
namely the TableScan created by the `scan` call.

Calcite has converted them to field references by ordinal,
`$7` and `$1`.

### Adding a Filter and Aggregate

A query with an Aggregate, and a Filter:

{% highlight java %}
final RelNode node = builder
  .scan("EMP")
  .filter(builder.field("DEPTNO"), builder.field("ENAME"))
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

is equivalent to SQL

{% highlight sql %}
SELECT deptno, count(*) AS c, sum(sal) AS s
FROM emp
GROUP BY deptno
HAVING count(*) > 10
{% endhighlight %}

and produces 

{% highlight text %}
LogicalFilter(condition=[>($1, 10)])
  LogicalAggregate(group=[{7}], C=[COUNT()], S=[SUM($5)])
    LogicalTableScan(table=[[scott, EMP]])
{% endhighlight %}


### API summary

#### Relational operators

| Method              | Description
|:------------------- |:-----------
| `scan(tableName)` | Creates a [TableScan](/apidocs/org/apache/calcite/rel/core/TableScan.html)
| `values(rowType, tupleList)` | Creates a [Values](/apidocs/org/apache/calcite/rel/core/Values.html)
| `filter(expr)` | Creates a [Filter](/apidocs/org/apache/calcite/rel/core/Filter.html)
| `project(expr)` | Creates a [Filter](/apidocs/org/apache/calcite/rel/core/Filter.html)
| `aggregate(groupKey, aggCall...)` | Creates an [Aggregate](/apidocs/org/apache/calcite/rel/core/Aggregate.html)
| `sort(int...)`<br/>`sort(expr...)`<br/>`sort(exprList)` | Creates a [Sort](/apidocs/org/apache/calcite/rel/core/Sort.html)
| `sortLimit(offset, fetch, expr...)`<br/>`sort(RexNode...)`<br/>`sort(List<RexNode>)` | Creates a [Sort](/apidocs/org/apache/calcite/rel/core/Sort.html)

Argument types:

* `expr`  [RexNode](/apidocs/org/apache/calcite/rex/RexNode.html)
* `expr...` Array of [RexNode](/apidocs/org/apache/calcite/rex/RexNode.html)
* `exprList` Iterable of [RexNode](/apidocs/org/apache/calcite/rex/RexNode.html)
* `rowType` [RelDataType](/apidocs/org/apache/calcite/rel/type/RelDataType.html)
* `groupKey` [RelBuilder.GroupKey](/apidocs/org/apache/calcite/tools/RelBuilder/GroupKey.html)
* `aggCall` [RelBuilder.AggCall](/apidocs/org/apache/calcite/tools/RelBuilder/AggCall.html)
* `tupleList` Iterable of List of [RexLiteral](/apidocs/org/apache/calcite/rex/RexLiteral.html)


#### Project

| Method              | Description
|:------------------- |:-----------
| `project(RexNode...)`       | Projects input fields 3, 5
| `project("A", "B")`   | Projects the input fields named "A" and "B"

`project(3, 5)` Projects inputs fields 3 and 5
`project(builder.field(3), builder.literal("a"))` Projects input field 3 and literal 'a'

| Method              | Description
|:------------------- |:-----------
| `project(RexNode...)`       | Projects input fields 3, 5
| `project("A", "B")`   | Projects the input fields named "A" and "B"
| `filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("DEPTNO"), builder.literal(20)))` | Filters based on boolean expression <i>expr</i>
| `aggregate(<i>groupKey</i>,<br/>  <i>aggCall</i>...)` | Aggregates by <i>groupKey</i>
| `sort(<i>)` |
