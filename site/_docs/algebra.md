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

The following methods create a relational expression
([RelNode](/apidocs/org/apache/calcite/rel/RelNode.html)),
push it onto the stack, and
return the `RelBuilder`.

| Method              | Description
|:------------------- |:-----------
| `scan(tableName)` | Creates a [TableScan](/apidocs/org/apache/calcite/rel/core/TableScan.html)
| `values(fieldNames, value...)`<br/>`values(rowType, tupleList)` | Creates a [Values](/apidocs/org/apache/calcite/rel/core/Values.html)
| `filter(expr)` | Creates a [Filter](/apidocs/org/apache/calcite/rel/core/Filter.html)
| `project(expr...)`<br/>`project(exprList)` | Creates a [Project](/apidocs/org/apache/calcite/rel/core/Project.html). To override the default name, wrap expressions using `alias`.
| `aggregate(groupKey, aggCall...)` | Creates an [Aggregate](/apidocs/org/apache/calcite/rel/core/Aggregate.html)
| `sort(fieldOrdinal...)`<br/>`sort(expr...)`<br/>`sort(exprList)` | Creates a [Sort](/apidocs/org/apache/calcite/rel/core/Sort.html).<br/><br/>In the first form, field ordinals are 0-based, and a negative ordinal indicates descending; for example, -2 means field 1 descending.<br/><br/>In the other forms, you can wrap expressions in `as`, `nullsFirst` or `nullsLast`.
| `sortLimit(offset, fetch, expr...)`<br/>`sortLimit(offset, fetch, exprList)` | Creates a [Sort](/apidocs/org/apache/calcite/rel/core/Sort.html) with offset and limit
| `limit(offset, fetch)` | Creates a [Sort](/apidocs/org/apache/calcite/rel/core/Sort.html) that does not sort, only applies with offset and limit
| `join(joinType, expr)`<br/>`join(joinType, fieldName...)` | Creates a [Join](/apidocs/org/apache/calcite/rel/core/Join.html)
| `union(all)` | Creates a [Union](/apidocs/org/apache/calcite/rel/core/Union.html) of the two most recent relational expressions
| `intersect(all)` | Creates an [Intersect](/apidocs/org/apache/calcite/rel/core/Intersect.html) of the two most recent relational expressions
| `minus(all)` | Creates a [Minus](/apidocs/org/apache/calcite/rel/core/Minus.html) of the two most recent relational expressions

Argument types:

* `expr`  [RexNode](/apidocs/org/apache/calcite/rex/RexNode.html)
* `expr...` Array of [RexNode](/apidocs/org/apache/calcite/rex/RexNode.html)
* `exprList` Iterable of [RexNode](/apidocs/org/apache/calcite/rex/RexNode.html)
* `fieldOrdinal` Ordinal of a field within its row (starting from 0)
* `fieldName` Name of a field, unique within its row
* `fieldName...` Array of String
* `fieldNames` Iterable of String
* `rowType` [RelDataType](/apidocs/org/apache/calcite/rel/type/RelDataType.html)
* `groupKey` [RelBuilder.GroupKey](/apidocs/org/apache/calcite/tools/RelBuilder/GroupKey.html)
* `aggCall` [RelBuilder.AggCall](/apidocs/org/apache/calcite/tools/RelBuilder/AggCall.html)
* `value...` Array of Object
* `value` Object
* `tupleList` Iterable of List of [RexLiteral](/apidocs/org/apache/calcite/rex/RexLiteral.html)
* `all` boolean
* `distinct` boolean
* `alias` String

### Stack methods

| Method              | Description
|:------------------- |:-----------
| `build()`           | Pops the most recently created relational expression off the stack
| `push(rel)`         | Pushes a relational expression onto the stack. Relational methods such as `scan`, above, call this method, but user code generally does not
| `peek()`            | Returns the relational expression most recently put onto the stack, but does not remove it

#### Scalar expression methods

The following methods return a scalar expression
([RexNode](/apidocs/org/apache/calcite/rex/RexNode.html)).

Many of them use the contents of the stack. For example, `field("DEPTNO")`
returns a reference to the "DEPTNO" field of the relational expression just
added to the stack.

| Method              | Description
|:------------------- |:-----------
| `field(fieldName)` | Reference, by name, to a field of the top-most relational expression
| `field(fieldOrdinal)` | Reference, by ordinal, to a field of the top-most relational expression
| `field(inputCount, inputOrdinal, fieldName)` | Reference, by name, to a field of the (`inputCount` - `inputOrdinal`)th relational expression
| `field(inputCount, inputOrdinal, fieldOrdinal)` | Reference, by ordinal, to a field of the (`inputCount` - `inputOrdinal`)th relational expression
| `call(op, expr...)`<br/>`call(op, exprList)` | Call to a function or operator
| `literal(value)` | Constant
| `alias(expr, fieldName)` | Renames an expression (only valid as an argument to `project`)
| `cast(expr, typeName)`<br/>`cast(expr, typeName, precision)`<br/>`cast(expr, typeName, precision, scale)`<br/> | Converts an expression to a given type
| `desc(expr)` | Changes sort direction to descending (only valid as an argument to `sort` or `sortLimit`)
| `nullsFirst(expr)` | Changes sort order to nulls first (only valid as an argument to `sort` or `sortLimit`)
| `nullsLast(expr)` | Changes sort order to nulls last (only valid as an argument to `sort` or `sortLimit`)

### Group key methods

The following methods return a
[RelBuilder.GroupKey](/apidocs/org/apache/calcite/tools/RelBuilder/GroupKey.html).

| Method              | Description
|:------------------- |:-----------
| `groupKey(fieldName...)`<br/>`groupKey(fieldOrdinal...)`<br/>`groupKey(expr...)`<br/>`groupKey(exprList)` | Creates a group key of the given expressions

### Aggregate call methods

The following methods return an
[RelBuilder.AggCall](/apidocs/org/apache/calcite/tools/RelBuilder/AggCall.html).

| Method              | Description
|:------------------- |:-----------
| `aggregateCall(op, distinct, alias, expr...)`<br/>`aggregateCall(op, distinct, alias, exprList)` | Creates a call to a given aggregate function
| `count(distinct, alias, expr...)` | Creates a call to the COUNT aggregate function
| `countStar(alias)` | Creates a call to the COUNT(*) aggregate function
| `sum(distinct, alias, expr)` | Creates a call to the SUM aggregate function
| `min(alias, expr)` | Creates a call to the MIN aggregate function
| `max(alias, expr)` | Creates a call to the MAX aggregate function
