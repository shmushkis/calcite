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
package org.apache.calcite.tools;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Stacks;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Builder for relational expressions.
 *
 * <p>{@code RelBuilder} does not make possible anything that you could not
 * also accomplish by calling the factory methods of the particular relational
 * expression. But it makes common tasks more straightforward and concise.
 *
 * <p>{@code RelBuilder} uses factories to create relational expressions.
 * By default, it uses the default factories, which create logical relational
 * expressions ({@link org.apache.calcite.rel.logical.LogicalFilter},
 * {@link org.apache.calcite.rel.logical.LogicalProject} and so forth).
 * But you could override those factories so that, say, {@code filter} creates
 * instead a {@code HiveFilter}.
 *
 * <p>It is not thread-safe.
 */
public class RelBuilder {
  private static final Function<RexNode, String> FN_TYPE =
      new Function<RexNode, String>() {
        public String apply(RexNode input) {
          return input + ": " + input.getType();
        }
      };

  protected final RelOptCluster cluster;
  protected final RelOptSchema relOptSchema;
  private final RelFactories.FilterFactory filterFactory;
  private final RelFactories.ProjectFactory projectFactory;
  private final RelFactories.AggregateFactory aggregateFactory;
  private final RelFactories.SortFactory sortFactory;
  private final RelFactories.SetOpFactory setOpFactory;
  private final RelFactories.JoinFactory joinFactory;
  private final RelFactories.SemiJoinFactory semiJoinFactory;
  private final RelFactories.ValuesFactory valuesFactory;
  private final RelFactories.TableScanFactory scanFactory;
  private final List<Frame> stack = new ArrayList<>();

  protected RelBuilder(Context context, RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    this.cluster = cluster;
    this.relOptSchema = relOptSchema;
    if (context == null) {
      context = Contexts.EMPTY_CONTEXT;
    }
    this.aggregateFactory =
        Util.first(context.unwrap(RelFactories.AggregateFactory.class),
            RelFactories.DEFAULT_AGGREGATE_FACTORY);
    this.filterFactory =
        Util.first(context.unwrap(RelFactories.FilterFactory.class),
            RelFactories.DEFAULT_FILTER_FACTORY);
    this.projectFactory =
        Util.first(context.unwrap(RelFactories.ProjectFactory.class),
            RelFactories.DEFAULT_PROJECT_FACTORY);
    this.sortFactory =
        Util.first(context.unwrap(RelFactories.SortFactory.class),
            RelFactories.DEFAULT_SORT_FACTORY);
    this.setOpFactory =
        Util.first(context.unwrap(RelFactories.SetOpFactory.class),
            RelFactories.DEFAULT_SET_OP_FACTORY);
    this.joinFactory =
        Util.first(context.unwrap(RelFactories.JoinFactory.class),
            RelFactories.DEFAULT_JOIN_FACTORY);
    this.semiJoinFactory =
        Util.first(context.unwrap(RelFactories.SemiJoinFactory.class),
            RelFactories.DEFAULT_SEMI_JOIN_FACTORY);
    this.valuesFactory =
        Util.first(context.unwrap(RelFactories.ValuesFactory.class),
            RelFactories.DEFAULT_VALUES_FACTORY);
    this.scanFactory =
        Util.first(context.unwrap(RelFactories.TableScanFactory.class),
            RelFactories.DEFAULT_TABLE_SCAN_FACTORY);
  }

  /** Creates a RelBuilder. */
  public static RelBuilder create(FrameworkConfig config) {
    final RelOptCluster[] clusters = {null};
    final RelOptSchema[] relOptSchemas = {null};
    Frameworks.withPrepare(
        new Frameworks.PrepareAction<Void>(config) {
          public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema, CalciteServerStatement statement) {
            clusters[0] = cluster;
            relOptSchemas[0] = relOptSchema;
            return null;
          }
        });
    return new RelBuilder(config.getContext(), clusters[0], relOptSchemas[0]);
  }

  /** Returns the type factory. */
  public RelDataTypeFactory getTypeFactory() {
    return cluster.getTypeFactory();
  }

  /** Returns the builder for {@link RexNode} expressions. */
  public RexBuilder getRexBuilder() {
    return cluster.getRexBuilder();
  }

  /** Creates a {@link RelBuilderFactory}, a partially-created RelBuilder.
   * Just add a {@link RelOptCluster} and a {@link RelOptSchema} */
  public static RelBuilderFactory proto(final Context context) {
    return new RelBuilderFactory() {
      public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
        return new RelBuilder(context, cluster, schema);
      }
    };
  }

  /** Creates a {@link RelBuilderFactory} that uses a given set of factories. */
  public static RelBuilderFactory proto(Object... factories) {
    return proto(Contexts.of(factories));
  }

  // Methods for manipulating the stack

  /** Adds a relational expression to be the input to the next relational
   * expression constructed.
   *
   * <p>This method is usual when you want to weave in relational expressions
   * that are not supported by the builder. If, while creating such expressions,
   * you need to use previously built expressions as inputs, call
   * {@link #build()} to pop those inputs. */
  public RelBuilder push(RelNode node) {
    Stacks.push(stack, new Frame(node));
    return this;
  }

  /** Pushes a collection of relational expressions. */
  public RelBuilder pushAll(Iterable<? extends RelNode> nodes) {
    for (RelNode node : nodes) {
      push(node);
    }
    return this;
  }

  /** Returns the final relational expression.
   *
   * <p>Throws if the stack is empty.
   */
  public RelNode build() {
    return Stacks.pop(stack).rel;
  }

  /** Returns the relational expression at the top of the stack, but does not
   * remove it. */
  public RelNode peek() {
    return Stacks.peek(stack).rel;
  }

  /** Returns the relational expression {@code n} positions from the top of the
   * stack, but does not remove it. */
  public RelNode peek(int n) {
    return Stacks.peek(n, stack).rel;
  }

  /** Returns the relational expression {@code n} positions from the top of the
   * stack, but does not remove it. */
  public RelNode peek(int inputCount, int inputOrdinal) {
    return Stacks.peek(inputCount - 1 - inputOrdinal, stack).rel;
  }

  // Methods that return scalar expressions

  /** Creates a literal (constant expression). */
  public RexNode literal(Object value) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (value == null) {
      return rexBuilder.constantNull();
    } else if (value instanceof Boolean) {
      return rexBuilder.makeLiteral((Boolean) value);
    } else if (value instanceof BigDecimal) {
      return rexBuilder.makeExactLiteral((BigDecimal) value);
    } else if (value instanceof Float || value instanceof Double) {
      return rexBuilder.makeApproxLiteral(
          BigDecimal.valueOf(((Number) value).doubleValue()));
    } else if (value instanceof Number) {
      return rexBuilder.makeExactLiteral(
          BigDecimal.valueOf(((Number) value).longValue()));
    } else if (value instanceof String) {
      return rexBuilder.makeLiteral((String) value);
    } else {
      throw new IllegalArgumentException("cannot convert " + value
          + " (" + value.getClass() + ") to a constant");
    }
  }

  /** Creates a reference to a field by name.
   *
   * <p>Equivalent to {@code field(1, 0, fieldName)}.
   *
   * @param fieldName Field name
   */
  public RexInputRef field(String fieldName) {
    return field(1, 0, fieldName);
  }

  /** Creates a reference to a field of given input relational expression
   * by name.
   *
   * @param inputCount Number of inputs
   * @param inputOrdinal Input ordinal
   * @param fieldName Field name
   */
  public RexInputRef field(int inputCount, int inputOrdinal, String fieldName) {
    final RelNode input = peek(inputCount, inputOrdinal);
    final RelDataType rowType = input.getRowType();
    final int ordinal = rowType.getFieldNames().indexOf(fieldName);
    if (ordinal < 0) {
      throw new IllegalArgumentException("field [" + fieldName
          + "] not found; input fields are: " + rowType.getFieldNames());
    }
    return field(inputCount, inputOrdinal, ordinal);
  }

  /** Creates a reference to an input field by ordinal.
   *
   * <p>Equivalent to {@code field(1, 0, ordinal)}.
   *
   * @param fieldOrdinal Field ordinal
   */
  public RexInputRef field(int fieldOrdinal) {
    return field(1, 0, fieldOrdinal);
  }

  /** Creates a reference to a field of a given input relational expression
   * by ordinal.
   *
   * @param inputCount Number of inputs
   * @param inputOrdinal Input ordinal
   * @param fieldOrdinal Field ordinal within input
   */
  public RexInputRef field(int inputCount, int inputOrdinal, int fieldOrdinal) {
    final RelNode input = peek(inputCount, inputOrdinal);
    final RelDataType rowType = input.getRowType();
    if (fieldOrdinal < 0 || fieldOrdinal > rowType.getFieldCount()) {
      throw new IllegalArgumentException("field ordinal [" + fieldOrdinal
          + "] out of range; input fields are: " + rowType.getFieldNames());
    }
    return cluster.getRexBuilder().makeInputRef(input, fieldOrdinal);
  }

  /** Creates a reference to a field of the current record which originated
   * in a relation with a given alias. */
  public RexNode field(String alias, String fieldName) {
    Preconditions.checkNotNull(alias);
    Preconditions.checkNotNull(fieldName);
    final Frame frame = Stacks.peek(stack);
    final List<String> aliases = new ArrayList<>();
    int offset = 0;
    for (Pair<String, RelDataType> pair : frame.right) {
      if (pair.left != null && pair.left.equals(alias)) {
        int i = pair.right.getFieldNames().indexOf(fieldName);
        if (i >= 0) {
          return field(offset + i);
        } else {
          throw new IllegalArgumentException("no field '" + fieldName
              + "' in relation '" + alias
              + "'; fields are: " + pair.right.getFieldNames());
        }
      }
      aliases.add(pair.left);
      offset += pair.right.getFieldCount();
    }
    throw new IllegalArgumentException("no relation wtih alias '" + alias
        + "'; aliases are: " + aliases);
  }

  /** Returns references to the fields of the top input. */
  public ImmutableList<RexNode> fields() {
    return fields(1, 0);
  }

  /** Returns references to the fields of a given input. */
  public ImmutableList<RexNode> fields(int inputCount, int inputOrdinal) {
    final RelNode input = peek(inputCount, inputOrdinal);
    final RelDataType rowType = input.getRowType();
    final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
    for (int fieldOrdinal : Util.range(rowType.getFieldCount())) {
      nodes.add(field(inputCount, inputOrdinal, fieldOrdinal));
    }
    return nodes.build();
  }

  /** Returns references to fields for a given collation. */
  public ImmutableList<RexNode> fields(RelCollation collation) {
    final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
    for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      RexNode node = field(fieldCollation.getFieldIndex());
      switch (fieldCollation.direction) {
      case DESCENDING:
        node = desc(node);
      }
      switch (fieldCollation.nullDirection) {
      case FIRST:
        node = nullsFirst(node);
        break;
      case LAST:
        node = nullsLast(node);
        break;
      }
      nodes.add(node);
    }
    return nodes.build();
  }

  /** Returns references to fields for a given list of input ordinals. */
  public ImmutableList<RexNode> fields(List<? extends Number> ordinals) {
    final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
    for (Number ordinal : ordinals) {
      RexNode node = field(ordinal.intValue());
      nodes.add(node);
    }
    return nodes.build();
  }

  /** Returns references to fields identified by name. */
  public ImmutableList<RexNode> fields(Iterable<String> fieldNames) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (String fieldName : fieldNames) {
      builder.add(field(fieldName));
    }
    return builder.build();
  }

  /** Returns references to fields identified by a mapping. */
  public ImmutableList<RexNode> fields(Mappings.TargetMapping mapping) {
    return fields(Mappings.asList(mapping));
  }

  /** Creates an access to a field by name. */
  public RexNode dot(RexNode node, String fieldName) {
    final RexBuilder builder = cluster.getRexBuilder();
    return builder.makeFieldAccess(node, fieldName, true);
  }

  /** Creates an access to a field by ordinal. */
  public RexNode dot(RexNode node, int fieldOrdinal) {
    final RexBuilder builder = cluster.getRexBuilder();
    return builder.makeFieldAccess(node, fieldOrdinal);
  }

  /** Creates a call to a scalar operator. */
  public RexNode call(SqlOperator operator, RexNode... operands) {
    final RexBuilder builder = cluster.getRexBuilder();
    final List<RexNode> operandList = ImmutableList.copyOf(operands);
    final RelDataType type = builder.deriveReturnType(operator, operandList);
    if (type == null) {
      throw new IllegalArgumentException("cannot derive type: " + operator
          + "; operands: " + Lists.transform(operandList, FN_TYPE));
    }
    return builder.makeCall(type, operator, operandList);
  }

  /** Creates a call to a scalar operator. */
  public RexNode call(SqlOperator operator,
      Iterable<? extends RexNode> operands) {
    return cluster.getRexBuilder().makeCall(operator,
        ImmutableList.copyOf(operands));
  }

  /** Creates an AND. */
  public RexNode and(RexNode... operands) {
    return and(ImmutableList.copyOf(operands));
  }

  /** Creates an AND. */
  public RexNode and(Iterable<? extends RexNode> operands) {
    return RexUtil.composeConjunction(cluster.getRexBuilder(), operands, false);
  }

  /** Creates an OR. */
  public RexNode or(RexNode... operands) {
    return or(ImmutableList.copyOf(operands));
  }

  /** Creates an OR. */
  public RexNode or(Iterable<? extends RexNode> operands) {
    return RexUtil.composeDisjunction(cluster.getRexBuilder(), operands, false);
  }

  /** Creates a NOT. */
  public RexNode not(RexNode operand) {
    return call(SqlStdOperatorTable.NOT, operand);
  }

  /** Creates an =. */
  public RexNode equals(RexNode operand0, RexNode operand1) {
    return call(SqlStdOperatorTable.EQUALS, operand0, operand1);
  }

  /** Creates a IS NULL. */
  public RexNode isNull(RexNode operand) {
    return call(SqlStdOperatorTable.IS_NULL, operand);
  }

  /** Creates a IS NOT NULL. */
  public RexNode isNotNull(RexNode operand) {
    return call(SqlStdOperatorTable.IS_NOT_NULL, operand);
  }

  /** Creates an expression that casts an expression to a given type. */
  public RexNode cast(RexNode expr, SqlTypeName typeName) {
    final RelDataType type = cluster.getTypeFactory().createSqlType(typeName);
    return cluster.getRexBuilder().makeCast(type, expr);
  }

  /** Creates an expression that casts an expression to a type with a given name
   * and precision or length. */
  public RexNode cast(RexNode expr, SqlTypeName typeName, int precision) {
    final RelDataType type =
        cluster.getTypeFactory().createSqlType(typeName, precision);
    return cluster.getRexBuilder().makeCast(type, expr);
  }

  /** Creates an expression that casts an expression to a type with a given
   * name, precision and scale. */
  public RexNode cast(RexNode expr, SqlTypeName typeName, int precision,
      int scale) {
    final RelDataType type =
        cluster.getTypeFactory().createSqlType(typeName, precision, scale);
    return cluster.getRexBuilder().makeCast(type, expr);
  }

  /**
   * Returns an expression wrapped in an alias.
   *
   * @see #project
   */
  public RexNode alias(RexNode expr, String alias) {
    return call(SqlStdOperatorTable.AS, expr, literal(alias));
  }

  /** Converts a sort expression to descending. */
  public RexNode desc(RexNode node) {
    return call(SqlStdOperatorTable.DESC, node);
  }

  /** Converts a sort expression to nulls last. */
  public RexNode nullsLast(RexNode node) {
    return call(SqlStdOperatorTable.NULLS_LAST, node);
  }

  /** Converts a sort expression to nulls first. */
  public RexNode nullsFirst(RexNode node) {
    return call(SqlStdOperatorTable.NULLS_FIRST, node);
  }

  // Methods that create group keys and aggregate calls

  /** Creates an empty group key. */
  public GroupKey groupKey() {
    return groupKey(ImmutableList.<RexNode>of());
  }

  /** Creates a group key. */
  public GroupKey groupKey(RexNode... nodes) {
    return groupKey(ImmutableList.copyOf(nodes));
  }

  /** Creates a group key. */
  public GroupKey groupKey(Iterable<? extends RexNode> nodes) {
    return new GroupKeyImpl(ImmutableList.copyOf(nodes), false, null, null);
  }

  /** Creates a group key with grouping sets. */
  public GroupKey groupKey(Iterable<? extends RexNode> nodes, boolean indicator,
      Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
    final ImmutableList.Builder<ImmutableList<RexNode>> builder =
        ImmutableList.builder();
    for (Iterable<? extends RexNode> nodeList : nodeLists) {
      builder.add(ImmutableList.copyOf(nodeList));
    }
    return new GroupKeyImpl(ImmutableList.copyOf(nodes), indicator, builder.build(), null);
  }

  /** Creates a group key of fields identified by ordinal. */
  public GroupKey groupKey(int... fieldOrdinals) {
    return groupKey(fields(ImmutableIntList.of(fieldOrdinals)));
  }

  /** Creates a group key of fields identified by name. */
  public GroupKey groupKey(String... fieldNames) {
    return groupKey(fields(ImmutableList.copyOf(fieldNames)));
  }

  /** Creates a group key with grouping sets, both identified by field positions
   * in the underlying relational expression.
   *
   * <p>This method of creating a group key does not allow you to group on new
   * expressions, only column projections, but is efficient, especially when you
   * are coming from an existing {@link Aggregate}. */
  public GroupKey groupKey(ImmutableBitSet groupSet, boolean indicator,
      ImmutableList<ImmutableBitSet> groupSets) {
    if (groupSet.length() > peek().getRowType().getFieldCount()) {
      throw new IllegalArgumentException("out of bounds: " + groupSet);
    }
    if (groupSets == null) {
      groupSets = ImmutableList.of(groupSet);
    }
    final ImmutableList<RexNode> nodes =
        fields(ImmutableIntList.of(groupSet.toArray()));
    final List<ImmutableList<RexNode>> nodeLists =
        Lists.transform(groupSets,
            new Function<ImmutableBitSet, ImmutableList<RexNode>>() {
              public ImmutableList<RexNode> apply(ImmutableBitSet input) {
                return fields(ImmutableIntList.of(input.toArray()));
              }
            });
    return groupKey(nodes, indicator, nodeLists);
  }

  /** Creates a call to an aggregate function. */
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      RexNode filter, String alias, RexNode... operands) {
    return aggregateCall(aggFunction, distinct, filter, alias,
        ImmutableList.copyOf(operands));
  }

  /** Creates a call to an aggregate function. */
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct,
      RexNode filter, String alias, Iterable<? extends RexNode> operands) {
    return new AggCallImpl(aggFunction, distinct, filter, alias,
        ImmutableList.copyOf(operands));
  }

  /** Creates a call to the COUNT aggregate function. */
  public AggCall count(boolean distinct, String alias, RexNode... operands) {
    return aggregateCall(SqlStdOperatorTable.COUNT, distinct, null, alias,
        operands);
  }

  /** Creates a call to the COUNT(*) aggregate function. */
  public AggCall countStar(String alias) {
    return aggregateCall(SqlStdOperatorTable.COUNT, false, null, alias);
  }

  /** Creates a call to the SUM aggregate function. */
  public AggCall sum(boolean distinct, String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.SUM, distinct, null, alias,
        operand);
  }

  /** Creates a call to the AVG aggregate function. */
  public AggCall avg(boolean distinct, String alias, RexNode operand) {
    return aggregateCall(
        SqlStdOperatorTable.AVG, distinct, null, alias, operand);
  }

  /** Creates a call to the MIN aggregate function. */
  public AggCall min(String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.MIN, false, null, alias, operand);
  }

  /** Creates a call to the MAX aggregate function. */
  public AggCall max(String alias, RexNode operand) {
    return aggregateCall(SqlStdOperatorTable.MAX, false, null, alias, operand);
  }

  // Methods that create relational expressions

  /** Creates a {@link org.apache.calcite.rel.core.TableScan} of the table
   * with a given name.
   *
   * <p>Throws if the table does not exist within the current schema.
   *
   * <p>Returns this builder.
   *
   * @param tableName Name of table
   */
  public RelBuilder scan(String tableName) {
    final RelOptTable relOptTable =
        relOptSchema.getTableForMember(ImmutableList.of(tableName));
    if (relOptTable == null) {
      throw Static.RESOURCE.tableNotFound(tableName).ex();
    }
    final RelNode scan = scanFactory.createScan(cluster, relOptTable);
    push(scan);
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.Filter} of an array of
   * predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(RexNode... predicates) {
    return filter(ImmutableList.copyOf(predicates));
  }

  /** Creates a {@link org.apache.calcite.rel.core.Filter} of a list of
   * predicates.
   *
   * <p>The predicates are combined using AND,
   * and optimized in a similar way to the {@link #and} method.
   * If the result is TRUE no filter is created. */
  public RelBuilder filter(Iterable<? extends RexNode> predicates) {
    final RexNode x = RexUtil.composeConjunction(cluster.getRexBuilder(),
        predicates, true);
    if (x != null) {
      final Frame frame = Stacks.pop(stack);
      final RelNode filter = filterFactory.createFilter(frame.rel, x);
      Stacks.push(stack, new Frame(filter, frame.right));
    }
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.Project} of the given list
   * of expressions.
   *
   * <p>Infers names as would {@link #project(Iterable, Iterable)} if all
   * suggested names were null.
   *
   * @param nodes Expressions
   */
  public RelBuilder project(Iterable<? extends RexNode> nodes) {
    return project(nodes, ImmutableList.<String>of());
  }

  /** Creates a {@link org.apache.calcite.rel.core.Project} of the given list
   * of expressions, using the given names.
   *
   * <p>Names are deduced as follows:
   * <ul>
   *   <li>If the length of {@code fieldNames} is greater than the index of
   *     the current entry in {@code nodes}, and the entry in
   *     {@code fieldNames} is not null, uses it; otherwise
   *   <li>If an expression projects an input field,
   *     or is a cast an input field,
   *     uses the input field name; otherwise
   *   <li>If an expression is a call to
   *     {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#AS}
   *     (see {@link #alias}), removes the call but uses the intended alias.
   * </ul>
   *
   * <p>After the field names have been inferred, makes the
   * field names unique by appending numeric suffixes.
   *
   * @param nodes Expressions
   * @param fieldNames Suggested field names
   */
  public RelBuilder project(Iterable<? extends RexNode> nodes,
      Iterable<String> fieldNames) {
    final List<String> names = new ArrayList<>();
    final List<RexNode> exprList = Lists.newArrayList(nodes);
    final Iterator<String> nameIterator = fieldNames.iterator();
    for (RexNode node : nodes) {
      final String name = nameIterator.hasNext() ? nameIterator.next() : null;
      final String name2 = inferAlias(exprList, node);
      names.add(Util.first(name, name2));
    }
    if (ProjectRemoveRule.isIdentity(exprList, peek().getRowType())) {
      return this;
    }
    final RelNode project =
        projectFactory.createProject(build(), ImmutableList.copyOf(exprList),
            names);
    push(project);
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.Project} of the given
   * expressions. */
  public RelBuilder project(RexNode... nodes) {
    return project(ImmutableList.copyOf(nodes));
  }

  /** Infers the alias of an expression.
   *
   * <p>If the expression was created by {@link #alias}, replaces the expression
   * in the project list.
   */
  private String inferAlias(List<RexNode> exprList, RexNode expr) {
    switch (expr.getKind()) {
    case INPUT_REF:
      final RexInputRef ref = (RexInputRef) expr;
      return peek(0).getRowType().getFieldNames().get(ref.getIndex());
    case CAST:
      return inferAlias(exprList, ((RexCall) expr).getOperands().get(0));
    case AS:
      final RexCall call = (RexCall) expr;
      for (;;) {
        final int i = exprList.indexOf(expr);
        if (i < 0) {
          break;
        }
        exprList.set(i, call.getOperands().get(0));
      }
      return ((NlsString) ((RexLiteral) call.getOperands().get(1)).getValue())
          .getValue();
    default:
      return null;
    }
  }

  /** Creates an {@link org.apache.calcite.rel.core.Aggregate} that makes the
   * relational expression distinct on all fields. */
  public RelBuilder distinct() {
    return aggregate(groupKey(fields()));
  }

  /** Creates an {@link org.apache.calcite.rel.core.Aggregate} with an array of
   * calls. */
  public RelBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
    return aggregate(groupKey, ImmutableList.copyOf(aggCalls));
  }

  /** Creates an {@link org.apache.calcite.rel.core.Aggregate} with a list of
   * calls. */
  public RelBuilder aggregate(GroupKey groupKey, Iterable<AggCall> aggCalls) {
    final RelDataType inputRowType = peek().getRowType();
    final List<RexNode> extraNodes = projects(inputRowType);
    final GroupKeyImpl groupKey_ = (GroupKeyImpl) groupKey;
    final ImmutableBitSet groupSet =
        ImmutableBitSet.of(registerExpressions(extraNodes, groupKey_.nodes));
    final ImmutableList<ImmutableBitSet> groupSets;
    if (groupKey_.nodeLists != null) {
      final int sizeBefore = extraNodes.size();
      final SortedSet<ImmutableBitSet> groupSetSet =
          new TreeSet<>(ImmutableBitSet.ORDERING);
      for (ImmutableList<RexNode> nodeList : groupKey_.nodeLists) {
        final ImmutableBitSet groupSet2 =
            ImmutableBitSet.of(registerExpressions(extraNodes, nodeList));
        if (!groupSet.contains(groupSet2)) {
          throw new IllegalArgumentException("group set element " + nodeList
              + " must be a subset of group key");
        }
        groupSetSet.add(groupSet2);
      }
      groupSets = ImmutableList.copyOf(groupSetSet);
      if (extraNodes.size() > sizeBefore) {
        throw new IllegalArgumentException(
            "group sets contained expressions not in group key: "
                + extraNodes.subList(sizeBefore, extraNodes.size()));
      }
    } else {
      groupSets = ImmutableList.of(groupSet);
    }
    for (AggCall aggCall : aggCalls) {
      if (aggCall instanceof AggCallImpl) {
        final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
        registerExpressions(extraNodes, aggCall1.operands);
        if (aggCall1.filter != null) {
          registerExpression(extraNodes, aggCall1.filter);
        }
      }
    }
    if (extraNodes.size() > inputRowType.getFieldCount()) {
      project(extraNodes);
    }
    final RelNode r = build();
    final List<AggregateCall> aggregateCalls = new ArrayList<>();
    for (AggCall aggCall : aggCalls) {
      final AggregateCall aggregateCall;
      if (aggCall instanceof AggCallImpl) {
        final AggCallImpl aggCall1 = (AggCallImpl) aggCall;
        final List<Integer> args = registerExpressions(extraNodes, aggCall1.operands);
        final int filterArg = aggCall1.filter == null ? -1
            : registerExpression(extraNodes, aggCall1.filter);
        aggregateCall =
            AggregateCall.create(aggCall1.aggFunction, aggCall1.distinct, args,
                filterArg, groupSet.cardinality(), r, null, aggCall1.alias);
      } else {
        aggregateCall = ((AggCallImpl2) aggCall).aggregateCall;
      }
      aggregateCalls.add(aggregateCall);
    }

    assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
    for (ImmutableBitSet set : groupSets) {
      assert groupSet.contains(set);
    }
    RelNode aggregate = aggregateFactory.createAggregate(r,
        groupKey_.indicator, groupSet, groupSets, aggregateCalls);
    push(aggregate);
    return this;
  }

  private List<RexNode> projects(RelDataType inputRowType) {
    final List<RexNode> exprList = new ArrayList<>();
    for (RelDataTypeField field : inputRowType.getFieldList()) {
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      exprList.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
    }
    return exprList;
  }

  private static int registerExpression(List<RexNode> exprList, RexNode node) {
    int i = exprList.indexOf(node);
    if (i < 0) {
      i = exprList.size();
      exprList.add(node);
    }
    return i;
  }

  private static List<Integer> registerExpressions(List<RexNode> extraNodes,
      Iterable<? extends RexNode> nodes) {
    final List<Integer> builder = new ArrayList<>();
    for (RexNode node : nodes) {
      builder.add(registerExpression(extraNodes, node));
    }
    return builder;
  }

  private RelBuilder setOp(boolean all, SqlKind kind, int n) {
    List<RelNode> inputs = new LinkedList<>();
    for (int i = 0; i < n; i++) {
      inputs.add(0, build());
    }
    switch (kind) {
    case UNION:
    case INTERSECT:
      if (n < 1) {
        throw new IllegalArgumentException("bad INTERSECT/UNION input count");
      }
      break;
    case EXCEPT:
      if (n != 2) {
        throw new AssertionError("bad EXCEPT input count");
      }
      break;
    default:
      throw new AssertionError("bad setOp " + kind);
    }
    switch (n) {
    case 1:
      return push(inputs.get(0));
    default:
      return push(setOpFactory.createSetOp(kind, inputs, all));
    }
  }

  /** Creates a {@link org.apache.calcite.rel.core.Union} of the two most recent
   * relational expressions on the stack.
   *
   * @param all Whether to create UNION ALL
   */
  public RelBuilder union(boolean all) {
    return union(all, 2);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Union} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create UNION ALL
   * @param n Number of inputs to the UNION operator
   */
  public RelBuilder union(boolean all, int n) {
    return setOp(all, SqlKind.UNION, n);
  }

  /** Creates an {@link org.apache.calcite.rel.core.Intersect} of the two most
   * recent relational expressions on the stack.
   *
   * @param all Whether to create INTERSECT ALL
   */
  public RelBuilder intersect(boolean all) {
    return intersect(all, 2);
  }

  /** Creates an {@link org.apache.calcite.rel.core.Intersect} of the {@code n}
   * most recent relational expressions on the stack.
   *
   * @param all Whether to create INTERSECT ALL
   * @param n Number of inputs to the INTERSECT operator
   */
  public RelBuilder intersect(boolean all, int n) {
    return setOp(all, SqlKind.INTERSECT, n);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Minus} of the two most recent
   * relational expressions on the stack.
   *
   * @param all Whether to create EXCEPT ALL
   */
  public RelBuilder minus(boolean all) {
    return setOp(all, SqlKind.EXCEPT, 2);
  }

  /** Creates a {@link org.apache.calcite.rel.core.Join}. */
  public RelBuilder join(JoinRelType joinType, RexNode condition0,
      RexNode... conditions) {
    return join(joinType, Lists.asList(condition0, conditions));
  }

  /** Creates a {@link org.apache.calcite.rel.core.Join} with multiple
   * conditions. */
  public RelBuilder join(JoinRelType joinType,
      Iterable<? extends RexNode> conditions) {
    final Frame right = Stacks.pop(stack);
    final Frame left = Stacks.pop(stack);
    final RelNode join = joinFactory.createJoin(left.rel, right.rel,
        RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, false),
        joinType, ImmutableSet.<String>of(), false);
    final List<Pair<String, RelDataType>> pairs = new ArrayList<>();
    pairs.addAll(left.right);
    pairs.addAll(right.right);
    Stacks.push(stack, new Frame(join, ImmutableList.copyOf(pairs)));
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.Join} using USING syntax.
   *
   * <p>For each of the field names, both left and right inputs must have a
   * field of that name. Constructs a join condition that the left and right
   * fields are equal.
   *
   * @param joinType Join type
   * @param fieldNames Field names
   */
  public RelBuilder join(JoinRelType joinType, String... fieldNames) {
    final List<RexNode> conditions = new ArrayList<>();
    for (String fieldName : fieldNames) {
      conditions.add(
          call(SqlStdOperatorTable.EQUALS,
              field(2, 0, fieldName),
              field(2, 1, fieldName)));
    }
    return join(joinType, conditions);
  }

  /** Creates a {@link org.apache.calcite.rel.core.SemiJoin}. */
  public RelBuilder semiJoin(Iterable<? extends RexNode> conditions) {
    final Frame right = Stacks.pop(stack);
    final Frame left = Stacks.pop(stack);
    final RelNode semiJoin =
        semiJoinFactory.createSemiJoin(left.rel, right.rel,
            RexUtil.composeConjunction(cluster.getRexBuilder(), conditions,
                false));
    Stacks.push(stack, new Frame(semiJoin, left.right));
    return this;
  }

  /** Creates a {@link org.apache.calcite.rel.core.SemiJoin}. */
  public RelBuilder semiJoin(RexNode... conditions) {
    return semiJoin(ImmutableList.copyOf(conditions));
  }

  /** Assigns a table alias to the top entry on the stack. */
  public RelBuilder as(String alias) {
    final Frame pair = Stacks.pop(stack);
    Stacks.push(stack,
        new Frame(pair.rel,
            ImmutableList.of(Pair.of(alias, pair.rel.getRowType()))));
    return this;
  }

  /** Creates a {@link Values}.
   *
   * <p>The {@code values} array must have the same number of entries as
   * {@code fieldNames}, or an integer multiple if you wish to create multiple
   * rows.
   *
   * <p>If there are zero rows, or if all values of a any column are
   * null, this method cannot deduce the type of columns. For these cases,
   * call {@link #values(Iterable, RelDataType)}.
   *
   * @param fieldNames Field names
   * @param values Values
   */
  public RelBuilder values(String[] fieldNames, Object... values) {
    if (fieldNames == null
        || fieldNames.length == 0
        || values.length % fieldNames.length != 0
        || values.length < fieldNames.length) {
      throw new IllegalArgumentException(
          "Value count must be a positive multiple of field count");
    }
    final int rowCount = values.length / fieldNames.length;
    for (Ord<String> fieldName : Ord.zip(fieldNames)) {
      if (allNull(values, fieldName.i, fieldNames.length)) {
        throw new IllegalArgumentException("All values of field '" + fieldName.e
            + "' are null; cannot deduce type");
      }
    }
    final ImmutableList<ImmutableList<RexLiteral>> tupleList =
        tupleList(fieldNames.length, values);
    final RelDataTypeFactory.FieldInfoBuilder rowTypeBuilder =
        cluster.getTypeFactory().builder();
    for (final Ord<String> fieldName : Ord.zip(fieldNames)) {
      final String name =
          fieldName.e != null ? fieldName.e : "expr$" + fieldName.i;
      final RelDataType type = cluster.getTypeFactory().leastRestrictive(
          new AbstractList<RelDataType>() {
            public RelDataType get(int index) {
              return tupleList.get(index).get(fieldName.i).getType();
            }

            public int size() {
              return rowCount;
            }
          });
      rowTypeBuilder.add(name, type);
    }
    final RelDataType rowType = rowTypeBuilder.build();
    return values(tupleList, rowType);
  }

  private ImmutableList<ImmutableList<RexLiteral>> tupleList(int columnCount,
      Object[] values) {
    final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder =
        ImmutableList.builder();
    final List<RexLiteral> valueList = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      valueList.add((RexLiteral) literal(value));
      if ((i + 1) % columnCount == 0) {
        listBuilder.add(ImmutableList.copyOf(valueList));
        valueList.clear();
      }
    }
    return listBuilder.build();
  }

  /** Returns whether all values for a given column are null. */
  private boolean allNull(Object[] values, int column, int columnCount) {
    for (int i = column; i < values.length; i += columnCount) {
      if (values[i] != null) {
        return false;
      }
    }
    return true;
  }

  /** Creates a {@link Values} with a specified row type.
   *
   * <p>This method can handle cases that {@link #values(String[], Object...)}
   * cannot, such as all values of a column being null, or there being zero
   * rows.
   *
   * @param rowType Row type
   * @param columnValues Values
   */
  public RelBuilder values(RelDataType rowType, Object... columnValues) {
    final ImmutableList<ImmutableList<RexLiteral>> tupleList =
        tupleList(rowType.getFieldCount(), columnValues);
    RelNode values = valuesFactory.createValues(cluster, rowType,
        ImmutableList.copyOf(tupleList));
    push(values);
    return this;
  }

  /** Creates a {@link Values} with a specified row type.
   *
   * <p>This method can handle cases that {@link #values(String[], Object...)}
   * cannot, such as all values of a column being null, or there being zero
   * rows.
   *
   * @param tupleList Tuple list
   * @param rowType Row type
   */
  public RelBuilder values(Iterable<? extends List<RexLiteral>> tupleList,
      RelDataType rowType) {
    RelNode values =
        valuesFactory.createValues(cluster, rowType, copy(tupleList));
    push(values);
    return this;
  }

  /** Creates a {@link Values} with a specified row type and
   * zero rows.
   *
   * @param rowType Row type
   */
  public RelBuilder values(RelDataType rowType) {
    return values(ImmutableList.<ImmutableList<RexLiteral>>of(), rowType);
  }

  /** Converts an iterable of lists into an immutable list of immutable lists
   * with the same contents. Returns the same object if possible. */
  private static <E> ImmutableList<ImmutableList<E>>
  copy(Iterable<? extends List<E>> tupleList) {
    final ImmutableList.Builder<ImmutableList<E>> builder =
        ImmutableList.builder();
    int changeCount = 0;
    for (List<E> literals : tupleList) {
      final ImmutableList<E> literals2 =
          ImmutableList.copyOf(literals);
      builder.add(literals2);
      if (literals != literals2) {
        ++changeCount;
      }
    }
    if (changeCount == 0) {
      // don't make a copy if we don't have to
      //noinspection unchecked
      return (ImmutableList<ImmutableList<E>>) tupleList;
    }
    return builder.build();
  }

  /** Creates a limit without a sort. */
  public RelBuilder limit(int offset, int fetch) {
    return sortLimit(offset, fetch, ImmutableList.<RexNode>of());
  }

  /** Creates a {@link Sort} by field ordinals.
   *
   * <p>Negative fields mean descending: -1 means field(0) descending,
   * -2 means field(1) descending, etc.
   */
  public RelBuilder sort(int... fields) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (int field : fields) {
      builder.add(field < 0 ? desc(field(-field - 1)) : field(field));
    }
    return sortLimit(-1, -1, builder.build());
  }

  /** Creates a {@link Sort} by expressions. */
  public RelBuilder sort(RexNode... nodes) {
    return sortLimit(-1, -1, ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Sort} by expressions. */
  public RelBuilder sort(Iterable<? extends RexNode> nodes) {
    return sortLimit(-1, -1, nodes);
  }

  /** Creates a {@link Sort} by expressions, with limit and offset. */
  public RelBuilder sortLimit(int offset, int fetch, RexNode... nodes) {
    return sortLimit(offset, fetch, ImmutableList.copyOf(nodes));
  }

  /** Creates a {@link Sort} by a list of expressions, with limit and offset.
   *
   * @param offset Number of rows to skip; non-positive means don't skip any
   * @param fetch Maximum number of rows to fetch; negative means no limit
   * @param nodes Sort expressions
   */
  public RelBuilder sortLimit(int offset, int fetch,
      Iterable<? extends RexNode> nodes) {
    final List<RelFieldCollation> fieldCollations = new ArrayList<>();
    final RelDataType inputRowType = peek().getRowType();
    final List<RexNode> extraNodes = projects(inputRowType);
    final List<RexNode> originalExtraNodes = ImmutableList.copyOf(extraNodes);
    for (RexNode node : nodes) {
      fieldCollations.add(
          collation(node, RelFieldCollation.Direction.ASCENDING,
              RelFieldCollation.Direction.ASCENDING.defaultNullDirection(),
              extraNodes));
    }
    final RexNode offsetNode = offset <= 0 ? null : literal(offset);
    final RexNode fetchNode = fetch < 0 ? null : literal(fetch);
    final boolean addedFields = extraNodes.size() > originalExtraNodes.size();
    if (fieldCollations.isEmpty()) {
      assert !addedFields;
      RelNode top = peek();
      if (top instanceof Sort) {
        final Sort sort2 = (Sort) top;
        if (sort2.offset == null && sort2.fetch == null) {
          Stacks.pop(stack);
          push(sort2.getInput());
          final RelNode sort =
              sortFactory.createSort(build(), sort2.collation,
                  offsetNode, fetchNode);
          push(sort);
          return this;
        }
      }
      if (top instanceof Project) {
        final Project project = (Project) top;
        if (project.getInput() instanceof Sort) {
          final Sort sort2 = (Sort) project.getInput();
          if (sort2.offset == null && sort2.fetch == null) {
            Stacks.pop(stack);
            push(sort2.getInput());
            final RelNode sort =
                sortFactory.createSort(build(), sort2.collation,
                    offsetNode, fetchNode);
            push(sort);
            project(project.getProjects());
            return this;
          }
        }
      }
    }
    if (addedFields) {
      project(extraNodes);
    }
    final RelNode sort =
        sortFactory.createSort(build(), RelCollations.of(fieldCollations),
            offsetNode, fetchNode);
    push(sort);
    if (addedFields) {
      project(originalExtraNodes);
    }
    return this;
  }

  private static RelFieldCollation collation(RexNode node,
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection, List<RexNode> extraNodes) {
    switch (node.getKind()) {
    case INPUT_REF:
      return new RelFieldCollation(((RexInputRef) node).getIndex(),
          direction, nullDirection);
    case DESCENDING:
      return collation(((RexCall) node).getOperands().get(0),
          RelFieldCollation.Direction.DESCENDING,
          nullDirection, extraNodes);
    case NULLS_FIRST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.FIRST, extraNodes);
    case NULLS_LAST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.LAST, extraNodes);
    default:
      final int fieldIndex = extraNodes.size();
      extraNodes.add(node);
      return new RelFieldCollation(fieldIndex, direction, nullDirection);
    }
  }

  /**
   * Creates a projection that converts the current relational expression's
   * output to a desired row type.
   *
   * @param castRowType row type after cast
   * @param rename      if true, use field names from castRowType; if false,
   *                    preserve field names from rel
   */
  public RelBuilder convert(RelDataType castRowType, boolean rename) {
    final RelNode r = build();
    final RelNode r2 =
        RelOptUtil.createCastRel(r, castRowType, rename, projectFactory);
    push(r2);
    return this;
  }

  public RelBuilder permute(Mapping mapping) {
    assert mapping.getMappingType().isSingleSource();
    assert mapping.getMappingType().isMandatorySource();
    if (mapping.isIdentity()) {
      return this;
    }
    final List<RexNode> exprList = Lists.newArrayList();
    for (int i = 0; i < mapping.getTargetCount(); i++) {
      exprList.add(field(mapping.getSource(i)));
    }
    return project(exprList);
  }

  public RelBuilder aggregate(GroupKey groupKey,
      List<AggregateCall> aggregateCalls) {
    return aggregate(groupKey,
        Lists.transform(
            aggregateCalls, new Function<AggregateCall, AggCall>() {
              public AggCall apply(AggregateCall input) {
                return new AggCallImpl2(input);
              }
            }));
  }

  /** Clears the stack.
   *
   * <p>The builder's state is now the same as when it was created. */
  public void clear() {
    stack.clear();
  }

  protected String getAlias() {
    final Frame frame = Stacks.peek(stack);
    return frame.right.size() == 1
        ? frame.right.get(0).left
        : null;
  }

  /** Information necessary to create a call to an aggregate function.
   *
   * @see RelBuilder#aggregateCall */
  public interface AggCall {
  }

  /** Information necessary to create the GROUP BY clause of an Aggregate.
   *
   * @see RelBuilder#groupKey */
  public interface GroupKey {
    /** Assigns an alias to this group key.
     *
     * <p>Used to assign field names in the {@code group} operation. */
    GroupKey alias(String alias);
  }

  /** Implementation of {@link RelBuilder.GroupKey}. */
  protected static class GroupKeyImpl implements GroupKey {
    final ImmutableList<RexNode> nodes;
    final boolean indicator;
    final ImmutableList<ImmutableList<RexNode>> nodeLists;
    final String alias;

    GroupKeyImpl(ImmutableList<RexNode> nodes, boolean indicator,
        ImmutableList<ImmutableList<RexNode>> nodeLists, String alias) {
      this.nodes = Preconditions.checkNotNull(nodes);
      this.indicator = indicator;
      this.nodeLists = nodeLists;
      this.alias = alias;
    }

    @Override public String toString() {
      return alias == null ? nodes.toString() : nodes + " as " + alias;
    }

    public GroupKey alias(String alias) {
      return Objects.equals(this.alias, alias)
          ? this
          : new GroupKeyImpl(nodes, indicator, nodeLists, alias);
    }
  }

  /** Implementation of {@link RelBuilder.AggCall}. */
  private static class AggCallImpl implements AggCall {
    private final SqlAggFunction aggFunction;
    private final boolean distinct;
    private final RexNode filter;
    private final String alias;
    private final ImmutableList<RexNode> operands;

    AggCallImpl(SqlAggFunction aggFunction, boolean distinct, RexNode filter,
        String alias, ImmutableList<RexNode> operands) {
      this.aggFunction = aggFunction;
      this.distinct = distinct;
      this.filter = filter;
      this.alias = alias;
      this.operands = operands;
    }
  }

  /** Implementation of {@link RelBuilder.AggCall} that wraps an
   * {@link AggregateCall}. */
  private static class AggCallImpl2 implements AggCall {
    private final AggregateCall aggregateCall;

    AggCallImpl2(AggregateCall aggregateCall) {
      this.aggregateCall = Preconditions.checkNotNull(aggregateCall);
    }
  }

  /** Builder stack frame.
   *
   * <p>Describes a previously created relational expression and
   * information about how table aliases map into its row type. */
  private static class Frame {
    final RelNode rel;
    final ImmutableList<Pair<String, RelDataType>> right;

    private Frame(RelNode rel, ImmutableList<Pair<String, RelDataType>> pairs) {
      this.rel = rel;
      this.right = pairs;
    }

    private Frame(RelNode rel) {
      this(rel, ImmutableList.of(Pair.of(deriveAlias(rel), rel.getRowType())));
    }

    private static String deriveAlias(RelNode rel) {
      if (rel instanceof TableScan) {
        final List<String> names = rel.getTable().getQualifiedName();
        if (!names.isEmpty()) {
          return Util.last(names);
        }
      }
      return null;
    }
  }
}

// End RelBuilder.java
