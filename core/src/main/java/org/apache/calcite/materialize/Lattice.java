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
package org.apache.calcite.materialize;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.TopologicalOrderIterator;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Structure that allows materialized views based upon a star schema to be
 * recognized and recommended.
 */
public class Lattice {
  private static final Function<Column, String> GET_ALIAS =
      new Function<Column, String>() {
        public String apply(Column input) {
          return input.alias;
        }
      };

  private static final Function<Column, Integer> GET_ORDINAL =
      new Function<Column, Integer>() {
        public Integer apply(Column input) {
          return input.ordinal;
        }
      };

  public final CalciteSchema rootSchema;
  public final LatticeRootNode rootNode;
  public final ImmutableList<Column> columns;
  public final boolean auto;
  public final boolean algorithm;
  public final long algorithmMaxMillis;
  public final double rowCountEstimate;
  public final ImmutableList<Measure> defaultMeasures;
  public final ImmutableList<Tile> tiles;
  public final LatticeStatisticProvider statisticProvider;

  private final Function<Integer, Column> toColumnFunction =
      new Function<Integer, Column>() {
        public Column apply(Integer input) {
          return columns.get(input);
        }
      };
  private final Function<AggregateCall, Measure> toMeasureFunction =
      new Function<AggregateCall, Measure>() {
        public Measure apply(AggregateCall input) {
          return new Measure(input.getAggregation(), input.isDistinct(),
              Lists.transform(input.getArgList(), toColumnFunction));
        }
      };

  private Lattice(CalciteSchema rootSchema, LatticeRootNode rootNode,
      boolean auto, boolean algorithm, long algorithmMaxMillis,
      LatticeStatisticProvider statisticProvider, Double rowCountEstimate,
      ImmutableList<Column> columns,
      ImmutableSortedSet<Measure> defaultMeasures, ImmutableList<Tile> tiles) {
    this.rootSchema = rootSchema;
    this.rootNode = Preconditions.checkNotNull(rootNode);
    this.columns = Preconditions.checkNotNull(columns);
    this.auto = auto;
    this.algorithm = algorithm;
    this.algorithmMaxMillis = algorithmMaxMillis;
    this.statisticProvider = Preconditions.checkNotNull(statisticProvider);
    this.defaultMeasures = defaultMeasures.asList(); // unique and sorted
    this.tiles = Preconditions.checkNotNull(tiles);

    assert isValid(Litmus.THROW);

    if (rowCountEstimate == null) {
      // We could improve this when we fix
      // [CALCITE-429] Add statistics SPI for lattice optimization algorithm
      rowCountEstimate = 1000d;
    }
    Preconditions.checkArgument(rowCountEstimate > 0d);
    this.rowCountEstimate = rowCountEstimate;
  }

  /** Creates a Lattice. */
  public static Lattice create(CalciteSchema schema, String sql, boolean auto) {
    return builder(schema, sql).auto(auto).build();
  }

  private boolean isValid(Litmus litmus) {
    if (!rootNode.isValid(litmus)) {
      return false;
    }
    for (Measure measure : defaultMeasures) {
      for (Column arg : measure.args) {
        if (columns.get(arg.ordinal) != arg) {
          return litmus.fail("measure argument must be a column registered in"
              + " this lattice: {}", measure);
        }
      }
    }
    return litmus.succeed();
  }

  private static void populateAliases(SqlNode from, List<String> aliases,
      String current) {
    if (from instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) from;
      populateAliases(join.getLeft(), aliases, null);
      populateAliases(join.getRight(), aliases, null);
    } else if (from.getKind() == SqlKind.AS) {
      populateAliases(SqlUtil.stripAs(from), aliases,
          SqlValidatorUtil.getAlias(from, -1));
    } else {
      if (current == null) {
        current = SqlValidatorUtil.getAlias(from, -1);
      }
      aliases.add(current);
    }
  }

  private static boolean populate(List<RelNode> nodes, List<int[][]> tempLinks,
      RelNode rel) {
    if (nodes.isEmpty() && rel instanceof LogicalProject) {
      return populate(nodes, tempLinks, ((LogicalProject) rel).getInput());
    }
    if (rel instanceof TableScan) {
      nodes.add(rel);
      return true;
    }
    if (rel instanceof LogicalJoin) {
      LogicalJoin join = (LogicalJoin) rel;
      if (join.getJoinType() != JoinRelType.INNER) {
        throw new RuntimeException("only inner join allowed, but got "
            + join.getJoinType());
      }
      populate(nodes, tempLinks, join.getLeft());
      populate(nodes, tempLinks, join.getRight());
      for (RexNode rex : RelOptUtil.conjunctions(join.getCondition())) {
        tempLinks.add(grab(nodes, rex));
      }
      return true;
    }
    throw new RuntimeException("Invalid node type "
        + rel.getClass().getSimpleName() + " in lattice query");
  }

  /** Converts an "t1.c1 = t2.c2" expression into two (input, field) pairs. */
  private static int[][] grab(List<RelNode> leaves, RexNode rex) {
    switch (rex.getKind()) {
    case EQUALS:
      break;
    default:
      throw new AssertionError("only equi-join allowed");
    }
    final List<RexNode> operands = ((RexCall) rex).getOperands();
    return new int[][] {
        inputField(leaves, operands.get(0)),
        inputField(leaves, operands.get(1))};
  }

  /** Converts an expression into an (input, field) pair. */
  private static int[] inputField(List<RelNode> leaves, RexNode rex) {
    if (!(rex instanceof RexInputRef)) {
      throw new RuntimeException("only equi-join of columns allowed: " + rex);
    }
    RexInputRef ref = (RexInputRef) rex;
    int start = 0;
    for (int i = 0; i < leaves.size(); i++) {
      final RelNode leaf = leaves.get(i);
      final int end = start + leaf.getRowType().getFieldCount();
      if (ref.getIndex() < end) {
        return new int[] {i, ref.getIndex() - start};
      }
      start = end;
    }
    throw new AssertionError("input not found");
  }

  @Override public String toString() {
    return rootNode + ":" + defaultMeasures;
  }

  public String sql(ImmutableBitSet groupSet, List<Measure> aggCallList) {
    final ImmutableBitSet.Builder columnSetBuilder = groupSet.rebuild();
    for (Measure call : aggCallList) {
      for (Column arg : call.args) {
        columnSetBuilder.set(arg.ordinal);
      }
    }
    final ImmutableBitSet columnSet = columnSetBuilder.build();

    // Figure out which nodes are needed. Use a node if its columns are used
    // or if has a child whose columns are used.
    List<LatticeNode> usedNodes = Lists.newArrayList();
    for (LatticeNode node : rootNode.descendants) {
      if (ImmutableBitSet.range(node.startCol, node.endCol)
          .intersects(columnSet)) {
        node.use(usedNodes);
      }
    }
    if (usedNodes.isEmpty()) {
      usedNodes.add(rootNode);
    }
    final SqlDialect dialect = SqlDialect.DatabaseProduct.CALCITE.getDialect();
    final StringBuilder buf = new StringBuilder("SELECT ");
    final StringBuilder groupBuf = new StringBuilder("\nGROUP BY ");
    int k = 0;
    final Set<String> columnNames = Sets.newHashSet();
    for (int i : groupSet) {
      if (k++ > 0) {
        buf.append(", ");
        groupBuf.append(", ");
      }
      final Column column = columns.get(i);
      dialect.quoteIdentifier(buf, column.identifiers());
      dialect.quoteIdentifier(groupBuf, column.identifiers());
      columnNames.add(column.column);
      if (!column.alias.equals(column.column)) {
        buf.append(" AS ");
        dialect.quoteIdentifier(buf, column.alias);
      }
    }
    if (groupSet.isEmpty()) {
      groupBuf.append("()");
    }
    int m = 0;
    for (Measure measure : aggCallList) {
      if (k++ > 0) {
        buf.append(", ");
      }
      buf.append(measure.agg.getName())
          .append("(");
      if (measure.args.isEmpty()) {
        buf.append("*");
      } else {
        int z = 0;
        for (Column arg : measure.args) {
          if (z++ > 0) {
            buf.append(", ");
          }
          dialect.quoteIdentifier(buf, arg.identifiers());
        }
      }
      buf.append(") AS ");
      String measureName;
      while (!columnNames.add(measureName = "m" + m)) {
        ++m;
      }
      dialect.quoteIdentifier(buf, measureName);
    }
    buf.append("\nFROM ");
    for (LatticeNode node : usedNodes) {
      if (node instanceof LatticeChildNode) {
        buf.append("\nJOIN ");
      }
      dialect.quoteIdentifier(buf, node.table.t.getQualifiedName());
      buf.append(" AS ");
      dialect.quoteIdentifier(buf, node.alias);
      if (node instanceof LatticeChildNode) {
        final LatticeChildNode node1 = (LatticeChildNode) node;
        buf.append(" ON ");
        k = 0;
        for (IntPair pair : node1.link) {
          if (k++ > 0) {
            buf.append(" AND ");
          }
          final Column left = columns.get(node1.parent.startCol + pair.source);
          dialect.quoteIdentifier(buf, left.identifiers());
          buf.append(" = ");
          final Column right = columns.get(node.startCol + pair.target);
          dialect.quoteIdentifier(buf, right.identifiers());
        }
      }
    }
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println("Lattice SQL:\n"
          + buf);
    }
    buf.append(groupBuf);
    return buf.toString();
  }

  /** Returns a SQL query that counts the number of distinct values of the
   * attributes given in {@code groupSet}. */
  public String countSql(ImmutableBitSet groupSet) {
    return "select count(*) as c from ("
        + sql(groupSet, ImmutableList.<Measure>of())
        + ")";
  }

  public StarTable createStarTable() {
    final List<Table> tables = Lists.newArrayList();
    for (LatticeNode node : rootNode.descendants) {
      tables.add(node.table.t.unwrap(Table.class));
    }
    return StarTable.of(this, tables);
  }

  public static Builder builder(CalciteSchema calciteSchema, String sql) {
    return builder(new LatticeSpace(), calciteSchema, sql);
  }

  static Builder builder(LatticeSpace space, CalciteSchema calciteSchema,
      String sql) {
    return new Builder(space, calciteSchema, sql);
  }

  public List<Measure> toMeasures(List<AggregateCall> aggCallList) {
    return Lists.transform(aggCallList, toMeasureFunction);
  }

  public Iterable<? extends Tile> computeTiles() {
    if (!algorithm) {
      return tiles;
    }
    return new TileSuggester(this).tiles();
  }

  /** Returns an estimate of the number of rows in the un-aggregated star. */
  public double getFactRowCount() {
    return rowCountEstimate;
  }

  /** Returns an estimate of the number of rows in the tile with the given
   * dimensions. */
  public double getRowCount(List<Column> columns) {
    // The expected number of distinct values when choosing p values
    // with replacement from n integers is n . (1 - ((n - 1) / n) ^ p).
    //
    // If we have several uniformly distributed attributes A1 ... Am
    // with N1 ... Nm distinct values, they behave as one uniformly
    // distributed attribute with N1 * ... * Nm distinct values.
    BigInteger n = BigInteger.ONE;
    for (Column column : columns) {
      final int cardinality = statisticProvider.cardinality(this, column);
      if (cardinality > 1) {
        n = n.multiply(BigInteger.valueOf(cardinality));
      }
    }
    final double nn = n.doubleValue();
    final double f = getFactRowCount();
    final double a = (nn - 1d) / nn;
    if (a == 1d) {
      // A under-flows if nn is large.
      return f;
    }
    final double v = nn * (1d - Math.pow(a, f));
    // Cap at fact-row-count, because numerical artifacts can cause it
    // to go a few % over.
    return Math.min(v, f);
  }

  public List<String> uniqueColumnNames() {
    return Lists.transform(columns, GET_ALIAS);
  }

  Pair<Path, Integer> columnToPathOffset(Column c) {
    for (Pair<LatticeNode, Path> p
        : Pair.zip(rootNode.descendants, rootNode.paths)) {
      if (p.left.alias.equals(c.table)) {
        return Pair.of(p.right, c.ordinal - p.left.startCol);
      }
    }
    throw new AssertionError("lattice column not found: " + c);
  }

  /** Edge in the temporary graph. */
  private static class Edge extends DefaultEdge {
    public static final DirectedGraph.EdgeFactory<Vertex, Edge> FACTORY =
        new DirectedGraph.EdgeFactory<Vertex, Edge>() {
          public Edge createEdge(Vertex source, Vertex target) {
            return new Edge(source, target);
          }
        };

    final List<IntPair> pairs = Lists.newArrayList();

    Edge(Vertex source, Vertex target) {
      super(source, target);
    }

    Vertex getTarget() {
      return (Vertex) target;
    }

    Vertex getSource() {
      return (Vertex) source;
    }
  }

  /** Vertex in the temporary graph. */
  private static class Vertex {
    final LatticeTable table;
    final String alias;

    private Vertex(LatticeTable table, String alias) {
      this.table = table;
      this.alias = alias;
    }
  }

  /** A measure within a {@link Lattice}.
   *
   * <p>It is immutable.
   *
   * <p>Examples: SUM(products.weight), COUNT() (means "COUNT(*")),
   * COUNT(DISTINCT customer.id).
   */
  public static class Measure implements Comparable<Measure> {
    public final SqlAggFunction agg;
    public final boolean distinct;
    public final ImmutableList<Column> args;
    public final String digest;

    public Measure(SqlAggFunction agg, boolean distinct,
        Iterable<Column> args) {
      this.agg = Preconditions.checkNotNull(agg);
      this.distinct = distinct;
      this.args = ImmutableList.copyOf(args);

      final StringBuilder b = new StringBuilder()
          .append(agg)
          .append(distinct ? "(DISTINCT " : "(");
      for (Ord<Column> arg : Ord.zip(this.args)) {
        if (arg.i > 0) {
          b.append(", ");
        }
        b.append(arg.e.table);
        b.append('.');
        b.append(arg.e.column);
      }
      b.append(')');
      this.digest = b.toString();
    }

    public int compareTo(@Nonnull Measure measure) {
      int c = compare(args, measure.args);
      if (c == 0) {
        c = agg.getName().compareTo(measure.agg.getName());
        if (c == 0) {
          c = Boolean.compare(distinct, measure.distinct);
        }
      }
      return c;
    }

    @Override public String toString() {
      return digest;
    }

    @Override public int hashCode() {
      return Objects.hash(agg, args);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Measure
          && this.agg.equals(((Measure) obj).agg)
          && this.args.equals(((Measure) obj).args)
          && this.distinct == ((Measure) obj).distinct;
    }

    /** Returns the set of distinct argument ordinals. */
    public ImmutableBitSet argBitSet() {
      final ImmutableBitSet.Builder bitSet = ImmutableBitSet.builder();
      for (Column arg : args) {
        bitSet.set(arg.ordinal);
      }
      return bitSet.build();
    }

    /** Returns a list of argument ordinals. */
    public List<Integer> argOrdinals() {
      return Lists.transform(args, GET_ORDINAL);
    }

    private static int compare(List<Column> list0, List<Column> list1) {
      final int size = Math.min(list0.size(), list1.size());
      for (int i = 0; i < size; i++) {
        final int o0 = list0.get(i).ordinal;
        final int o1 = list1.get(i).ordinal;
        final int c = Utilities.compare(o0, o1);
        if (c != 0) {
          return c;
        }
      }
      return Utilities.compare(list0.size(), list1.size());
    }

    /** Copies this measure, mapping its arguments using a given function. */
    Measure copy(Function<Column, Column> mapper) {
      return new Measure(agg, distinct, Lists.transform(args, mapper));
    }
  }

  /** Column in a lattice. Columns are identified by table alias and
   * column name, and may have an additional alias that is unique
   * within the entire lattice. */
  public static class Column implements Comparable<Column> {
    /** Ordinal of the column within the lattice. */
    public final int ordinal;
    /** Alias of the table reference that the column belongs to. */
    public final String table;
    /** Name of the column. Unique within the table reference, but not
     * necessarily within the lattice. */
    public final String column;
    /** Alias of the column, unique within the lattice. Derived from the column
     * name, automatically disambiguated if necessary. */
    public final String alias;

    private Column(int ordinal, String table, String column, String alias) {
      this.ordinal = ordinal;
      this.table = Preconditions.checkNotNull(table);
      this.column = Preconditions.checkNotNull(column);
      this.alias = Preconditions.checkNotNull(alias);
    }

    public int compareTo(Column column) {
      return Utilities.compare(ordinal, column.ordinal);
    }

    @Override public int hashCode() {
      return ordinal;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Column
          && this.ordinal == ((Column) obj).ordinal;
    }

    @Override public String toString() {
      return identifiers().toString();
    }

    public List<String> identifiers() {
      return ImmutableList.of(table, column);
    }
  }

  /** Lattice builder. */
  public static class Builder {
    private final LatticeRootNode rootNode;
    private final ImmutableList<Column> columns;
    private final ImmutableListMultimap<String, Column> columnsByAlias;
    private final SortedSet<Measure> defaultMeasureSet =
        new TreeSet<>();
    private final ImmutableList.Builder<Tile> tileListBuilder =
        ImmutableList.builder();
    private final CalciteSchema rootSchema;
    private boolean algorithm = false;
    private long algorithmMaxMillis = -1;
    private boolean auto = true;
    private Double rowCountEstimate;
    private String statisticProvider;

    public Builder(LatticeSpace space, CalciteSchema schema, String sql) {
      this.rootSchema = Preconditions.checkNotNull(schema.root());
      Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema");
      CalcitePrepare.ConvertResult parsed =
          Schemas.convert(MaterializedViewTable.MATERIALIZATION_CONNECTION,
              schema, schema.path(null), sql);

      // Walk the join tree.
      List<RelNode> relNodes = Lists.newArrayList();
      List<int[][]> tempLinks = Lists.newArrayList();
      populate(relNodes, tempLinks, parsed.root.rel);

      // Get aliases.
      List<String> aliases = Lists.newArrayList();
      populateAliases(((SqlSelect) parsed.sqlNode).getFrom(), aliases, null);

      // Build a graph.
      final DirectedGraph<Vertex, Edge> graph =
          DefaultDirectedGraph.create(Edge.FACTORY);
      final List<Vertex> vertices = new ArrayList<>();
      for (Pair<RelNode, String> p : Pair.zip(relNodes, aliases)) {
        final LatticeTable table = space.register(p.left.getTable());
        final Vertex vertex = new Vertex(table, p.right);
        graph.addVertex(vertex);
        vertices.add(vertex);
      }
      for (int[][] tempLink : tempLinks) {
        final Vertex source = vertices.get(tempLink[0][0]);
        final Vertex target = vertices.get(tempLink[1][0]);
        Edge edge = graph.getEdge(source, target);
        if (edge == null) {
          edge = graph.addEdge(source, target);
        }
        edge.pairs.add(IntPair.of(tempLink[0][1], tempLink[1][1]));
      }

      // Convert the graph into a tree of nodes, each connected to a parent and
      // with a join condition to that parent.
      MutableNode root = null;
      final Map<LatticeTable, MutableNode> map = Maps.newIdentityHashMap();
      for (Vertex vertex : TopologicalOrderIterator.of(graph)) {
        final List<Edge> edges = graph.getInwardEdges(vertex);
        MutableNode node;
        if (root == null) {
          if (!edges.isEmpty()) {
            throw new RuntimeException("root node must not have relationships: "
                + vertex);
          }
          root = node = new MutableNode(vertex.table);
          node.alias = vertex.alias;
        } else {
          if (edges.size() != 1) {
            throw new RuntimeException(
                "child node must have precisely one parent: " + vertex);
          }
          final Edge edge = edges.get(0);
          final MutableNode parent = map.get(edge.getSource().table);
          final Step step =
              new Step(edge.getSource().table,
                  edge.getTarget().table, edge.pairs);
          node = new MutableNode(vertex.table, parent, step);
          node.alias = vertex.alias;
        }
        map.put(vertex.table, node);
      }
      assert root != null;
      final Fixer fixer = new Fixer();
      fixer.fixUp(root);
      columns = fixer.columnList.build();
      columnsByAlias = fixer.columnAliasList.build();
      rootNode = new LatticeRootNode(space, root);
    }

    /** Creates a Builder based upon a mutable node. */
    Builder(LatticeSpace space, CalciteSchema schema,
        MutableNode mutableNode) {
      this.rootSchema = schema;

      assert mutableNode != null;
      final Fixer fixer = new Fixer();
      fixer.fixUp(mutableNode);

      final LatticeRootNode node0 = new LatticeRootNode(space, mutableNode);
      final LatticeRootNode node1 = space.nodeMap.get(node0.digest);
      final LatticeRootNode node;
      if (node1 != null) {
        node = node1;
      } else {
        node = node0;
        space.nodeMap.put(node0.digest, node0);
      }

      this.rootNode = node;
      columns = fixer.columnList.build();
      columnsByAlias = fixer.columnAliasList.build();
    }

    /** Sets the "auto" attribute (default true). */
    public Builder auto(boolean auto) {
      this.auto = auto;
      return this;
    }

    /** Sets the "algorithm" attribute (default false). */
    public Builder algorithm(boolean algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    /** Sets the "algorithmMaxMillis" attribute (default -1). */
    public Builder algorithmMaxMillis(long algorithmMaxMillis) {
      this.algorithmMaxMillis = algorithmMaxMillis;
      return this;
    }

    /** Sets the "rowCountEstimate" attribute (default null). */
    public Builder rowCountEstimate(double rowCountEstimate) {
      this.rowCountEstimate = rowCountEstimate;
      return this;
    }

    /** Sets the "statisticProvider" attribute.
     *
     * <p>If not set, the lattice will use {@link Lattices#CACHED_SQL}. */
    public Builder statisticProvider(String statisticProvider) {
      this.statisticProvider = statisticProvider;
      return this;
    }

    /** Builds a lattice. */
    public Lattice build() {
      LatticeStatisticProvider statisticProvider =
          this.statisticProvider != null
              ? AvaticaUtils.instantiatePlugin(LatticeStatisticProvider.class,
                  this.statisticProvider)
              : Lattices.CACHED_SQL;
      Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema");
      return new Lattice(rootSchema, rootNode, auto,
          algorithm, algorithmMaxMillis, statisticProvider, rowCountEstimate,
          columns, ImmutableSortedSet.copyOf(defaultMeasureSet),
          tileListBuilder.build());
    }

    /** Resolves the arguments of a
     * {@link org.apache.calcite.model.JsonMeasure}. They must either be null,
     * a string, or a list of strings. Throws if the structure is invalid, or if
     * any of the columns do not exist in the lattice. */
    public ImmutableList<Column> resolveArgs(Object args) {
      if (args == null) {
        return ImmutableList.of();
      } else if (args instanceof String) {
        return ImmutableList.of(resolveColumnByAlias((String) args));
      } else if (args instanceof List) {
        final ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (Object o : (List) args) {
          if (o instanceof String) {
            builder.add(resolveColumnByAlias((String) o));
          } else {
            throw new RuntimeException(
                "Measure arguments must be a string or a list of strings; argument: "
                    + o);
          }
        }
        return builder.build();
      } else {
        throw new RuntimeException(
            "Measure arguments must be a string or a list of strings");
      }
    }

    /** Looks up a column in this lattice by alias. The alias must be unique
     * within the lattice.
     */
    private Column resolveColumnByAlias(String name) {
      final ImmutableList<Column> list = columnsByAlias.get(name);
      if (list == null || list.size() == 0) {
        throw new RuntimeException("Unknown lattice column '" + name + "'");
      } else if (list.size() == 1) {
        return list.get(0);
      } else {
        throw new RuntimeException("Lattice column alias '" + name
            + "' is not unique");
      }
    }

    public Column resolveColumn(Object name) {
      if (name instanceof String) {
        return resolveColumnByAlias((String) name);
      }
      if (name instanceof List) {
        List list = (List) name;
        switch (list.size()) {
        case 1:
          final Object alias = list.get(0);
          if (alias instanceof String) {
            return resolveColumnByAlias((String) alias);
          }
          break;
        case 2:
          final Object table = list.get(0);
          final Object column = list.get(1);
          if (table instanceof String && column instanceof String) {
            return resolveQualifiedColumn((String) table, (String) column);
          }
          break;
        }
      }
      throw new RuntimeException(
          "Lattice column reference must be a string or a list of 1 or 2 strings; column: "
              + name);
    }

    private Column resolveQualifiedColumn(String table, String column) {
      for (Column column1 : columns) {
        if (column1.table.equals(table)
            && column1.column.equals(column)) {
          return column1;
        }
      }
      throw new RuntimeException("Unknown lattice column [" + table + ", "
          + column + "]");
    }

    public Measure resolveMeasure(String aggName, boolean distinct,
        Object args) {
      final SqlAggFunction agg = resolveAgg(aggName);
      final ImmutableList<Column> list = resolveArgs(args);
      return new Measure(agg, distinct, list);
    }

    private SqlAggFunction resolveAgg(String aggName) {
      if (aggName.equalsIgnoreCase("count")) {
        return SqlStdOperatorTable.COUNT;
      } else if (aggName.equalsIgnoreCase("sum")) {
        return SqlStdOperatorTable.SUM;
      } else {
        throw new RuntimeException("Unknown lattice aggregate function "
            + aggName);
      }
    }

    /** Adds a measure, if it does not already exist.
     * Returns false if an identical measure already exists. */
    public boolean addMeasure(Measure measure) {
      return defaultMeasureSet.add(measure);
    }

    public void addTile(Tile tile) {
      tileListBuilder.add(tile);
    }

    public Column column(int table, int column) {
      int i = 0;
      for (LatticeNode descendant : rootNode.descendants) {
        if (table-- == 0) {
          break;
        }
        i += descendant.table.t.getRowType().getFieldCount();
      }
      return columns.get(i + column);
    }

    Column pathOffsetToColumn(Path path, int offset) {
      final int i = rootNode.paths.indexOf(path);
      final LatticeNode node = rootNode.descendants.get(i);
      final int c = node.startCol + offset;
      if (c >= node.endCol) {
        throw new AssertionError();
      }
      return columns.get(c);
    }

    /** Work space for fixing up a tree of mutable nodes. */
    private static class Fixer {
      final Set<String> aliases = new HashSet<>();
      final Set<String> columnAliases = new HashSet<>();
      final Set<MutableNode> seen = new HashSet<>();
      final ImmutableList.Builder<Column> columnList = ImmutableList.builder();
      final ImmutableListMultimap.Builder<String, Column> columnAliasList =
          ImmutableListMultimap.builder();
      int c;

      void fixUp(MutableNode node) {
        if (!seen.add(node)) {
          throw new IllegalArgumentException("cyclic query graph");
        }
        if (node.alias == null) {
          node.alias = Util.last(node.table.t.getQualifiedName());
        }
        node.alias = SqlValidatorUtil.uniquify(node.alias, aliases,
            SqlValidatorUtil.ATTEMPT_SUGGESTER);
        node.startCol = c;
        for (String name : node.table.t.getRowType().getFieldNames()) {
          final String alias = SqlValidatorUtil.uniquify(name,
              columnAliases, SqlValidatorUtil.ATTEMPT_SUGGESTER);
          final Column column = new Column(c++, node.alias, name, alias);
          columnList.add(column);
          columnAliasList.put(name, column); // name before it is made unique
        }
        node.endCol = c;

        assert MutableNode.ORDERING.isStrictlyOrdered(node.children)
            : node.children;
        for (MutableNode child : node.children) {
          fixUp(child);
        }
      }
    }
  }

  /** Materialized aggregate within a lattice. */
  public static class Tile {
    public final ImmutableList<Measure> measures;
    public final ImmutableList<Column> dimensions;
    public final ImmutableBitSet bitSet;

    public Tile(ImmutableList<Measure> measures,
        ImmutableList<Column> dimensions) {
      this.measures = measures;
      this.dimensions = dimensions;
      assert Ordering.natural().isStrictlyOrdered(dimensions);
      assert Ordering.natural().isStrictlyOrdered(measures);
      final ImmutableBitSet.Builder bitSetBuilder = ImmutableBitSet.builder();
      for (Column dimension : dimensions) {
        bitSetBuilder.set(dimension.ordinal);
      }
      bitSet = bitSetBuilder.build();
    }

    public static TileBuilder builder() {
      return new TileBuilder();
    }

    public ImmutableBitSet bitSet() {
      return bitSet;
    }
  }

  /** Tile builder. */
  public static class TileBuilder {
    private final List<Measure> measureBuilder = Lists.newArrayList();
    private final List<Column> dimensionListBuilder = Lists.newArrayList();

    public Tile build() {
      return new Tile(
          Ordering.natural().immutableSortedCopy(measureBuilder),
          Ordering.natural().immutableSortedCopy(dimensionListBuilder));
    }

    public void addMeasure(Measure measure) {
      measureBuilder.add(measure);
    }

    public void addDimension(Column column) {
      dimensionListBuilder.add(column);
    }
  }
}

// End Lattice.java
