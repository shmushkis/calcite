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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.PartiallyOrderedSet;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Implementation of {@link Profiler} that only investigates "interesting"
 * combinations of columns.
 */
public class ProfilerImpl implements Profiler {
  private static final Function<List<Comparable>, Comparable> ONLY =
      new Function<List<Comparable>, Comparable>() {
        public Comparable apply(List<Comparable> input) {
          return Iterables.getOnlyElement(input);
        }
      };

  /** The number of combinations to consider per pass.
   * The number is determined by memory, but a value of 1,000 is typical.
   * You need 2KB memory per sketch, and one sketch for each combination. */
  private final int combinationsPerPass;

  /** Whether a successor is considered interesting enough to analyze. */
  private final Predicate<Pair<Space, Column>> predicate;

  /**
   * Creates a {@code ProfilerImpl}.
   *
   * @param combinationsPerPass Maximum number of columns (or combinations of
   *   columns) to compute each pass
   * @param predicate Whether a successor is considered interesting enough to
   *   analyze
   */
  public ProfilerImpl(int combinationsPerPass,
      Predicate<Pair<Space, Column>> predicate) {
    Preconditions.checkArgument(combinationsPerPass > 2);
    this.combinationsPerPass = combinationsPerPass;
    this.predicate = predicate;
  }

  public List<Statistic> profile(Iterable<List<Comparable>> rows,
      final List<Column> columns) {
    return new Run(columns).profile(rows);
  }

  /** A run of the profiler. */
  class Run {
    private final List<Column> columns;
    final Map<ImmutableBitSet, Distribution> distributions = new HashMap<>();
    /** List of spaces that have one column. */
    final List<Space> singletonSpaces;
    /** Combinations of columns that we have computed but whose successors have
     * not yet been computed. We may add some of those successors to
     * {@link #spaceQueue}. */
    final Deque<Space> doneQueue = new ArrayDeque<>();
    /** Combinations of columns that we will compute next pass. */
    final Deque<ImmutableBitSet> spaceQueue = new ArrayDeque<>();
    final List<Statistic> statistics = new ArrayList<>();
    final PartiallyOrderedSet.Ordering<Space> ordering =
        new PartiallyOrderedSet.Ordering<Space>() {
          public boolean lessThan(Space e1, Space e2) {
            return e2.columnOrdinals.contains(e1.columnOrdinals);
          }
        };
    final PartiallyOrderedSet<Space> results =
        new PartiallyOrderedSet<>(ordering);
    final PartiallyOrderedSet<Space> keyResults =
        new PartiallyOrderedSet<>(ordering);
    private final List<ImmutableBitSet> keyOrdinalLists =
        new ArrayList<>();
    final Function<Integer, Column> get =
        new Function<Integer, Column>() {
          public Column apply(Integer input) {
            return columns.get(input);
          }
        };

    /**
     * Creates a Run.
     *
     * @param columns List of columns
     */
    Run(final List<Column> columns) {
      this.columns = ImmutableList.copyOf(columns);
      for (Ord<Column> column : Ord.zip(columns)) {
        if (column.e.ordinal != column.i) {
          throw new IllegalArgumentException();
        }
      }
      this.singletonSpaces =
          new ArrayList<>(Collections.nCopies(columns.size(), (Space) null));
      if (combinationsPerPass > Math.pow(2D, columns.size())) {
        // There are not many columns. We can compute all combinations in the
        // first pass.
        for (ImmutableBitSet ordinals
            : ImmutableBitSet.range(columns.size()).powerSet()) {
          spaceQueue.add(ordinals);
        }
      } else {
        // We will need to take multiple passes.
        // Pass 0, just put the empty combination on the queue.
        // Next pass, we will do its successors, the singleton combinations.
        spaceQueue.add(ImmutableBitSet.of());
      }
    }

    List<Statistic> profile(Iterable<List<Comparable>> rows) {
      int pass = 0;
      for (;;) {
        final List<Space> spaces = nextBatch();
        if (spaces.isEmpty()) {
          break;
        }
        pass(pass++, spaces, rows);
      }

      for (Space s : singletonSpaces) {
        for (ImmutableBitSet dependent : s.dependents) {
          statistics.add(
              new FunctionalDependency(toColumns(dependent),
                  Iterables.getOnlyElement(s.columns)));
        }
      }
      return statistics;
    }

    /** Populates {@code spaces} with the next batch.
     * Returns an empty list if done. */
    List<Space> nextBatch() {
      final List<Space> spaces = new ArrayList<>();
    loop:
      for (;;) {
        if (spaces.size() >= combinationsPerPass) {
          // We have enough for the next pass.
          return spaces;
        }
        // First, see if there is a space we did have room for last pass.
        final ImmutableBitSet ordinals = spaceQueue.poll();
        if (ordinals != null) {
          final Space space = new Space(this, ordinals, toColumns(ordinals));
          spaces.add(space);
          doneQueue.add(space);
          if (ordinals.cardinality() == 1) {
            singletonSpaces.set(ordinals.nth(0), space);
          }
        } else {
          // Next, take a space that was done last time, generate its
          // successors, and add the interesting ones to the space queue.
          for (;;) {
            final Space doneSpace = doneQueue.poll();
            if (doneSpace == null) {
              // There are no more done spaces. We're done.
              return spaces;
            }
            for (Column column : columns) {
              if (!doneSpace.columnOrdinals.get(column.ordinal)) {
                if (isInterestingSuccessor(doneSpace, column)) {
                  final ImmutableBitSet nextOrdinals =
                      doneSpace.columnOrdinals.set(column.ordinal);
                  spaceQueue.add(nextOrdinals);
                }
              }
            }
            // We've converted at a space into at least one interesting
            // successor.
            if (!spaceQueue.isEmpty()) {
              continue loop;
            }
          }
        }
      }
    }

    boolean isInterestingSuccessor(Space space, Column column) {
      return space.columnOrdinals.cardinality() == 0
          || !containsKey(space.columnOrdinals.set(column.ordinal))
          && predicate.apply(Pair.of(space, column));
    }

    private boolean containsKey(ImmutableBitSet ordinals) {
      for (ImmutableBitSet keyOrdinals : keyOrdinalLists) {
        if (ordinals.contains(keyOrdinals)) {
          return true;
        }
      }
      return false;
    }

    void pass(int pass, List<Space> spaces, Iterable<List<Comparable>> rows) {
      System.out.println("pass: " + pass
          + ", spaces.size: " + spaces.size()
          + ", distributions.size: " + distributions.size());
      final List<Comparable> values = new ArrayList<>();
      int rowCount = 0;
      for (final List<Comparable> row : rows) {
        ++rowCount;
      joint:
        for (Space space : spaces) {
          values.clear();
          for (Column column : space.columns) {
            final Comparable value = row.get(column.ordinal);
            values.add(value);
            if (value == NullSentinel.INSTANCE) {
              space.nullCount++;
              continue joint;
            }
          }
          space.values.add(FlatLists.ofComparable(values));
        }
      }

      // Populate unique keys
      for (Space space : spaces) {
        keyResults.add(space);
        if (!keyResults.getChildren(space).isEmpty()) {
          // If [x, y] is a key,
          // then [x, y, z] is a non-minimal key (therefore not interesting),
          // and [x, y] => [a] is a functional dependency but not interesting,
          // and [x, y, z] is not an interesting distribution.
          keyResults.remove(space);
          continue;
        }
        keyResults.remove(space);
        results.add(space);

        int nonMinimal = 0;
      dependents:
        for (Space s : results.getDescendants(space)) {
          if (s.cardinality() == space.cardinality()) {
            // We have discovered a sub-set that has the same cardinality.
            // The column(s) that are not in common are functionally
            // dependent.
            final ImmutableBitSet dependents =
                space.columnOrdinals.except(s.columnOrdinals);
            for (int i : s.columnOrdinals) {
              final Space s1 = singletonSpaces.get(i);
              final ImmutableBitSet rest = s.columnOrdinals.clear(i);
              for (ImmutableBitSet dependent : s1.dependents) {
                if (rest.contains(dependent)) {
                  // The "key" of this functional dependency is not minimal.
                  // For instance, if we know that
                  //   (a) -> x
                  // then
                  //   (a, b, x) -> y
                  // is not minimal; we could say the same with a smaller key:
                  //   (a, b) -> y
                  ++nonMinimal;
                  continue dependents;
                }
              }
            }
            for (int dependent : dependents) {
              final Space s1 = singletonSpaces.get(dependent);
              for (ImmutableBitSet d : s1.dependents) {
                if (s.columnOrdinals.contains(d)) {
                  ++nonMinimal;
                  continue dependents;
                }
              }
            }
            space.dependencies.or(dependents.toBitSet());
            for (int d : dependents) {
              singletonSpaces.get(d).dependents.add(s.columnOrdinals);
            }
          }
        }
        if (nonMinimal > 0) {
          continue;
        }
        int nullCount;
        final SortedSet<Comparable> valueSet;
        if (space.columns.size() == 1) {
          nullCount = space.nullCount;
          valueSet = ImmutableSortedSet.copyOf(
              Iterables.transform(space.values, ONLY));
        } else {
          nullCount = -1;
          valueSet = null;
        }
        double expectedCardinality;
        final double cardinality = space.cardinality();
        switch (space.columns.size()) {
        case 0:
          expectedCardinality = 1d;
          break;
        case 1:
          expectedCardinality = rowCount;
          break;
        default:
          expectedCardinality = rowCount;
          for (Column column : space.columns) {
            final Distribution d1 =
                distributions.get(ImmutableBitSet.of(column.ordinal));
            final double c1 = d1.cardinality;
            final Distribution d2 =
                distributions.get(space.columnOrdinals.clear(column.ordinal));
            final double c2 = d2.cardinality;
            final double d =
                Lattice.getRowCount(rowCount, Arrays.asList(c1, c2));
            expectedCardinality = Math.min(expectedCardinality, d);
          }
        }
        final boolean minimal = nonMinimal == 0
            && !space.unique
            && !containsKey(space.columnOrdinals);
        final Distribution distribution =
            new Distribution(space.columns, valueSet, cardinality, nullCount,
                expectedCardinality, minimal);
        statistics.add(distribution);
        distributions.put(space.columnOrdinals, distribution);

        if (space.values.size() == rowCount) {
          // We have discovered a new key. It is not a super-set of a key.
          statistics.add(new Unique(space.columns));
          keyOrdinalLists.add(space.columnOrdinals);
          space.unique = true;
        }
      }

      if (pass == 0) {
        statistics.add(new RowCount(rowCount));
      }
    }

    private ImmutableSortedSet<Column> toColumns(Iterable<Integer> ordinals) {
      return ImmutableSortedSet.copyOf(Iterables.transform(ordinals, get));
    }
  }

  /** Work space for a particular combination of columns. */
  static class Space implements Comparable<Space> {
    private final Run run;
    final ImmutableBitSet columnOrdinals;
    final ImmutableSortedSet<Column> columns;
    int nullCount;
    final SortedSet<FlatLists.ComparableList<Comparable>> values =
        new TreeSet<>();
    boolean unique;
    final BitSet dependencies = new BitSet();
    final Set<ImmutableBitSet> dependents = new HashSet<>();

    Space(Run run, ImmutableBitSet columnOrdinals, Iterable<Column> columns) {
      this.run = run;
      this.columnOrdinals = columnOrdinals;
      this.columns = ImmutableSortedSet.copyOf(columns);
    }

    @Override public int hashCode() {
      return columnOrdinals.hashCode();
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof Space
          && columnOrdinals.equals(((Space) o).columnOrdinals);
    }

    public int compareTo(@Nonnull Space o) {
      return columnOrdinals.equals(o.columnOrdinals) ? 0
          : columnOrdinals.contains(o.columnOrdinals) ? 1
              : -1;
    }

    public double cardinality() {
      return values.size();
    }

    /** Returns the distribution created from this space, or null if no
     * distribution has been registered yet. */
    public Distribution distribution() {
      return run.distributions.get(columnOrdinals);
    }
  }
}

// End ProfilerImpl.java
