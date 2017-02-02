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
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.PartiallyOrderedSet;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Basic implementation of {@link Profiler}.
 */
public class SimpleProfiler implements Profiler {
  private static final Function<List<Comparable>, Comparable> ONLY =
      new Function<List<Comparable>, Comparable>() {
        public Comparable apply(List<Comparable> input) {
          return Iterables.getOnlyElement(input);
        }
      };

  public List<Statistic> profile(Iterable<List<Comparable>> rows,
      final List<Column> columns) {
    return new Run(columns).profile(rows);
  }

  /** A run of the profiler. */
  static class Run {
    private final List<Column> columns;
    final List<Space> spaces = new ArrayList<>();
    final List<Space> singletonSpaces;
    final List<Statistic> statistics = new ArrayList<>();
    final PartiallyOrderedSet.Ordering<Space> ordering =
        new PartiallyOrderedSet.Ordering<Space>() {
          public boolean lessThan(Space e1, Space e2) {
            return e2.columns.containsAll(e1.columns);
          }
        };
    final PartiallyOrderedSet<Space> results =
        new PartiallyOrderedSet<>(ordering);
    final PartiallyOrderedSet<Space> keyResults =
        new PartiallyOrderedSet<>(ordering);
    final Function<Integer, Column> get =
        new Function<Integer, Column>() {
          public Column apply(Integer input) {
            return columns.get(input);
          }
        };

    Run(final List<Column> columns) {
      for (Ord<Column> column : Ord.zip(columns)) {
        if (column.e.ordinal != column.i) {
          throw new IllegalArgumentException();
        }
      }
      this.columns = columns;
      this.singletonSpaces =
          new ArrayList<>(Collections.nCopies(columns.size(), (Space) null));
      for (ImmutableBitSet ordinals
          : ImmutableBitSet.range(columns.size()).powerSet()) {
        final Space space = new Space(ordinals, toColumns(ordinals));
        spaces.add(space);
        if (ordinals.cardinality() == 1) {
          singletonSpaces.set(ordinals.nth(0), space);
        }
      }
    }

    List<Statistic> profile(Iterable<List<Comparable>> rows) {
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
          // then [x, y, z] is a key but not intersecting,
          // and [x, y] => [a] is a functional dependency but not interesting,
          // and [x, y, z] is not an interesting distribution.
          keyResults.remove(space);
          continue;
        }
        if (space.values.size() == rowCount) {
          // We have discovered a new key. It is not a super-set of a key.
          statistics.add(new Unique(space.columns));
          space.unique = true;
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
        statistics.add(
            new Distribution(space.columns, valueSet, space.cardinality(),
                nullCount));
      }

      for (Space s : singletonSpaces) {
        for (ImmutableBitSet dependent : s.dependents) {
          statistics.add(
              new FunctionalDependency(toColumns(dependent),
                  Iterables.getOnlyElement(s.columns)));
        }
      }
      statistics.add(new RowCount(rowCount));
      return statistics;
    }

    private ImmutableSortedSet<Column> toColumns(Iterable<Integer> ordinals) {
      return ImmutableSortedSet.copyOf(Iterables.transform(ordinals, get));
    }
  }

  /** Work space for a particular combination of columns. */
  static class Space implements Comparable<Space> {
    final ImmutableBitSet columnOrdinals;
    final ImmutableSortedSet<Column> columns;
    int nullCount;
    final SortedSet<FlatLists.ComparableList<Comparable>> values =
        new TreeSet<>();
    boolean unique;
    final BitSet dependencies = new BitSet();
    final Set<ImmutableBitSet> dependents = new HashSet<>();

    Space(ImmutableBitSet columnOrdinals, Iterable<Column> columns) {
      this.columnOrdinals = columnOrdinals;
      this.columns = ImmutableSortedSet.copyOf(columns);
    }

    public int compareTo(@Nonnull Space o) {
      return columnOrdinals.equals(o.columnOrdinals) ? 0
          : columnOrdinals.contains(o.columnOrdinals) ? 1
              : -1;
    }

    public double cardinality() {
      return values.size();
    }
  }
}

// End SimpleProfiler.java
