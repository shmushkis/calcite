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

import org.apache.calcite.util.JsonBuilder;

import com.google.common.collect.ImmutableSortedSet;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import javax.annotation.Nonnull;

/**
 * Analyzes data sets.
 */
public interface Profiler {
  List<Statistic> profile(Iterable<List<Comparable>> rows,
      List<Column> columns);

  /** Column. */
  class Column implements Comparable<Column> {
    public final int ordinal;
    public final String name;

    /** Creates a Column.
     *
     * @param ordinal Unique and contiguous within a particular data set
     * @param name Name of the column
     */
    public Column(int ordinal, String name) {
      this.ordinal = ordinal;
      this.name = name;
    }

    @Override public int hashCode() {
      return ordinal;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof Column
          && ordinal == ((Column) o).ordinal;
    }

    @Override public int compareTo(@Nonnull Column column) {
      return Integer.compare(ordinal, column.ordinal);
    }

    @Override public String toString() {
      return name;
    }
  }

  /** Statistic produced by the profiler. */
  interface Statistic {
    Object toMap(JsonBuilder jsonBuilder);
  }

  /** Whole data set. */
  class RowCount implements Statistic {
    final int rowCount;

    public RowCount(int rowCount) {
      this.rowCount = rowCount;
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "rowCount");
      map.put("rowCount", rowCount);
      return map;
    }
  }

  /** Unique key. */
  class Unique implements Statistic {
    final NavigableSet<Column> columns;

    public Unique(SortedSet<Column> columns) {
      this.columns = ImmutableSortedSet.copyOf(columns);
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "unique");
      map.put("columns", FunctionalDependency.getObjects(jsonBuilder, columns));
      return map;
    }
  }

  /** Functional dependency. */
  class FunctionalDependency implements Statistic {
    final NavigableSet<Column> columns;
    final Column dependentColumn;

    FunctionalDependency(SortedSet<Column> columns, Column dependentColumn) {
      this.columns = ImmutableSortedSet.copyOf(columns);
      this.dependentColumn = dependentColumn;
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "fd");
      map.put("columns", getObjects(jsonBuilder, columns));
      map.put("dependentColumn", dependentColumn.name);
      return map;
    }

    private static List<Object> getObjects(JsonBuilder jsonBuilder,
        NavigableSet<Column> columns) {
      final List<Object> list = jsonBuilder.list();
      for (Column column : columns) {
        list.add(column.name);
      }
      return list;
    }
  }

  /** Value distribution, including cardinality and optionally values, of a
   * column or set of columns. If the set of columns is empty, it describes
   * the number of rows in the entire data set. */
  class Distribution implements Statistic {
    final NavigableSet<Column> columns;
    final NavigableSet<Comparable> values;
    final double cardinality;
    final int nullCount;
    final double expectedCardinality;
    final boolean minimal;

    /** Creates a Distribution.
     *
     * @param columns Column or columns being described
     * @param values Values of columns, or null if there are too many
     * @param cardinality Number of distinct values
     * @param nullCount Number of rows where this column had a null value;
     * @param expectedCardinality Expected cardinality
     * @param minimal Whether the distribution is not implied by a unique
     *   or functional dependency
     */
    public Distribution(SortedSet<Column> columns, SortedSet<Comparable> values,
        double cardinality, int nullCount, double expectedCardinality,
        boolean minimal) {
      this.columns = ImmutableSortedSet.copyOf(columns);
      this.values = values == null ? null : ImmutableSortedSet.copyOf(values);
      this.cardinality = cardinality;
      this.nullCount = nullCount;
      this.expectedCardinality = expectedCardinality;
      this.minimal = minimal;
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "distribution");
      map.put("columns", FunctionalDependency.getObjects(jsonBuilder, columns));
      if (values != null) {
        List<Object> list = jsonBuilder.list();
        for (Comparable value : values) {
          if (value instanceof java.sql.Date) {
            value = value.toString();
          }
          list.add(value);
        }
        map.put("values", list);
      }
      map.put("cardinality", cardinality);
      if (nullCount > 0) {
        map.put("nullCount", nullCount);
      }
      map.put("expectedCardinality", expectedCardinality);
      map.put("surprise", surprise());
      return map;
    }

    double surprise() {
      return SimpleProfiler.surprise(expectedCardinality, cardinality);
    }
  }
}

// End Profiler.java
