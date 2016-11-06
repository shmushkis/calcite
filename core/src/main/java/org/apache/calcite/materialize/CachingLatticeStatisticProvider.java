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

import org.apache.calcite.util.Pair;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

/**
 * Implementation of {@link LatticeStatisticProvider} that gets statistics by
 * executing "SELECT COUNT(DISTINCT ...) ..." SQL queries.
 */
class CachingLatticeStatisticProvider implements LatticeStatisticProvider {
  private final LoadingCache<Key, Integer> cache;

  /** Creates a CachingStatisticProvider. */
  public CachingLatticeStatisticProvider(
      final LatticeStatisticProvider provider) {
    cache = CacheBuilder.<Key>newBuilder()
        .build(
            new CacheLoader<Key, Integer>() {
              public Integer load(@Nonnull Key key) throws Exception {
                return provider.cardinality(key.left, key.right);
              }
            });
  }

  public int cardinality(Lattice lattice, Collection<Lattice.Column> columns) {
    try {
      return cache.get(new Key(lattice, columns));
    } catch (UncheckedExecutionException | ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  /** Key. */
  private static class Key extends Pair<Lattice, ImmutableList<Lattice.Column>> {
    public Key(Lattice left, Collection<Lattice.Column> right) {
      super(left, sort(right));
    }

    private static ImmutableList<Lattice.Column> sort(Collection<Lattice.Column> columns) {
      if (Ordering.natural().isOrdered(columns)) {
        return ImmutableList.copyOf(columns);
      } else {
        return Ordering.natural().immutableSortedCopy(columns);
      }
    }

  }
}

// End CachingLatticeStatisticProvider.java
