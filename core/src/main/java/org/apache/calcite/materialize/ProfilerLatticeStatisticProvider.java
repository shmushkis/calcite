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

import org.apache.calcite.profile.Profiler;
import org.apache.calcite.profile.ProfilerImpl;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link LatticeStatisticProvider} that uses a
 * {@link org.apache.calcite.profile.Profiler}.
 */
class ProfilerLatticeStatisticProvider implements LatticeStatisticProvider {
  static final Factory FACTORY =
      new Factory() {
        public LatticeStatisticProvider apply(Lattice lattice) {
          return ProfilerLatticeStatisticProvider.create(lattice);
        }
      };
  private final Lattice lattice;
  private final Profiler.Profile profile;

  /** Creates a ProfilerLatticeStatisticProvider. */
  ProfilerLatticeStatisticProvider(Lattice lattice,
      Profiler.Profile profile) {
    this.lattice = lattice;
    this.profile = profile;
  }

  /** Creates a profiler. */
  private static ProfilerLatticeStatisticProvider create(Lattice lattice) {
    final ProfilerImpl profiler = ProfilerImpl.builder().build();
    final Iterable<List<Comparable>> rows = new ArrayList<>();
    final ArrayList<Profiler.Column> columns = new ArrayList<>();
    final Profiler.Profile profile = profiler.profile(rows, columns);
    return new ProfilerLatticeStatisticProvider(lattice, profile);
  }

  public double cardinality(List<Lattice.Column> columns) {
    final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (Lattice.Column column : columns) {
      builder.set(column.ordinal);
    }
    return profile.cardinality(builder.build());
  }
}

// End ProfilerLatticeStatisticProvider.java
