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
package org.apache.calcite.util;

import org.apache.calcite.profile.ProfilerImpl;
import org.apache.calcite.test.CalciteAssert;

import org.junit.Test;

import java.util.AbstractList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link PartiallyOrderedSet}.
 */
public class PartiallyOrderedSetTest {
  private static final boolean DEBUG = false;

  // 100, 250, 1000, 3000 are reasonable
  private static final int SCALE = CalciteAssert.ENABLE_SLOW ? 250 : 50;

  final long seed = new Random().nextLong();
  final Random random = new Random(seed);

  static final PartiallyOrderedSet.Ordering<String> STRING_SUBSET_ORDERING =
      new PartiallyOrderedSet.Ordering<String>() {
        public boolean lessThan(String e1, String e2) {
          // e1 < e2 if every char in e1 is also in e2
          for (int i = 0; i < e1.length(); i++) {
            if (e2.indexOf(e1.charAt(i)) < 0) {
              return false;
            }
          }
          return true;
        }
      };

  // Integers, ordered by division. Top is 1, its children are primes,
  // etc.
  static final PartiallyOrderedSet.Ordering<Integer> IS_DIVISOR =
      new PartiallyOrderedSet.Ordering<Integer>() {
        public boolean lessThan(Integer e1, Integer e2) {
          return e2 % e1 == 0;
        }
      };

  // Bottom is 1, parents are primes, etc.
  static final PartiallyOrderedSet.Ordering<Integer> IS_DIVISOR_INVERSE =
      new PartiallyOrderedSet.Ordering<Integer>() {
        public boolean lessThan(Integer e1, Integer e2) {
          return e1 % e2 == 0;
        }
      };

  // Ordered by bit inclusion. E.g. the children of 14 (1110) are
  // 12 (1100), 10 (1010) and 6 (0110).
  static final PartiallyOrderedSet.Ordering<Integer> IS_BIT_SUBSET =
      new PartiallyOrderedSet.Ordering<Integer>() {
        public boolean lessThan(Integer e1, Integer e2) {
          return (e2 & e1) == e2;
        }
      };

  // Ordered by bit inclusion. E.g. the children of 14 (1110) are
  // 12 (1100), 10 (1010) and 6 (0110).
  static final PartiallyOrderedSet.Ordering<Integer> IS_BIT_SUPERSET =
      new PartiallyOrderedSet.Ordering<Integer>() {
        public boolean lessThan(Integer e1, Integer e2) {
          return (e2 & e1) == e1;
        }
      };

  @Test public void testPoset() {
    String empty = "''";
    String abcd = "'abcd'";
    PartiallyOrderedSet<String> poset =
        new PartiallyOrderedSet<String>(STRING_SUBSET_ORDERING);
    assertEquals(0, poset.size());

    final StringBuilder buf = new StringBuilder();
    poset.out(buf);
    TestUtil.assertEqualsVerbose(
        "PartiallyOrderedSet size: 0 elements: {\n"
            + "}",
        buf.toString());

    poset.add("a");
    printValidate(poset);
    poset.add("b");
    printValidate(poset);

    poset.clear();
    assertEquals(0, poset.size());
    poset.add(empty);
    printValidate(poset);
    poset.add(abcd);
    printValidate(poset);
    assertEquals(2, poset.size());
    assertEquals("['abcd']", poset.getNonChildren().toString());
    assertEquals("['']", poset.getNonParents().toString());

    final String ab = "'ab'";
    poset.add(ab);
    printValidate(poset);
    assertEquals(3, poset.size());
    assertEquals("[]", poset.getChildren(empty).toString());
    assertEquals("['ab']", poset.getParents(empty).toString());
    assertEquals("['ab']", poset.getChildren(abcd).toString());
    assertEquals("[]", poset.getParents(abcd).toString());
    assertEquals("['']", poset.getChildren(ab).toString());
    assertEquals("['abcd']", poset.getParents(ab).toString());

    // "bcd" is child of "abcd" and parent of ""
    final String bcd = "'bcd'";
    poset.add(bcd);
    printValidate(poset);
    assertTrue(poset.isValid(false));
    assertEquals("['']", poset.getChildren(bcd).toString());
    assertEquals("['abcd']", poset.getParents(bcd).toString());
    assertEquals("['ab', 'bcd']", poset.getChildren(abcd).toString());

    buf.setLength(0);
    poset.out(buf);
    TestUtil.assertEqualsVerbose(
        "PartiallyOrderedSet size: 4 elements: {\n"
            + "  'abcd' parents: [] children: ['ab', 'bcd']\n"
            + "  'ab' parents: ['abcd'] children: ['']\n"
            + "  'bcd' parents: ['abcd'] children: ['']\n"
            + "  '' parents: ['ab', 'bcd'] children: []\n"
            + "}",
        buf.toString());

    final String b = "'b'";

    // ancestors of an element not in the set
    assertEqualsList("['ab', 'abcd', 'bcd']", poset.getAncestors(b));

    poset.add(b);
    printValidate(poset);
    assertEquals("['abcd']", poset.getNonChildren().toString());
    assertEquals("['']", poset.getNonParents().toString());
    assertEquals("['']", poset.getChildren(b).toString());
    assertEqualsList("['ab', 'bcd']", poset.getParents(b));
    assertEquals("['']", poset.getChildren(b).toString());
    assertEquals("['ab', 'bcd']", poset.getChildren(abcd).toString());
    assertEquals("['b']", poset.getChildren(bcd).toString());
    assertEquals("['b']", poset.getChildren(ab).toString());
    assertEqualsList("['ab', 'abcd', 'bcd']", poset.getAncestors(b));

    // descendants and ancestors of an element with no descendants
    assertEquals("[]", poset.getDescendants(empty).toString());
    assertEqualsList(
        "['ab', 'abcd', 'b', 'bcd']",
        poset.getAncestors(empty));

    // some more ancestors of missing elements
    assertEqualsList("['abcd']", poset.getAncestors("'ac'"));
    assertEqualsList("[]", poset.getAncestors("'z'"));
    assertEqualsList("['ab', 'abcd']", poset.getAncestors("'a'"));
  }

  @Test public void testPosetTricky() {
    PartiallyOrderedSet<String> poset =
        new PartiallyOrderedSet<String>(STRING_SUBSET_ORDERING);

    // A tricky little poset with 4 elements:
    // {a <= ab and ac, b < ab, ab, ac}
    poset.clear();
    poset.add("'a'");
    printValidate(poset);
    poset.add("'b'");
    printValidate(poset);
    poset.add("'ac'");
    printValidate(poset);
    poset.add("'ab'");
    printValidate(poset);
  }

  @Test public void testPosetBits() {
    final PartiallyOrderedSet<Integer> poset =
        new PartiallyOrderedSet<Integer>(IS_BIT_SUPERSET);
    poset.add(2112); // {6, 11} i.e. 64 + 2048
    poset.add(2240); // {6, 7, 11} i.e. 64 + 128 + 2048
    poset.add(2496); // {6, 7, 8, 11} i.e. 64 + 128 + 256 + 2048
    printValidate(poset);
    poset.remove(2240);
    printValidate(poset);
    poset.add(2240); // {6, 7, 11} i.e. 64 + 128 + 2048
    printValidate(poset);
  }

  @Test public void testPosetBitsRemoveParent() {
    final PartiallyOrderedSet<Integer> poset =
        new PartiallyOrderedSet<Integer>(IS_BIT_SUPERSET);
    poset.add(66); // {bit 2, bit 6}
    poset.add(68); // {bit 3, bit 6}
    poset.add(72); // {bit 4, bit 6}
    poset.add(64); // {bit 6}
    printValidate(poset);
    poset.remove(64); // {bit 6}
    printValidate(poset);
  }

  @Test public void testDivisorPoset() {
    if (!CalciteAssert.ENABLE_SLOW) {
      return;
    }
    PartiallyOrderedSet<Integer> integers =
        new PartiallyOrderedSet<Integer>(IS_DIVISOR, range(1, 1000));
    assertEquals(
        "[1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 24, 30, 40, 60]",
        new TreeSet<Integer>(integers.getDescendants(120)).toString());
    assertEquals(
        "[240, 360, 480, 600, 720, 840, 960]",
        new TreeSet<Integer>(integers.getAncestors(120)).toString());
    assertTrue(integers.getDescendants(1).isEmpty());
    assertEquals(
        998,
        integers.getAncestors(1).size());
    assertTrue(integers.isValid(true));
  }

  @Test public void testDivisorSeries() {
    checkPoset(IS_DIVISOR, DEBUG, range(1, SCALE * 3), false);
  }

  @Test public void testDivisorRandom() {
    boolean ok = false;
    try {
      checkPoset(
          IS_DIVISOR, DEBUG, random(random, SCALE, SCALE * 3), false);
      ok = true;
    } finally {
      if (!ok) {
        System.out.println("Random seed: " + seed);
      }
    }
  }

  @Test public void testDivisorRandomWithRemoval() {
    boolean ok = false;
    try {
      checkPoset(
          IS_DIVISOR, DEBUG, random(random, SCALE, SCALE * 3), true);
      ok = true;
    } finally {
      if (!ok) {
        System.out.println("Random seed: " + seed);
      }
    }
  }

  @Test public void testDivisorInverseSeries() {
    checkPoset(IS_DIVISOR_INVERSE, DEBUG, range(1, SCALE * 3), false);
  }

  @Test public void testDivisorInverseRandom() {
    boolean ok = false;
    try {
      checkPoset(
          IS_DIVISOR_INVERSE, DEBUG, random(random, SCALE, SCALE * 3),
          false);
      ok = true;
    } finally {
      if (!ok) {
        System.out.println("Random seed: " + seed);
      }
    }
  }

  @Test public void testDivisorInverseRandomWithRemoval() {
    boolean ok = false;
    try {
      checkPoset(
          IS_DIVISOR_INVERSE, DEBUG, random(random, SCALE, SCALE * 3),
          true);
      ok = true;
    } finally {
      if (!ok) {
        System.out.println("Random seed: " + seed);
      }
    }
  }

  @Test public void testSubsetSeries() {
    checkPoset(IS_BIT_SUBSET, DEBUG, range(1, SCALE / 2), false);
  }

  @Test public void testSubsetRandom() {
    boolean ok = false;
    try {
      checkPoset(
          IS_BIT_SUBSET, DEBUG, random(random, SCALE / 4, SCALE), false);
      ok = true;
    } finally {
      if (!ok) {
        System.out.println("Random seed: " + seed);
      }
    }
  }

  private <E> void printValidate(PartiallyOrderedSet<E> poset) {
    if (DEBUG) {
      dump(poset);
    }
    assertTrue(poset.isValid(DEBUG));
  }

  public void checkPoset(
      PartiallyOrderedSet.Ordering<Integer> ordering,
      boolean debug,
      Iterable<Integer> generator,
      boolean remove) {
    final PartiallyOrderedSet<Integer> poset =
        new PartiallyOrderedSet<Integer>(ordering);
    int n = 0;
    int z = 0;
    if (debug) {
      dump(poset);
    }
    for (int i : generator) {
      if (remove && z++ % 2 == 0) {
        if (debug) {
          System.out.println("remove " + i);
        }
        poset.remove(i);
        if (debug) {
          dump(poset);
        }
        continue;
      }
      if (debug) {
        System.out.println("add " + i);
      }
      poset.add(i);
      if (debug) {
        dump(poset);
      }
      assertEquals(++n, poset.size());
      if (i < 100) {
        if (!poset.isValid(false)) {
          dump(poset);
        }
        assertTrue(poset.isValid(true));
      }
    }
    assertTrue(poset.isValid(true));

    final StringBuilder buf = new StringBuilder();
    poset.out(buf);
    assertTrue(buf.length() > 0);
  }

  private <E> void dump(PartiallyOrderedSet<E> poset) {
    final StringBuilder buf = new StringBuilder();
    poset.out(buf);
    System.out.println(buf);
  }

  private static Collection<Integer> range(
      final int start, final int end) {
    return new AbstractList<Integer>() {
      @Override public Integer get(int index) {
        return start + index;
      }

      @Override public int size() {
        return end - start;
      }
    };
  }

  private static Iterable<Integer> random(
      Random random, final int size, final int max) {
    final Set<Integer> set = new LinkedHashSet<Integer>();
    while (set.size() < size) {
      set.add(random.nextInt(max) + 1);
    }
    return set;
  }

  private static void assertEqualsList(String expected, List<String> ss) {
    assertEquals(
        expected,
        new TreeSet<String>(ss).toString());
  }

  @Test public void testFoo() {
    final PartiallyOrderedSet.Ordering<ImmutableBitSet> ordering =
        new PartiallyOrderedSet.Ordering<ImmutableBitSet>() {
          public boolean lessThan(ImmutableBitSet e1, ImmutableBitSet e2) {
            return e2.contains(e1);
          }
        };
    final PartiallyOrderedSet<ImmutableBitSet> pos =
        new PartiallyOrderedSet<>(ordering);
    foo(pos, new int[] {});
    foo(pos, new int[] {0});
    foo(pos, new int[] {1});
    foo(pos, new int[] {2});
    foo(pos, new int[] {3});
    foo(pos, new int[] {4});
    foo(pos, new int[] {5});
    foo(pos, new int[] {6});
    foo(pos, new int[] {7});
    foo(pos, new int[] {8});
    foo(pos, new int[] {9});
    foo(pos, new int[] {10});
    foo(pos, new int[] {11});
    foo(pos, new int[] {12});
    foo(pos, new int[] {13});
    foo(pos, new int[] {14});
    foo(pos, new int[] {15});
    foo(pos, new int[] {16});
    foo(pos, new int[] {17});
    foo(pos, new int[] {18});
    foo(pos, new int[] {19});
    foo(pos, new int[] {20});
    foo(pos, new int[] {21});
    foo(pos, new int[] {22});
    foo(pos, new int[] {23});
    foo(pos, new int[] {24});
    foo(pos, new int[] {25});
    foo(pos, new int[] {26});
    foo(pos, new int[] {27});
    foo(pos, new int[] {28});
    foo(pos, new int[] {29});
    foo(pos, new int[] {30});
    foo(pos, new int[] {31});
    foo(pos, new int[] {32});
    foo(pos, new int[] {33});
    foo(pos, new int[] {34});
    foo(pos, new int[] {35});
    foo(pos, new int[] {36});
    foo(pos, new int[] {0, 6});
    foo(pos, new int[] {1, 6});
    foo(pos, new int[] {2, 6});
    foo(pos, new int[] {3, 6});
    foo(pos, new int[] {4, 6});
    foo(pos, new int[] {5, 6});
    foo(pos, new int[] {6, 7});
    foo(pos, new int[] {6, 8});
    foo(pos, new int[] {6, 9});
    foo(pos, new int[] {6, 10});
    foo(pos, new int[] {6, 11});
    foo(pos, new int[] {6, 12});
    foo(pos, new int[] {6, 13});
    foo(pos, new int[] {6, 14});
    foo(pos, new int[] {6, 15});
    foo(pos, new int[] {6, 16});
    foo(pos, new int[] {6, 17});
    foo(pos, new int[] {6, 18});
    foo(pos, new int[] {6, 19});
    foo(pos, new int[] {6, 20});
    foo(pos, new int[] {6, 21});
    foo(pos, new int[] {6, 22});
    foo(pos, new int[] {6, 23});
    foo(pos, new int[] {6, 24});
    foo(pos, new int[] {6, 25});
    foo(pos, new int[] {6, 26});
    foo(pos, new int[] {6, 27});
    foo(pos, new int[] {6, 28});
    foo(pos, new int[] {6, 29});
    foo(pos, new int[] {6, 30});
    foo(pos, new int[] {6, 31});
    foo(pos, new int[] {6, 32});
    foo(pos, new int[] {6, 33});
    foo(pos, new int[] {6, 34});
    foo(pos, new int[] {6, 35});
    foo(pos, new int[] {6, 36});
    foo(pos, new int[] {0, 2});
    foo(pos, new int[] {1, 2});
    foo(pos, new int[] {2, 3});
    foo(pos, new int[] {2, 4});
    foo(pos, new int[] {2, 5});
    foo(pos, new int[] {2, 6});
    foo(pos, new int[] {2, 7});
    foo(pos, new int[] {2, 8});
    foo(pos, new int[] {2, 9});
    foo(pos, new int[] {2, 10});
    foo(pos, new int[] {2, 11});
    foo(pos, new int[] {2, 12});
    foo(pos, new int[] {2, 13});
    foo(pos, new int[] {2, 14});
    foo(pos, new int[] {2, 15});
    foo(pos, new int[] {2, 16});
    foo(pos, new int[] {2, 17});
    foo(pos, new int[] {2, 18});
    foo(pos, new int[] {2, 19});
    foo(pos, new int[] {2, 20});
    foo(pos, new int[] {2, 21});
    foo(pos, new int[] {2, 22});
    foo(pos, new int[] {2, 23});
    foo(pos, new int[] {2, 24});
    foo(pos, new int[] {2, 25});
    foo(pos, new int[] {2, 26});
    foo(pos, new int[] {2, 27});
    foo(pos, new int[] {2, 28});
    foo(pos, new int[] {2, 29});
    foo(pos, new int[] {2, 30});
    foo(pos, new int[] {2, 31});
    foo(pos, new int[] {2, 32});
    foo(pos, new int[] {2, 33});
    foo(pos, new int[] {2, 34});
    foo(pos, new int[] {2, 35});
    foo(pos, new int[] {2, 36});
    foo(pos, new int[] {0, 9});
    foo(pos, new int[] {1, 9});
    foo(pos, new int[] {2, 9});
    foo(pos, new int[] {3, 9});
    foo(pos, new int[] {4, 9});
    foo(pos, new int[] {5, 9});
    foo(pos, new int[] {6, 9});
    foo(pos, new int[] {7, 9});
    foo(pos, new int[] {8, 9});
    foo(pos, new int[] {9, 10});
    foo(pos, new int[] {9, 11});
    foo(pos, new int[] {9, 12});
    foo(pos, new int[] {9, 13});
    foo(pos, new int[] {9, 14});
    foo(pos, new int[] {9, 15});
    foo(pos, new int[] {9, 16});
    foo(pos, new int[] {9, 17});
    foo(pos, new int[] {9, 18});
    foo(pos, new int[] {9, 19});
    foo(pos, new int[] {9, 20});
    foo(pos, new int[] {9, 21});
    foo(pos, new int[] {9, 22});
    foo(pos, new int[] {9, 23});
    foo(pos, new int[] {9, 24});
    foo(pos, new int[] {9, 25});
    foo(pos, new int[] {9, 26});
    foo(pos, new int[] {9, 27});
    foo(pos, new int[] {9, 28});
    foo(pos, new int[] {9, 29});
    foo(pos, new int[] {9, 30});
    foo(pos, new int[] {9, 31});
    foo(pos, new int[] {9, 32});
    foo(pos, new int[] {9, 33});
    foo(pos, new int[] {9, 34});
    foo(pos, new int[] {9, 35});
    foo(pos, new int[] {9, 36});
    foo(pos, new int[] {0, 11});
    foo(pos, new int[] {1, 11});
    foo(pos, new int[] {2, 11});
    foo(pos, new int[] {3, 11});
    foo(pos, new int[] {4, 11});
    foo(pos, new int[] {5, 11});
    foo(pos, new int[] {6, 11});
    foo(pos, new int[] {7, 11});
    foo(pos, new int[] {8, 11});
    foo(pos, new int[] {9, 11});
    foo(pos, new int[] {10, 11});
    foo(pos, new int[] {11, 12});
    foo(pos, new int[] {11, 13});
    foo(pos, new int[] {11, 14});
    foo(pos, new int[] {11, 15});
    foo(pos, new int[] {11, 16});
    foo(pos, new int[] {11, 17});
    foo(pos, new int[] {11, 18});
    foo(pos, new int[] {11, 19});
    foo(pos, new int[] {11, 20});
    foo(pos, new int[] {11, 21});
    foo(pos, new int[] {11, 22});
    foo(pos, new int[] {11, 23});
    foo(pos, new int[] {11, 24});
    foo(pos, new int[] {11, 25});
    foo(pos, new int[] {11, 26});
    foo(pos, new int[] {11, 27});
    foo(pos, new int[] {11, 28});
    foo(pos, new int[] {11, 29});
    foo(pos, new int[] {11, 30});
    foo(pos, new int[] {11, 31});
    foo(pos, new int[] {11, 32});
    foo(pos, new int[] {11, 33});
    foo(pos, new int[] {11, 34});
    foo(pos, new int[] {11, 35});
    foo(pos, new int[] {11, 36});
    foo(pos, new int[] {0, 12});
    foo(pos, new int[] {1, 12});
    foo(pos, new int[] {2, 12});
    foo(pos, new int[] {3, 12});
    foo(pos, new int[] {4, 12});
    foo(pos, new int[] {5, 12});
    foo(pos, new int[] {6, 12});
    foo(pos, new int[] {7, 12});
    foo(pos, new int[] {8, 12});
    foo(pos, new int[] {9, 12});
    foo(pos, new int[] {10, 12});
    foo(pos, new int[] {11, 12});
    foo(pos, new int[] {12, 13});
    foo(pos, new int[] {12, 14});
    foo(pos, new int[] {12, 15});
    foo(pos, new int[] {12, 16});
    foo(pos, new int[] {12, 17});
    foo(pos, new int[] {12, 18});
    foo(pos, new int[] {12, 19});
    foo(pos, new int[] {12, 20});
    foo(pos, new int[] {12, 21});
    foo(pos, new int[] {12, 22});
    foo(pos, new int[] {12, 23});
    foo(pos, new int[] {12, 24});
    foo(pos, new int[] {12, 25});
    foo(pos, new int[] {12, 26});
    foo(pos, new int[] {12, 27});
    foo(pos, new int[] {12, 28});
    foo(pos, new int[] {12, 29});
    foo(pos, new int[] {12, 30});
    foo(pos, new int[] {12, 31});
    foo(pos, new int[] {12, 32});
    foo(pos, new int[] {12, 33});
    foo(pos, new int[] {12, 34});
    foo(pos, new int[] {12, 35});
    foo(pos, new int[] {12, 36});
    foo(pos, new int[] {0, 1});
    foo(pos, new int[] {0, 2});
    foo(pos, new int[] {0, 3});
    foo(pos, new int[] {0, 4});
    foo(pos, new int[] {0, 5});
    foo(pos, new int[] {0, 6});
    foo(pos, new int[] {0, 7});
    foo(pos, new int[] {0, 8});
    foo(pos, new int[] {0, 9});
    foo(pos, new int[] {0, 10});
    foo(pos, new int[] {0, 11});
    foo(pos, new int[] {0, 12});
    foo(pos, new int[] {0, 13});
    foo(pos, new int[] {0, 14});
    foo(pos, new int[] {0, 15});
    foo(pos, new int[] {0, 16});
    foo(pos, new int[] {0, 17});
    foo(pos, new int[] {0, 18});
    foo(pos, new int[] {0, 19});
    foo(pos, new int[] {0, 20});
    foo(pos, new int[] {0, 21});
    foo(pos, new int[] {0, 22});
    foo(pos, new int[] {0, 23});
    foo(pos, new int[] {0, 24});
    foo(pos, new int[] {0, 25});
    foo(pos, new int[] {0, 26});
    foo(pos, new int[] {0, 27});
    foo(pos, new int[] {0, 28});
    foo(pos, new int[] {0, 29});
    foo(pos, new int[] {0, 30});
    foo(pos, new int[] {0, 31});
    foo(pos, new int[] {0, 32});
    foo(pos, new int[] {0, 33});
    foo(pos, new int[] {0, 34});
    foo(pos, new int[] {0, 35});
    foo(pos, new int[] {0, 36});
    foo(pos, new int[] {0, 5});
    foo(pos, new int[] {1, 5});
    foo(pos, new int[] {2, 5});
    foo(pos, new int[] {3, 5});
    foo(pos, new int[] {4, 5});
    foo(pos, new int[] {5, 6});
    foo(pos, new int[] {5, 7});
    foo(pos, new int[] {5, 8});
    foo(pos, new int[] {5, 9});
    foo(pos, new int[] {5, 10});
    foo(pos, new int[] {5, 11});
    foo(pos, new int[] {5, 12});
    foo(pos, new int[] {5, 13});
    foo(pos, new int[] {5, 14});
    foo(pos, new int[] {5, 15});
    foo(pos, new int[] {5, 16});
    foo(pos, new int[] {5, 17});
    foo(pos, new int[] {5, 18});
    foo(pos, new int[] {5, 19});
    foo(pos, new int[] {5, 20});
    foo(pos, new int[] {5, 21});
    foo(pos, new int[] {5, 22});
    foo(pos, new int[] {5, 23});
    foo(pos, new int[] {5, 24});
    foo(pos, new int[] {5, 25});
    foo(pos, new int[] {5, 26});
    foo(pos, new int[] {5, 27});
    foo(pos, new int[] {5, 28});
    foo(pos, new int[] {5, 29});
    foo(pos, new int[] {5, 30});
    foo(pos, new int[] {5, 31});
    foo(pos, new int[] {5, 32});
    foo(pos, new int[] {5, 33});
    foo(pos, new int[] {5, 34});
    foo(pos, new int[] {5, 35});
    foo(pos, new int[] {5, 36});
    foo(pos, new int[] {0, 20});
    foo(pos, new int[] {1, 20});
    foo(pos, new int[] {2, 20});
    foo(pos, new int[] {3, 20});
    foo(pos, new int[] {4, 20});
    foo(pos, new int[] {5, 20});
    foo(pos, new int[] {6, 20});
    foo(pos, new int[] {7, 20});
    foo(pos, new int[] {8, 20});
    foo(pos, new int[] {9, 20});
    foo(pos, new int[] {10, 20});
    foo(pos, new int[] {11, 20});
    foo(pos, new int[] {12, 20});
    foo(pos, new int[] {13, 20});
    foo(pos, new int[] {14, 20});
    foo(pos, new int[] {15, 20});
    foo(pos, new int[] {16, 20});
    foo(pos, new int[] {17, 20});
    foo(pos, new int[] {18, 20});
    foo(pos, new int[] {19, 20});
    foo(pos, new int[] {20, 21});
    foo(pos, new int[] {20, 22});
    foo(pos, new int[] {20, 23});
    foo(pos, new int[] {20, 24});
    foo(pos, new int[] {20, 25});
    foo(pos, new int[] {20, 26});
    foo(pos, new int[] {20, 27});
    foo(pos, new int[] {20, 28});
    foo(pos, new int[] {20, 29});
    foo(pos, new int[] {20, 30});
    foo(pos, new int[] {20, 31});
    foo(pos, new int[] {20, 32});
    foo(pos, new int[] {20, 33});
    foo(pos, new int[] {20, 34});
    foo(pos, new int[] {20, 35});
    foo(pos, new int[] {20, 36});
    foo(pos, new int[] {0, 21});
    foo(pos, new int[] {1, 21});
    foo(pos, new int[] {2, 21});
    foo(pos, new int[] {3, 21});
    foo(pos, new int[] {4, 21});
    foo(pos, new int[] {5, 21});
    foo(pos, new int[] {6, 21});
    foo(pos, new int[] {7, 21});
    foo(pos, new int[] {8, 21});
    foo(pos, new int[] {9, 21});
    foo(pos, new int[] {10, 21});
    foo(pos, new int[] {11, 21});
    foo(pos, new int[] {12, 21});
    foo(pos, new int[] {13, 21});
    foo(pos, new int[] {14, 21});
    foo(pos, new int[] {15, 21});
    foo(pos, new int[] {16, 21});
    foo(pos, new int[] {17, 21});
    foo(pos, new int[] {18, 21});
    foo(pos, new int[] {19, 21});
    foo(pos, new int[] {20, 21});
    foo(pos, new int[] {21, 22});
    foo(pos, new int[] {21, 23});
    foo(pos, new int[] {21, 24});
    foo(pos, new int[] {21, 25});
    foo(pos, new int[] {21, 26});
    foo(pos, new int[] {21, 27});
    foo(pos, new int[] {21, 28});
    foo(pos, new int[] {21, 29});
    foo(pos, new int[] {21, 30});
    foo(pos, new int[] {21, 31});
    foo(pos, new int[] {21, 32});
    foo(pos, new int[] {21, 33});
    foo(pos, new int[] {21, 34});
    foo(pos, new int[] {21, 35});
    foo(pos, new int[] {21, 36});
    foo(pos, new int[] {0, 22});
    foo(pos, new int[] {1, 22});
    foo(pos, new int[] {2, 22});
    foo(pos, new int[] {3, 22});
    foo(pos, new int[] {4, 22});
    foo(pos, new int[] {5, 22});
    foo(pos, new int[] {6, 22});
    foo(pos, new int[] {7, 22});
    foo(pos, new int[] {8, 22});
    foo(pos, new int[] {9, 22});
    foo(pos, new int[] {10, 22});
    foo(pos, new int[] {11, 22});
    foo(pos, new int[] {12, 22});
    foo(pos, new int[] {13, 22});
    foo(pos, new int[] {14, 22});
    foo(pos, new int[] {15, 22});
    foo(pos, new int[] {16, 22});
    foo(pos, new int[] {17, 22});
    foo(pos, new int[] {18, 22});
    foo(pos, new int[] {19, 22});
    foo(pos, new int[] {20, 22});
    foo(pos, new int[] {21, 22});
    foo(pos, new int[] {22, 23});
    foo(pos, new int[] {22, 24});
    foo(pos, new int[] {22, 25});
    foo(pos, new int[] {22, 26});
    foo(pos, new int[] {22, 27});
    foo(pos, new int[] {22, 28});
    foo(pos, new int[] {22, 29});
    foo(pos, new int[] {22, 30});
    foo(pos, new int[] {22, 31});
    foo(pos, new int[] {22, 32});
    foo(pos, new int[] {22, 33});
    foo(pos, new int[] {22, 34});
    foo(pos, new int[] {22, 35});
    foo(pos, new int[] {22, 36});
    foo(pos, new int[] {0, 14});
    foo(pos, new int[] {1, 14});
    foo(pos, new int[] {2, 14});
    foo(pos, new int[] {3, 14});
    foo(pos, new int[] {4, 14});
    foo(pos, new int[] {5, 14});
    foo(pos, new int[] {6, 14});
    foo(pos, new int[] {7, 14});
    foo(pos, new int[] {8, 14});
    foo(pos, new int[] {9, 14});
    foo(pos, new int[] {10, 14});
    foo(pos, new int[] {11, 14});
    foo(pos, new int[] {12, 14});
    foo(pos, new int[] {13, 14});
    foo(pos, new int[] {14, 15});
    foo(pos, new int[] {14, 16});
    foo(pos, new int[] {14, 17});
    foo(pos, new int[] {14, 18});
    foo(pos, new int[] {14, 19});
    foo(pos, new int[] {14, 20});
    foo(pos, new int[] {14, 21});
    foo(pos, new int[] {14, 22});
    foo(pos, new int[] {14, 23});
    foo(pos, new int[] {14, 24});
    foo(pos, new int[] {14, 25});
    foo(pos, new int[] {14, 26});
    foo(pos, new int[] {14, 27});
    foo(pos, new int[] {14, 28});
    foo(pos, new int[] {14, 29});
    foo(pos, new int[] {14, 30});
    foo(pos, new int[] {14, 31});
    foo(pos, new int[] {14, 32});
    foo(pos, new int[] {14, 33});
    foo(pos, new int[] {14, 34});
    foo(pos, new int[] {14, 35});
    foo(pos, new int[] {14, 36});
    foo(pos, new int[] {0, 15});
    foo(pos, new int[] {1, 15});
    foo(pos, new int[] {2, 15});
    foo(pos, new int[] {3, 15});
    foo(pos, new int[] {4, 15});
    foo(pos, new int[] {5, 15});
    foo(pos, new int[] {6, 15});
    foo(pos, new int[] {7, 15});
    foo(pos, new int[] {8, 15});
    foo(pos, new int[] {9, 15});
    foo(pos, new int[] {10, 15});
    foo(pos, new int[] {11, 15});
    foo(pos, new int[] {12, 15});
    foo(pos, new int[] {13, 15});
    foo(pos, new int[] {14, 15});
    foo(pos, new int[] {15, 16});
    foo(pos, new int[] {15, 17});
    foo(pos, new int[] {15, 18});
    foo(pos, new int[] {15, 19});
    foo(pos, new int[] {15, 20});
    foo(pos, new int[] {15, 21});
    foo(pos, new int[] {15, 22});
    foo(pos, new int[] {15, 23});
    foo(pos, new int[] {15, 24});
    foo(pos, new int[] {15, 25});
    foo(pos, new int[] {15, 26});
    foo(pos, new int[] {15, 27});
    foo(pos, new int[] {15, 28});
    foo(pos, new int[] {15, 29});
    foo(pos, new int[] {15, 30});
    foo(pos, new int[] {15, 31});
    foo(pos, new int[] {15, 32});
    foo(pos, new int[] {15, 33});
    foo(pos, new int[] {15, 34});
    foo(pos, new int[] {15, 35});
    foo(pos, new int[] {15, 36});
    foo(pos, new int[] {0, 1});
    foo(pos, new int[] {1, 2});
    foo(pos, new int[] {1, 3});
    foo(pos, new int[] {1, 4});
    foo(pos, new int[] {1, 5});
    foo(pos, new int[] {1, 6});
    foo(pos, new int[] {1, 7});
    foo(pos, new int[] {1, 8});
    foo(pos, new int[] {1, 9});
    foo(pos, new int[] {1, 10});
    foo(pos, new int[] {1, 11});
    foo(pos, new int[] {1, 12});
    foo(pos, new int[] {1, 13});
    foo(pos, new int[] {1, 14});
    foo(pos, new int[] {1, 15});
    foo(pos, new int[] {1, 16});
    foo(pos, new int[] {1, 17});
    foo(pos, new int[] {1, 18});
    foo(pos, new int[] {1, 19});
    foo(pos, new int[] {1, 20});
    foo(pos, new int[] {1, 21});
    foo(pos, new int[] {1, 22});
    foo(pos, new int[] {1, 23});
    foo(pos, new int[] {1, 24});
    foo(pos, new int[] {1, 25});
    foo(pos, new int[] {1, 26});
    foo(pos, new int[] {1, 27});
    foo(pos, new int[] {1, 28});
    foo(pos, new int[] {1, 29});
    foo(pos, new int[] {1, 30});
    foo(pos, new int[] {1, 31});
    foo(pos, new int[] {1, 32});
    foo(pos, new int[] {1, 33});
    foo(pos, new int[] {1, 34});
    foo(pos, new int[] {1, 35});
    foo(pos, new int[] {1, 36});
    foo(pos, new int[] {0, 23});
    foo(pos, new int[] {1, 23});
    foo(pos, new int[] {2, 23});
    foo(pos, new int[] {3, 23});
    foo(pos, new int[] {4, 23});
    foo(pos, new int[] {5, 23});
    foo(pos, new int[] {6, 23});
    foo(pos, new int[] {7, 23});
    foo(pos, new int[] {8, 23});
    foo(pos, new int[] {9, 23});
    foo(pos, new int[] {10, 23});
    foo(pos, new int[] {11, 23});
    foo(pos, new int[] {12, 23});
    foo(pos, new int[] {13, 23});
    foo(pos, new int[] {14, 23});
    foo(pos, new int[] {15, 23});
    foo(pos, new int[] {16, 23});
    foo(pos, new int[] {17, 23});
    foo(pos, new int[] {18, 23});
    foo(pos, new int[] {19, 23});
    foo(pos, new int[] {20, 23});
    foo(pos, new int[] {21, 23});
    foo(pos, new int[] {22, 23});
    foo(pos, new int[] {23, 24});
    foo(pos, new int[] {23, 25});
    foo(pos, new int[] {23, 26});
    foo(pos, new int[] {23, 27});
    foo(pos, new int[] {23, 28});
    foo(pos, new int[] {23, 29});
    foo(pos, new int[] {23, 30});
    foo(pos, new int[] {23, 31});
    foo(pos, new int[] {23, 32});
    foo(pos, new int[] {23, 33});
    foo(pos, new int[] {23, 34});
    foo(pos, new int[] {23, 35});
    foo(pos, new int[] {23, 36});
    foo(pos, new int[] {0, 24});
    foo(pos, new int[] {1, 24});
    foo(pos, new int[] {2, 24});
    foo(pos, new int[] {3, 24});
    foo(pos, new int[] {4, 24});
    foo(pos, new int[] {5, 24});
    foo(pos, new int[] {6, 24});
    foo(pos, new int[] {7, 24});
    foo(pos, new int[] {8, 24});
    foo(pos, new int[] {9, 24});
    foo(pos, new int[] {10, 24});
    foo(pos, new int[] {11, 24});
    foo(pos, new int[] {12, 24});
    foo(pos, new int[] {13, 24});
    foo(pos, new int[] {14, 24});
    foo(pos, new int[] {15, 24});
    foo(pos, new int[] {16, 24});
    foo(pos, new int[] {17, 24});
    foo(pos, new int[] {18, 24});
    foo(pos, new int[] {19, 24});
    foo(pos, new int[] {20, 24});
    foo(pos, new int[] {21, 24});
    foo(pos, new int[] {22, 24});
    foo(pos, new int[] {23, 24});
    foo(pos, new int[] {24, 25});
    foo(pos, new int[] {24, 26});
    foo(pos, new int[] {24, 27});
    foo(pos, new int[] {24, 28});
    foo(pos, new int[] {24, 29});
    foo(pos, new int[] {24, 30});
    foo(pos, new int[] {24, 31});
    foo(pos, new int[] {24, 32});
    foo(pos, new int[] {24, 33});
    foo(pos, new int[] {24, 34});
    foo(pos, new int[] {24, 35});
    foo(pos, new int[] {24, 36});
    foo(pos, new int[] {0, 13});
    foo(pos, new int[] {1, 13});
    foo(pos, new int[] {2, 13});
    foo(pos, new int[] {3, 13});
    foo(pos, new int[] {4, 13});
    foo(pos, new int[] {5, 13});
    foo(pos, new int[] {6, 13});
    foo(pos, new int[] {7, 13});
    foo(pos, new int[] {8, 13});
    foo(pos, new int[] {9, 13});
    foo(pos, new int[] {10, 13});
    foo(pos, new int[] {11, 13});
    foo(pos, new int[] {12, 13});
    foo(pos, new int[] {13, 14});
    foo(pos, new int[] {13, 15});
    foo(pos, new int[] {13, 16});
    foo(pos, new int[] {13, 17});
    foo(pos, new int[] {13, 18});
    foo(pos, new int[] {13, 19});
    foo(pos, new int[] {13, 20});
    foo(pos, new int[] {13, 21});
    foo(pos, new int[] {13, 22});
    foo(pos, new int[] {13, 23});
    foo(pos, new int[] {13, 24});
    foo(pos, new int[] {13, 25});
    foo(pos, new int[] {13, 26});
    foo(pos, new int[] {13, 27});
    foo(pos, new int[] {13, 28});
    foo(pos, new int[] {13, 29});
    foo(pos, new int[] {13, 30});
    foo(pos, new int[] {13, 31});
    foo(pos, new int[] {13, 32});
    foo(pos, new int[] {13, 33});
    foo(pos, new int[] {13, 34});
    foo(pos, new int[] {13, 35});
    foo(pos, new int[] {13, 36});
    foo(pos, new int[] {0, 3});
    foo(pos, new int[] {1, 3});
    foo(pos, new int[] {2, 3});
    foo(pos, new int[] {3, 4});
    foo(pos, new int[] {3, 5});
    foo(pos, new int[] {3, 6});
    foo(pos, new int[] {3, 7});
    foo(pos, new int[] {3, 8});
    foo(pos, new int[] {3, 9});
    foo(pos, new int[] {3, 10});
    foo(pos, new int[] {3, 11});
    foo(pos, new int[] {3, 12});
    foo(pos, new int[] {3, 13});
    foo(pos, new int[] {3, 14});
    foo(pos, new int[] {3, 15});
    foo(pos, new int[] {3, 16});
    foo(pos, new int[] {3, 17});
    foo(pos, new int[] {3, 18});
    foo(pos, new int[] {3, 19});
    foo(pos, new int[] {3, 20});
    foo(pos, new int[] {3, 21});
    foo(pos, new int[] {3, 22});
    foo(pos, new int[] {3, 23});
    foo(pos, new int[] {3, 24});
    foo(pos, new int[] {3, 25});
    foo(pos, new int[] {3, 26});
    foo(pos, new int[] {3, 27});
    foo(pos, new int[] {3, 28});
    foo(pos, new int[] {3, 29});
    foo(pos, new int[] {3, 30});
    foo(pos, new int[] {3, 31});
    foo(pos, new int[] {3, 32});
    foo(pos, new int[] {3, 33});
    foo(pos, new int[] {3, 34});
    foo(pos, new int[] {3, 35});
    foo(pos, new int[] {3, 36});
    foo(pos, new int[] {0, 10});
    foo(pos, new int[] {1, 10});
    foo(pos, new int[] {2, 10});
    foo(pos, new int[] {3, 10});
    foo(pos, new int[] {4, 10});
    foo(pos, new int[] {5, 10});
    foo(pos, new int[] {6, 10});
    foo(pos, new int[] {7, 10});
    foo(pos, new int[] {8, 10});
    foo(pos, new int[] {9, 10});
    foo(pos, new int[] {10, 11});
    foo(pos, new int[] {10, 12});
    foo(pos, new int[] {10, 13});
    foo(pos, new int[] {10, 14});
    foo(pos, new int[] {10, 15});
    foo(pos, new int[] {10, 16});
    foo(pos, new int[] {10, 17});
    foo(pos, new int[] {10, 18});
    foo(pos, new int[] {10, 19});
    foo(pos, new int[] {10, 20});
    foo(pos, new int[] {10, 21});
    foo(pos, new int[] {10, 22});
    foo(pos, new int[] {10, 23});
    foo(pos, new int[] {10, 24});
    foo(pos, new int[] {10, 25});
    foo(pos, new int[] {10, 26});
    foo(pos, new int[] {10, 27});
    foo(pos, new int[] {10, 28});
    foo(pos, new int[] {10, 29});
    foo(pos, new int[] {10, 30});
    foo(pos, new int[] {10, 31});
    foo(pos, new int[] {10, 32});
    foo(pos, new int[] {10, 33});
    foo(pos, new int[] {10, 34});
    foo(pos, new int[] {10, 35});
    foo(pos, new int[] {10, 36});
    foo(pos, new int[] {0, 33});
    foo(pos, new int[] {1, 33});
    foo(pos, new int[] {2, 33});
    foo(pos, new int[] {3, 33});
    foo(pos, new int[] {4, 33});
    foo(pos, new int[] {5, 33});
    foo(pos, new int[] {6, 33});
    foo(pos, new int[] {7, 33});
    foo(pos, new int[] {8, 33});
    foo(pos, new int[] {9, 33});
    foo(pos, new int[] {10, 33});
    foo(pos, new int[] {11, 33});
    foo(pos, new int[] {12, 33});
    foo(pos, new int[] {13, 33});
    foo(pos, new int[] {14, 33});
    foo(pos, new int[] {15, 33});
    foo(pos, new int[] {16, 33});
    foo(pos, new int[] {17, 33});
    foo(pos, new int[] {18, 33});
    foo(pos, new int[] {19, 33});
    foo(pos, new int[] {20, 33});
    foo(pos, new int[] {21, 33});
    foo(pos, new int[] {22, 33});
    foo(pos, new int[] {23, 33});
    foo(pos, new int[] {24, 33});
    foo(pos, new int[] {25, 33});
    foo(pos, new int[] {26, 33});
    foo(pos, new int[] {27, 33});
    foo(pos, new int[] {28, 33});
    foo(pos, new int[] {29, 33});
    foo(pos, new int[] {30, 33});
    foo(pos, new int[] {31, 33});
    foo(pos, new int[] {32, 33});
    foo(pos, new int[] {33, 34});
    foo(pos, new int[] {33, 35});
    foo(pos, new int[] {33, 36});
    foo(pos, new int[] {0, 8});
    foo(pos, new int[] {1, 8});
    foo(pos, new int[] {2, 8});
    foo(pos, new int[] {3, 8});
    foo(pos, new int[] {4, 8});
    foo(pos, new int[] {5, 8});
    foo(pos, new int[] {6, 8});
    foo(pos, new int[] {7, 8});
    foo(pos, new int[] {8, 9});
    foo(pos, new int[] {8, 10});
    foo(pos, new int[] {8, 11});
    foo(pos, new int[] {8, 12});
    foo(pos, new int[] {8, 13});
    foo(pos, new int[] {8, 14});
    foo(pos, new int[] {8, 15});
    foo(pos, new int[] {8, 16});
    foo(pos, new int[] {8, 17});
    foo(pos, new int[] {8, 18});
    foo(pos, new int[] {8, 19});
    foo(pos, new int[] {8, 20});
    foo(pos, new int[] {8, 21});
    foo(pos, new int[] {8, 22});
    foo(pos, new int[] {8, 23});
    foo(pos, new int[] {8, 24});
    foo(pos, new int[] {8, 25});
    foo(pos, new int[] {8, 26});
    foo(pos, new int[] {8, 27});
    foo(pos, new int[] {8, 28});
    foo(pos, new int[] {8, 29});
    foo(pos, new int[] {8, 30});
    foo(pos, new int[] {8, 31});
    foo(pos, new int[] {8, 32});
    foo(pos, new int[] {8, 33});
    foo(pos, new int[] {8, 34});
    foo(pos, new int[] {8, 35});
    foo(pos, new int[] {8, 36});
    foo(pos, new int[] {0, 34});
    foo(pos, new int[] {1, 34});
    foo(pos, new int[] {2, 34});
    foo(pos, new int[] {3, 34});
    foo(pos, new int[] {4, 34});
    foo(pos, new int[] {5, 34});
    foo(pos, new int[] {6, 34});
    foo(pos, new int[] {7, 34});
    foo(pos, new int[] {8, 34});
    foo(pos, new int[] {9, 34});
    foo(pos, new int[] {10, 34});
    foo(pos, new int[] {11, 34});
    foo(pos, new int[] {12, 34});
    foo(pos, new int[] {13, 34});
    foo(pos, new int[] {14, 34});
    foo(pos, new int[] {15, 34});
    foo(pos, new int[] {16, 34});
    foo(pos, new int[] {17, 34});
    foo(pos, new int[] {18, 34});
    foo(pos, new int[] {19, 34});
    foo(pos, new int[] {20, 34});
    foo(pos, new int[] {21, 34});
    foo(pos, new int[] {22, 34});
    foo(pos, new int[] {23, 34});
    foo(pos, new int[] {24, 34});
    foo(pos, new int[] {25, 34});
    foo(pos, new int[] {26, 34});
    foo(pos, new int[] {27, 34});
    foo(pos, new int[] {28, 34});
    foo(pos, new int[] {29, 34});
    foo(pos, new int[] {30, 34});
    foo(pos, new int[] {31, 34});
    foo(pos, new int[] {32, 34});
    foo(pos, new int[] {33, 34});
    foo(pos, new int[] {34, 35});
    foo(pos, new int[] {34, 36});
    foo(pos, new int[] {0, 29});
    foo(pos, new int[] {1, 29});
    foo(pos, new int[] {2, 29});
    foo(pos, new int[] {3, 29});
    foo(pos, new int[] {4, 29});
    foo(pos, new int[] {5, 29});
    foo(pos, new int[] {6, 29});
    foo(pos, new int[] {7, 29});
    foo(pos, new int[] {8, 29});
    foo(pos, new int[] {9, 29});
    foo(pos, new int[] {10, 29});
    foo(pos, new int[] {11, 29});
    foo(pos, new int[] {12, 29});
    foo(pos, new int[] {13, 29});
    foo(pos, new int[] {14, 29});
    foo(pos, new int[] {15, 29});
    foo(pos, new int[] {16, 29});
    foo(pos, new int[] {17, 29});
    foo(pos, new int[] {18, 29});
    foo(pos, new int[] {19, 29});
    foo(pos, new int[] {20, 29});
    foo(pos, new int[] {21, 29});
    foo(pos, new int[] {22, 29});
    foo(pos, new int[] {23, 29});
    foo(pos, new int[] {24, 29});
    foo(pos, new int[] {25, 29});
    foo(pos, new int[] {26, 29});
    foo(pos, new int[] {27, 29});
    foo(pos, new int[] {28, 29});
    foo(pos, new int[] {29, 30});
    foo(pos, new int[] {29, 31});
    foo(pos, new int[] {29, 32});
    foo(pos, new int[] {29, 33});
    foo(pos, new int[] {29, 34});
    foo(pos, new int[] {29, 35});
    foo(pos, new int[] {29, 36});
    foo(pos, new int[] {0, 35});
    foo(pos, new int[] {1, 35});
    foo(pos, new int[] {2, 35});
    foo(pos, new int[] {3, 35});
    foo(pos, new int[] {4, 35});
    foo(pos, new int[] {5, 35});
    foo(pos, new int[] {6, 35});
    foo(pos, new int[] {7, 35});
    foo(pos, new int[] {8, 35});
    foo(pos, new int[] {9, 35});
    foo(pos, new int[] {10, 35});
    foo(pos, new int[] {11, 35});
    foo(pos, new int[] {12, 35});
    foo(pos, new int[] {13, 35});
    foo(pos, new int[] {14, 35});
    foo(pos, new int[] {15, 35});
    foo(pos, new int[] {16, 35});
    foo(pos, new int[] {17, 35});
    foo(pos, new int[] {18, 35});
    foo(pos, new int[] {19, 35});
    foo(pos, new int[] {20, 35});
    foo(pos, new int[] {21, 35});
    foo(pos, new int[] {22, 35});
    foo(pos, new int[] {23, 35});
    foo(pos, new int[] {24, 35});
    foo(pos, new int[] {25, 35});
    foo(pos, new int[] {26, 35});
    foo(pos, new int[] {27, 35});
    foo(pos, new int[] {28, 35});
    foo(pos, new int[] {29, 35});
    foo(pos, new int[] {30, 35});
    foo(pos, new int[] {31, 35});
    foo(pos, new int[] {32, 35});
    foo(pos, new int[] {33, 35});
    foo(pos, new int[] {34, 35});
    foo(pos, new int[] {35, 36});
    foo(pos, new int[] {0, 18});
    foo(pos, new int[] {1, 18});
    foo(pos, new int[] {2, 18});
    foo(pos, new int[] {3, 18});
    foo(pos, new int[] {4, 18});
    foo(pos, new int[] {5, 18});
    foo(pos, new int[] {6, 18});
    foo(pos, new int[] {7, 18});
    foo(pos, new int[] {8, 18});
    foo(pos, new int[] {9, 18});
    foo(pos, new int[] {10, 18});
    foo(pos, new int[] {11, 18});
    foo(pos, new int[] {12, 18});
    foo(pos, new int[] {13, 18});
    foo(pos, new int[] {14, 18});
    foo(pos, new int[] {15, 18});
    foo(pos, new int[] {16, 18});
    foo(pos, new int[] {17, 18});
    foo(pos, new int[] {18, 19});
    foo(pos, new int[] {18, 20});
    foo(pos, new int[] {18, 21});
    foo(pos, new int[] {18, 22});
    foo(pos, new int[] {18, 23});
    foo(pos, new int[] {18, 24});
    foo(pos, new int[] {18, 25});
    foo(pos, new int[] {18, 26});
    foo(pos, new int[] {18, 27});
    foo(pos, new int[] {18, 28});
    foo(pos, new int[] {18, 29});
    foo(pos, new int[] {18, 30});
    foo(pos, new int[] {18, 31});
    foo(pos, new int[] {18, 32});
    foo(pos, new int[] {18, 33});
    foo(pos, new int[] {18, 34});
    foo(pos, new int[] {18, 35});
    foo(pos, new int[] {18, 36});
    foo(pos, new int[] {0, 28});
    foo(pos, new int[] {1, 28});
    foo(pos, new int[] {2, 28});
    foo(pos, new int[] {3, 28});
    foo(pos, new int[] {4, 28});
    foo(pos, new int[] {5, 28});
    foo(pos, new int[] {6, 28});
    foo(pos, new int[] {7, 28});
    foo(pos, new int[] {8, 28});
    foo(pos, new int[] {9, 28});
    foo(pos, new int[] {10, 28});
    foo(pos, new int[] {11, 28});
    foo(pos, new int[] {12, 28});
    foo(pos, new int[] {13, 28});
    foo(pos, new int[] {14, 28});
    foo(pos, new int[] {15, 28});
    foo(pos, new int[] {16, 28});
    foo(pos, new int[] {17, 28});
    foo(pos, new int[] {18, 28});
    foo(pos, new int[] {19, 28});
    foo(pos, new int[] {20, 28});
    foo(pos, new int[] {21, 28});
    foo(pos, new int[] {22, 28});
    foo(pos, new int[] {23, 28});
    foo(pos, new int[] {24, 28});
    foo(pos, new int[] {25, 28});
    foo(pos, new int[] {26, 28});
    foo(pos, new int[] {27, 28});
    foo(pos, new int[] {28, 29});
    foo(pos, new int[] {28, 30});
    foo(pos, new int[] {28, 31});
    foo(pos, new int[] {28, 32});
    foo(pos, new int[] {28, 33});
    foo(pos, new int[] {28, 34});
    foo(pos, new int[] {28, 35});
    foo(pos, new int[] {28, 36});
    foo(pos, new int[] {0, 36});
    foo(pos, new int[] {1, 36});
    foo(pos, new int[] {2, 36});
    assertThat(pos.size(), is(836));
  }

  private void foo(PartiallyOrderedSet<ImmutableBitSet> s, int[] ints) {
    s.add(ImmutableBitSet.of(ints));
  }
}

// End PartiallyOrderedSetTest.java
