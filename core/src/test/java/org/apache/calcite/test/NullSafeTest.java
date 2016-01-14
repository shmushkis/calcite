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
package org.apache.calcite.test;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@code NullSafeVisitor}.
 */
public class NullSafeTest extends RexProgramTest {
  RelDataType booleanType;
  RelDataType intType;
  RelDataType rowType;
  RexDynamicParam range;
  RexNode aRef;
  RexNode bRef;
  RexNode cRef;
  RexNode dRef;
  RexNode eRef;
  RexNode iRef;
  RexNode jRef;
  RexLiteral trueLit;
  RexLiteral nullLit;
  RexLiteral ten;
  RexLiteral one;

  private void checkNullSafe(RexNode node, String expected) {
    assertThat(RexUtil.nullSafe(rexBuilder, node).toString(),
        equalTo(expected.replaceAll("  +", " ").replace("( ", "(")));
  }

  @Before
  @Override public void setUp() {
    super.setUp();

    booleanType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
    intType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    rowType = typeFactory.builder()
        .add("a", booleanType)
        .add("b", booleanType)
        .add("c", booleanType)
        .add("d", booleanType)
        .add("e", booleanType)
        .add("f", booleanType)
        .add("g", booleanType)
        .add("h", intType)
        .add("i", intType)
        .add("j", intType)
        .build();

    range = rexBuilder.makeDynamicParam(rowType, 0);
    aRef = rexBuilder.makeFieldAccess(range, 0);
    bRef = rexBuilder.makeFieldAccess(range, 1);
    cRef = rexBuilder.makeFieldAccess(range, 2);
    dRef = rexBuilder.makeFieldAccess(range, 3);
    eRef = rexBuilder.makeFieldAccess(range, 4);
    iRef = rexBuilder.makeFieldAccess(range, 8);
    jRef = rexBuilder.makeFieldAccess(range, 9);
    trueLit = rexBuilder.makeLiteral(true);
    nullLit = rexBuilder.constantNull();
    ten = rexBuilder.makeExactLiteral(BigDecimal.TEN);
    one = rexBuilder.makeExactLiteral(BigDecimal.ONE);
  }

  @Test public void testNullSafe() {
    // and: remove duplicates and true
    String expected = ""
        + "CASE(OR(IS NULL(?0.a), IS NULL(?0.b)),"
        + "  CASE(OR(AND(IS NOT NULL(?0.a), NOT(CAST(?0.a):BOOLEAN NOT NULL)),"
        + "    AND(IS NOT NULL(?0.b), NOT(CAST(?0.b):BOOLEAN NOT NULL))),"
        + "    false,"
        + "    null),"
        + "  AND(CAST(?0.a):BOOLEAN NOT NULL, CAST(?0.b):BOOLEAN NOT NULL))";
    checkNullSafe(and(aRef, bRef, aRef, trueLit), expected);

    // and: exploit IS TRUE
    expected = ""
        + "AND(IS NOT NULL(?0.a),"
        + "  CAST(?0.a):BOOLEAN NOT NULL,"
        + "  IS NOT NULL(?0.b),"
        + "  CAST(?0.b):BOOLEAN NOT NULL)";
    checkNullSafe(isTrue(and(aRef, bRef)), expected);

    // not
    checkNullSafe(isTrue(not(aRef)),
        "AND(IS NOT NULL(?0.a), NOT(CAST(?0.a):BOOLEAN NOT NULL))");

    // not
    checkNullSafe(isTrue(not(lt(iRef, ten))),
        "AND(IS NOT NULL(?0.i), >=(CAST(?0.i):INTEGER NOT NULL, 10))");

    // and: remove duplicate "not"s
    expected = ""
        + "AND(IS NOT NULL(?0.a),"
        + "  IS NOT NULL(?0.b),"
        + "  CAST(?0.b):BOOLEAN NOT NULL,"
        + "  IS NOT NULL(?0.c),"
        + "  NOT(CAST(?0.a):BOOLEAN NOT NULL),"
        + "  NOT(CAST(?0.c):BOOLEAN NOT NULL))";
    checkNullSafe(isTrue(and(not(aRef), bRef, not(cRef), not(aRef))), expected);

    // push "isTrue" through and, <
    expected = ""
        + "AND(IS NOT NULL(?0.i), "
        + "OR(<(CAST(?0.i):INTEGER NOT NULL, 10), "
        + "AND(IS NOT NULL(?0.j), "
        + "<(CAST(?0.i):INTEGER NOT NULL, CAST(?0.j):INTEGER NOT NULL))))";
    checkNullSafe(isTrue(or(lt(iRef, ten), lt(iRef, jRef))), expected);

    // combine their "is not null" with our own
    expected = "AND(IS NOT NULL(?0.i), <(CAST(?0.i):INTEGER NOT NULL, 10))";
    checkNullSafe(isTrue(and(isNotNull(iRef), lt(iRef, ten))), expected);

    // and: flatten and remove duplicates
    expected = ""
        + "AND(IS NOT NULL(?0.a),"
        + "  CAST(?0.a):BOOLEAN NOT NULL,"
        + "  IS NOT NULL(?0.b),"
        + "  CAST(?0.b):BOOLEAN NOT NULL,"
        + "  IS NOT NULL(?0.c),"
        + "  IS NOT NULL(?0.d),"
        + "  CAST(?0.d):BOOLEAN NOT NULL,"
        + "  IS NOT NULL(?0.e),"
        + "  NOT(CAST(?0.c):BOOLEAN NOT NULL),"
        + "  NOT(CAST(?0.e):BOOLEAN NOT NULL))";
    checkNullSafe(
        isTrue(
            and(aRef, and(and(bRef, not(cRef), dRef, not(eRef)), not(eRef)))),
        expected);

    // push IS NOT NULL into AND
    checkNullSafe(isNotNull(and(aRef, isNotNull(bRef))),
        "IS NOT NULL(?0.a)");

    // IS NOT TRUE
    expected = ""
        + "CASE(IS NULL(?0.a),"
        + "  true,"
        + "  AND(IS NOT NULL(?0.a),"
        + "    NOT(CAST(?0.a):BOOLEAN NOT NULL)))";
    checkNullSafe(isTrue(isNotTrue(aRef)), expected);

    // IS NOT TRUE
    expected = ""
        + "CASE(IS NULL(?0.i),"
        + "  true,"
        + "  AND(IS NOT NULL(?0.i), >=(CAST(?0.i):INTEGER NOT NULL, 10)))";
    checkNullSafe(isTrue(isNotTrue(lt(iRef, ten))), expected);

    // push IS NOT TRUE into OR
    expected = ""
        + "CASE(OR(IS NULL(?0.a), IS NULL(?0.b)),"
        + "  true,"
        + "  AND(IS NOT NULL(?0.a),"
        + "    IS NOT NULL(?0.b),"
        + "    CAST(?0.b):BOOLEAN NOT NULL,"
        + "    NOT(CAST(?0.a):BOOLEAN NOT NULL)))";
    checkNullSafe(isNotTrue(or(aRef, not(bRef))), expected);

    // case: apply "isTrue" to conditions
    expected = ""
        + "CASE("
        + "  AND(IS NOT NULL(?0.b),"
        + "    IS NOT NULL(?0.c),"
        + "    =(CAST(?0.b):BOOLEAN NOT NULL, CAST(?0.c):BOOLEAN NOT NULL)),"
        + "  AND(IS NOT NULL(?0.d), CAST(?0.d):BOOLEAN NOT NULL),"
        + "  AND(IS NOT NULL(?0.b),"
        + "    IS NOT NULL(?0.d),"
        + "    =(CAST(?0.b):BOOLEAN NOT NULL, CAST(?0.d):BOOLEAN NOT NULL)),"
        + "  AND(IS NOT NULL(?0.b), CAST(?0.b):BOOLEAN NOT NULL),"
        + "  AND(IS NOT NULL(?0.e), CAST(?0.e):BOOLEAN NOT NULL))";
    checkNullSafe(
        isTrue(case_(eq(bRef, cRef), dRef, eq(bRef, dRef), bRef, eRef)),
        expected);

    // case: push "isFalse" into values, apply "isTrue" to conditions
    expected = ""
        + "CASE("
        + "  AND(IS NOT NULL(?0.b),"
        + "    IS NOT NULL(?0.c),"
        + "    =(CAST(?0.b):BOOLEAN NOT NULL, CAST(?0.c):BOOLEAN NOT NULL)),"
        + "  AND(IS NOT NULL(?0.d), NOT(CAST(?0.d):BOOLEAN NOT NULL)),"
        + "  AND(IS NOT NULL(?0.b),"
        + "    IS NOT NULL(?0.d),"
        + "    =(CAST(?0.b):BOOLEAN NOT NULL, CAST(?0.d):BOOLEAN NOT NULL)),"
        + "  AND(IS NOT NULL(?0.b), NOT(CAST(?0.b):BOOLEAN NOT NULL)),"
        + "  AND(IS NOT NULL(?0.e), NOT(CAST(?0.e):BOOLEAN NOT NULL)))";
    checkNullSafe(
        isFalse(case_(eq(bRef, cRef), dRef, eq(bRef, dRef), bRef, eRef)),
        expected);

    // case: make sure returned values are all of same type
    checkNullSafe(case_(aRef, nullLit, trueLit),
        "CASE(AND(IS NOT NULL(?0.a), CAST(?0.a):BOOLEAN NOT NULL), null, CAST(true):BOOLEAN)");
  }

  @Test public void testNullSafeCase() {
    // case: if we have checked that values are not null, either in the current
    // predicate or previous predicates that evaluated to false, do not check
    // again
    String expected = ""
        + "CASE(AND(IS NOT NULL(?0.i), >(CAST(?0.i):INTEGER NOT NULL, 10)),"
        + "  CASE(IS NULL(?0.j), null, +(CAST(?0.j):INTEGER NOT NULL, 1)),"
        + "  IS NULL(?0.j),"
        + "  CAST(1):INTEGER,"
        + "  AND(IS NOT NULL(?0.i), <(CAST(?0.i):INTEGER NOT NULL, 1)),"
        + "  +(CAST(?0.j):INTEGER NOT NULL, CAST(?0.i):INTEGER NOT NULL),"
        + "  CAST(10):INTEGER)";
    checkNullSafe(
        case_(gt(iRef, ten), plus(jRef, one), // j may be null at this point
            isNull(jRef), one,
            lt(iRef, one), plus(jRef, iRef), // we know i and j are not null
            ten),
        expected);
  }

  @Test public void testNullSafe2() {
    checkNullSafe(isTrue(or(lt(iRef, ten), isNotNull(iRef))),
        "IS NOT NULL(?0.i)");

    // OR without IS TRUE
    String expected = ""
        + "CASE(IS NULL(?0.i),"
        + "  CASE(IS NULL(?0.a),"
        + "    null,"
        + "    CAST(?0.a):BOOLEAN NOT NULL, true, null),"
        + "  <(CAST(?0.i):INTEGER NOT NULL, 10),"
        + "  true,"
        + "  ?0.a)";
    checkNullSafe(or(lt(iRef, ten), aRef), expected);

    expected = "AND(IS NOT NULL(?0.i), <(CAST(?0.i):INTEGER NOT NULL, 10))";
    checkNullSafe(isTrue(and(lt(iRef, ten), isNotNull(iRef))), expected);

    // After we've generated "iRef IS NULL OR ..." we know that iRef is not
    // null, so we don't need to check again.
    expected = "OR(IS NULL(?0.i),  <(CAST(?0.i):INTEGER NOT NULL, 10))";
    checkNullSafe(isTrue(or(isNull(iRef), lt(iRef, ten))), expected);

    expected = "AND(IS NOT NULL(?0.i), <(ABS(CAST(?0.i):INTEGER NOT NULL), 10))";
    checkNullSafe(isTrue(lt(abs(iRef), ten)), expected);

    expected = "CASE(IS NULL(?0.i), null, <(ABS(CAST(?0.i):INTEGER NOT NULL), 10))";
    checkNullSafe(lt(abs(iRef), ten), expected);

    checkNullSafe(lt(abs(nullLit), ten), "null");

    expected = "CASE(IS NULL(?0.i), null, ABS(+(10, CAST(?0.i):INTEGER NOT NULL)))";
    checkNullSafe(abs(plus(ten, iRef)), expected);

    expected = "CASE(IS NULL(?0.i), null,"
        + "  CAST(CAST(?0.i):INTEGER NOT NULL):JavaType(double) NOT NULL)";
    checkNullSafe(cast(iRef, typeFactory.createType(Double.class)), expected);

    expected =
        "CASE(AND(IS TRUE(?0.a), IS NOT NULL(?0.i)), true, IS NULL(?0.j))";
    checkNullSafe(or(and(isTrue(aRef), isNotNull(iRef)), isNull(jRef)),
        expected);
    checkNullSafe(isTrue(or(and(isTrue(aRef), isNotNull(iRef)), isNull(jRef))),
        expected);
  }
}

// End NullSafeTest.java
