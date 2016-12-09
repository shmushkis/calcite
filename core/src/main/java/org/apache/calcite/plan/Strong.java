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
package org.apache.calcite.plan;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/** Utilities for strong predicates.
 *
 * <p>A predicate is strong (or null-rejecting) if it is UNKNOWN if any of its
 * inputs is UNKNOWN.</p>
 *
 * <p>By the way, UNKNOWN is just the boolean form of NULL.</p>
 *
 * <p>Examples:</p>
 * <ul>
 *   <li>{@code UNKNOWN} is strong in [] (definitely null)
 *   <li>{@code c = 1} is strong in [c] (definitely null if and only if c is
 *   null)
 *   <li>{@code c IS NULL} is not strong (always returns TRUE or FALSE, never
 *   null)
 *   <li>{@code p1 AND p2} is strong in [p1, p2] (definitely null if either p1
 *   is null or p2 is null)
 *   <li>{@code p1 OR p2} is strong if p1 and p2 are strong
 *   <li>{@code c1 = 1 OR c2 IS NULL} is strong in [c1] (definitely null if c1
 *   is null)
 * </ul>
 */
public class Strong {
  /** Returns a checker that consults a bit set to find out whether particular
   * inputs may be null. */
  public static Strong of(final ImmutableBitSet nullColumns) {
    return new Strong() {
      @Override public boolean isNull(RexInputRef ref) {
        return nullColumns.get(ref.getIndex());
      }
    };
  }

  /** Returns whether the analyzed expression will definitely return null if
   * all of a given set of input columns are null. */
  public static boolean isNull(RexNode node, ImmutableBitSet nullColumns) {
    return of(nullColumns).isNull(node);
  }

  /** Returns whether an operator always returns null if any of its arguments
   * is null. */
  public static boolean inAllOperands(SqlOperator op) {
    switch (op.kind) {
    case IS_DISTINCT_FROM:
    case IS_NOT_DISTINCT_FROM:
    case IS_NULL:
    case IS_NOT_NULL:
    case IS_TRUE:
    case IS_NOT_TRUE:
    case IS_FALSE:
    case IS_NOT_FALSE:
    case NULLIF: // not strict: NULLIF(1, NULL) yields 1
    case COALESCE: // not strict: COALESCE(NULL, 2) yields 2
    case AND: // not strict: FALSE OR NULL yields FALSE
    case OR: // not strict: TRUE OR NULL yields TRUE
    case OTHER:
    case OTHER_FUNCTION:
      return false;
    default:
      return true;
    }
  }

  /** Returns whether an expression is definitely null.
   *
   * <p>The answer is based on calls to {@link #isNull} for its constituent
   * expressions, and you may override methods to test hypotheses such as
   * "if {@code x} is null, is {@code x + y} null? */
  public boolean isNull(RexNode node) {
    switch (node.getKind()) {
    case LITERAL:
      return ((RexLiteral) node).getValue() == null;
    case IS_TRUE:
    case IS_NOT_NULL:
    case AND:
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case PLUS_PREFIX:
    case MINUS_PREFIX:
    case PLUS:
    case MINUS:
    case TIMES:
    case DIVIDE:
      return anyNull(((RexCall) node).getOperands());
    case OR:
      return allNull(((RexCall) node).getOperands());
    case INPUT_REF:
      return isNull((RexInputRef) node);
    default:
      return false;
    }
  }

  /** Returns whether a given input is definitely null. */
  public boolean isNull(RexInputRef ref) {
    return false;
  }

  /** Returns whether all expressions in a list are definitely null. */
  private boolean allNull(List<RexNode> operands) {
    for (RexNode operand : operands) {
      if (!isNull(operand)) {
        return false;
      }
    }
    return true;
  }

  /** Returns whether any expressions in a list are definitely null. */
  private boolean anyNull(List<RexNode> operands) {
    for (RexNode operand : operands) {
      if (isNull(operand)) {
        return true;
      }
    }
    return false;
  }
}

// End Strong.java
