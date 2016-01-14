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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Takes a tree of {@link RexNode} objects and transforms it into another in one
 * sense equivalent tree. Nodes in tree will be modified and hence tree will not
 * remain unchanged.
 *
 * <p>NOTE: You must validate the tree of RexNodes before using this class.
 */
public class RexTransformer {
  //~ Instance fields --------------------------------------------------------

  private RexNode root;
  private final RexBuilder rexBuilder;
  private int isParentsCount;

  // NOTE: The OR operator is NOT missing. See RexTransformerTest.
  private final Set<SqlOperator> transformableOperators =
      ImmutableSet.<SqlOperator>of(SqlStdOperatorTable.AND,
          SqlStdOperatorTable.EQUALS,
          SqlStdOperatorTable.NOT_EQUALS,
          SqlStdOperatorTable.GREATER_THAN,
          SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          SqlStdOperatorTable.LESS_THAN,
          SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

  //~ Constructors -----------------------------------------------------------

  public RexTransformer(
      RexNode root,
      RexBuilder rexBuilder) {
    this.root = root;
    this.rexBuilder = rexBuilder;
    isParentsCount = 0;
  }

  //~ Methods ----------------------------------------------------------------

  private boolean isBoolean(RexNode node) {
    RelDataType type = node.getType();
    return SqlTypeUtil.inBooleanFamily(type);
  }

  private boolean isNullable(RexNode node) {
    return node.getType().isNullable();
  }

  private boolean isTransformable(RexNode node) {
    if (0 == isParentsCount) {
      return false;
    }

    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      return !transformableOperators.contains(
          call.getOperator())
          && isNullable(node);
    }
    return isNullable(node);
  }

  public RexNode transformNullSemantics() {
    root = transformNullSemantics(root);
    return root;
  }

  private RexNode transformNullSemantics(RexNode node) {
    assert isParentsCount >= 0 : "Cannot be negative";
    if (!isBoolean(node)) {
      return node;
    }

    final Boolean directlyUnderIs;
    switch (node.getKind()) {
    case IS_TRUE:
      directlyUnderIs = Boolean.TRUE;
      isParentsCount++;
      break;
    case IS_FALSE:
      directlyUnderIs = Boolean.FALSE;
      isParentsCount++;
      break;
    default:
      directlyUnderIs = null;
    }

    // Special case when we have a Literal, Parameter or Identifier directly
    // as an operand to IS TRUE or IS FALSE.
    if (directlyUnderIs != null) {
      final RexCall call = (RexCall) node;
      assert isParentsCount > 0 : "Stack should not be empty";
      assert 1 == call.operands.size();
      RexNode operand = call.operands.get(0);
      if (operand instanceof RexLiteral
          || operand instanceof RexInputRef
          || operand instanceof RexDynamicParam) {
        if (isNullable(node)) {
          final RexNode notNullNode =
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand);
          final RexNode boolNode = rexBuilder.makeLiteral(directlyUnderIs);
          final RexNode eqNode =
              rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, operand,
                  boolNode);
          return rexBuilder.makeCall(SqlStdOperatorTable.AND, notNullNode,
              eqNode);
        } else {
          final RexNode boolNode = rexBuilder.makeLiteral(directlyUnderIs);
          return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, node,
              boolNode);
        }
      }

      // else continue as normal
    }

    if (node instanceof RexCall) {
      final RexCall call = (RexCall) node;

      // Transform children (if any) before transforming node itself.
      final List<RexNode> operands = new ArrayList<>();
      for (RexNode operand : call.operands) {
        operands.add(transformNullSemantics(operand));
      }

      if (directlyUnderIs != null) {
        isParentsCount--;
        return operands.get(0);
      }

      if (transformableOperators.contains(call.getOperator())) {
        assert 2 == operands.size();

        final RexNode isNotNullOne;
        if (isTransformable(operands.get(0))) {
          isNotNullOne =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.IS_NOT_NULL,
                  operands.get(0));
        } else {
          isNotNullOne = null;
        }

        final RexNode isNotNullTwo;
        if (isTransformable(operands.get(1))) {
          isNotNullTwo =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.IS_NOT_NULL,
                  operands.get(1));
        } else {
          isNotNullTwo = null;
        }

        RexNode intoFinalAnd = null;
        if (isNotNullOne != null && isNotNullTwo != null) {
          intoFinalAnd =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.AND,
                  isNotNullOne,
                  isNotNullTwo);
        } else if (isNotNullOne != null) {
          intoFinalAnd = isNotNullOne;
        } else if (isNotNullTwo != null) {
          intoFinalAnd = isNotNullTwo;
        }

        if (intoFinalAnd != null) {
          return rexBuilder.makeCall(SqlStdOperatorTable.AND,
              intoFinalAnd,
              call.clone(call.getType(), operands));
        }

        // if come here no need to do anything
      }

      if (!operands.equals(call.operands)) {
        return call.clone(call.getType(), operands);
      }
    }

    return node;
  }
}

// End RexTransformer.java
