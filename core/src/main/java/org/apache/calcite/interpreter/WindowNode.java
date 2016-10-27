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
package org.apache.calcite.interpreter;

import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;

import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Window}.
 */
public class WindowNode extends AbstractSingleNode<Window> {
  private final Scalar scalar;
  private final Context context;
  private final List<Action> beforeActions = new ArrayList<>();
  private final List<Action> afterActions = new ArrayList<>();
  private List<GroupImpl> groups = new ArrayList<>();

  WindowNode(Interpreter interpreter, Window rel) {
    super(interpreter, rel);
    this.context = interpreter.createContext();
    final int inputCount = rel.getInput().getRowType().getFieldCount();
    int i = inputCount;
    for (Window.Group group : rel.groups) {
      groups.add(new GroupImpl(group, i));
      i += group.aggCalls.size();
    }
    this.scalar = new Scalar() {
      public Object execute(Context context) {
        throw new UnsupportedOperationException();
      }

      public void execute(Context context, Object[] results) {
        System.arraycopy(context.values, 0, results, 0, inputCount);
        for (GroupImpl group : groups) {
          final Comparable key = group.key(context);
          final Deque<Object[]> deque = group.map.get(key);
          for (AggCallImpl call : group.calls) {
            results[call.output] = call.compute(deque);
          }
        }
      }
    };

    for (final GroupImpl group : groups) {
      this.beforeActions.add(
          new Action() {
            public void apply(Context context) {
              final Comparable key = group.key(context);
              Deque<Object[]> deque = group.map.get(key);
              if (deque == null) {
                deque = new ArrayDeque<>();
                group.map.put(key, deque);
              }
              deque.addLast(context.values);
            }
          });
      final RexNode offset = group.group.lowerBound.getOffset();
      if (offset instanceof RexInputRef) {
        final RexLiteral literal =
            rel.constants.get(((RexInputRef) offset).getIndex() - inputCount);
        final int intOffset = ((Number) literal.getValue()).intValue();
        this.afterActions.add(
            new Action() {
              public void apply(Context context) {
                final Comparable key = group.key(context);
                final Deque deque = group.map.get(key);
                if (deque != null) {
                  if (deque.size() >= intOffset) {
                    deque.removeFirst();
                  }
                }
              }
            });
      }
    }
  }

  public void run() throws InterruptedException {
    final int projectCount = rel.getRowType().getFieldCount();
    Row row;
    while ((row = source.receive()) != null) {
      context.values = row.getValues();
      for (Action action : beforeActions) {
        action.apply(context);
      }
      Object[] values = new Object[projectCount];
      scalar.execute(context, values);
      sink.send(new Row(values));
      for (Action action : afterActions) {
        action.apply(context);
      }
    }
    sink.end();
  }

  /** Action that may be applied before or after seeing a row, to maintain
   * internal state. */
  interface Action {
    void apply(Context context);
  }

  /** Mechanism to implement a group (a collection of windowed aggregate
   * functions that have the same window definition). */
  private static class GroupImpl {
    final Window.Group group;
    final Map<Comparable, Deque<Object[]>> map = new HashMap<>();
    final List<AggCallImpl> calls = new ArrayList<>();
    final List<Integer> keys;

    GroupImpl(Window.Group group, int output) {
      this.group = group;
      this.keys = ImmutableList.copyOf(group.keys.asList());
      for (Window.RexWinAggCall aggCall : group.aggCalls) {
        calls.add(new AggCallImpl(aggCall, output++));
      }
    }

    Comparable key(Context context) {
      switch (keys.size()) {
      case 0:
        return FlatLists.of();
      case 1:
        return (Comparable) context.values[keys.get(0)];
      default:
        throw new UnsupportedOperationException("TODO: " + keys);
      }
    }
  }

  /** Mechanism to implement an aggregate function. */
  private static class AggCallImpl {
    private final Window.RexWinAggCall aggCall;
    private final int output;
    private final int operand;

    AggCallImpl(Window.RexWinAggCall aggCall, int output) {
      this.aggCall = aggCall;
      this.output = output;
      this.operand = aggCall.operands.size() == 1
          && aggCall.operands.get(0) instanceof RexInputRef
          ? ((RexInputRef) aggCall.operands.get(0)).getIndex()
          : -1;
    }

    private Object compute(Deque<Object[]> deque) {
      switch (aggCall.op.getKind()) {
      case COUNT:
        if (operand < 0) {
          return deque.size();
        } else {
          int c = 0;
          for (Object[] row : deque) {
            if (row[operand] != null) {
              ++c;
            }
          }
          return c;
        }
      case SUM0:
        long sum = 0;
        for (Object[] row : deque) {
          Number v = (Number) row[operand];
          if (v != null) {
            sum += v.longValue();
          }
        }
        return sum;
      default:
        throw new AssertionError("TODO: " + aggCall);
      }
    }
  }
}

// End WindowNode.java
