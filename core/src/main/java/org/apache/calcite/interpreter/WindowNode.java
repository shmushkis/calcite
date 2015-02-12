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

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Window}.
 */
public class WindowNode extends AbstractSingleNode<Window> {
  private final ImmutableList<Win> wins;

  WindowNode(Interpreter interpreter, Window rel) {
    super(interpreter, rel);

    final DataContext dataContext = interpreter.getDataContext();
    final ImmutableList.Builder<Win> wins = ImmutableList.builder();
    for (Window.Group group : rel.groups) {
      for (Window.RexWinAggCall rexWinAggCall : group.aggCalls) {
        final SqlAggFunction op = (SqlAggFunction) rexWinAggCall.getOperator();
        final ImmutableList<Integer> operands = foo(rexWinAggCall.operands);
        AggregateCall aggCall =
            new AggregateCall(op, false, operands, rexWinAggCall.getType(),
                null);
        wins.add(
            new Win(group, rexWinAggCall,
                AggregateNode.getAccumulator(aggCall, rel, dataContext)));
      }
    }
    this.wins = wins.build();
  }

  private ImmutableList<Integer> foo(ImmutableList<RexNode> operands) {
    return ImmutableList.of(); // TODO:
  }

  public void run() throws InterruptedException {
    Row row;
    final List<Row> rows = Lists.newArrayList();
    while ((row = source.receive()) != null) {
      rows.add(row);
    }

    final Object[] values2 = new Object[rel.getRowType().getFieldCount()];
    for (int ordinal = 0; ordinal < rows.size(); ordinal++) {
      row = rows.get(ordinal);
      final Object[] values = row.getValues();
      System.arraycopy(values, 0, values2, 0, values.length);
      int i = values.length;
      for (Win win : wins) {
        values2[i++] = win.compute(row, ordinal, rows);
      }
      sink.send(Row.asCopy(values2));
    }
    sink.end();
  }

  /** Compiled windowed aggregate function */
  private static class Win {
    private final Window.Group group;
    private final Window.RexWinAggCall aggCall;
    private final AggregateNode.AccumulatorFactory accumulatorFactory;

    public Win(Window.Group group, Window.RexWinAggCall aggCall,
        AggregateNode.AccumulatorFactory accumulatorFactory) {
      this.group = group;
      this.aggCall = aggCall;
      this.accumulatorFactory = accumulatorFactory;
    }

    public Object compute(Row row, int ordinal, List<Row> rows) {
      // First, create a partition
      final List<Row> partitionRows;
      final int partitionOrdinal;
      if (group.keys.isEmpty()) {
        partitionRows = rows;
        partitionOrdinal = ordinal;
      } else {
        partitionRows = Lists.newArrayList();
        int k = -1;
        for (int i = 0; i < rows.size(); i++) {
          Row row1 = rows.get(i);
          if (match(row, row1, group.keys)) {
            partitionRows.add(row1);
            if (i == ordinal) {
              k = i;
            }
          }
        }
        partitionOrdinal = k;
      }

      // Next, narrow down the partition
      final List<Row> windowRows = partitionRows; // TODO:

      // Last, compute the aggregate function

      return null;
    }

    private boolean match(Row row0, Row row1, ImmutableBitSet keys) {
      for (int key : keys) {
        if (!Objects.equals(row0.getObject(key), row1.getObject(key))) {
          return false;
        }
      }
      return true;
    }
  }
}

// End WindowNode.java
