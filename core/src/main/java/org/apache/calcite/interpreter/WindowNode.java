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

import java.util.ArrayList;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Window}.
 */
public class WindowNode extends AbstractSingleNode<Window> {
  private final Scalar scalar;
  private final Context context;
  private final int projectCount;
  private final List<Action> beforeActions = new ArrayList<>();
  private final List<Action> afterActions = new ArrayList<>();

  WindowNode(Interpreter interpreter, Window rel) {
    super(interpreter, rel);
    projectCount = rel.getRowType().getFieldCount();
    this.context = interpreter.createContext();
    this.scalar = null;
  }

  public void run() throws InterruptedException {
    Row row;
    while ((row = source.receive()) != null) {
      context.values = row.getValues();
      for (Action action : beforeActions) {
        action.apply(context);
      }
      Object[] values = new Object[projectCount];
      scalar.execute(context, values);
      sink.send(new Row(values));
      for (Action action : beforeActions) {
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
}

// End WindowNode.java
