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

import org.apache.calcite.rel.core.Project;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.logical.LogicalFilter}.
 */
public class ProjectNode implements Node {
  private final Scalar projects;
  private final Source source;
  private final Sink sink;
  private final Context context;
  private final int projectCount;

  public ProjectNode(Interpreter interpreter, Project rel) {
    this.projectCount = rel.getProjects().size();
    this.projects = interpreter.compile(rel.getProjects(), rel.getInputs());
    this.source = interpreter.source(rel, 0);
    this.sink = interpreter.sink(rel);
    this.context = interpreter.createContext();
  }

  public void run() throws InterruptedException {
    Row row;
    while ((row = source.receive()) != null) {
      context.values = row.getValues();
      Object[] values = new Object[projectCount];
      projects.execute(context, values);
      sink.send(new Row(values));
    }
  }
}

// End ProjectNode.java
