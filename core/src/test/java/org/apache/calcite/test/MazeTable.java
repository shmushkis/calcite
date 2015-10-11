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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Maze generator.
 */
public class MazeTable extends AbstractTable implements ScannableTable {
  final int width;

  private MazeTable(int width, int height, int seed) {
    this.width = width;
  }

  public static ScannableTable generate(int width, int height, int seed) {
    return new MazeTable(width, 10, 1);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("S", SqlTypeName.VARCHAR, width * 2 + 1)
        .build();
  }

  public Enumerable<Object[]> scan(DataContext root) {
    Object[][] rows = {{"abc"}, {"def"}};
    return Linq4j.asEnumerable(rows);
  }
/*
    final Random random = seed >= 0 ? new Random(seed) : new Random();
    final Maze maze = new Maze(width, height);
    final PrintWriter pw = new PrintWriter(out);
    maze.layout(random, pw);
    Set<Integer> solution = null;
    if (showSolution) {
        solution =
          true
           ? maze.solve(0, 0)
           : maze.solve(width / 2, height / 2);
    }
    int[] distances = null;
    if (showDistance) {
        distances = maze.distance(solution);
    }
    maze.printHtml(style, buf, solution, distances);
}
%>

<html>
<head>
<style type="text/css">
<%= style %>
</style>
</head>
<body>
<form action="maze.jsp">
<p>Maze dimensions:
<input name="width" size="4" value="<%= widthDefault %>"/>
wide x
<input name="height" size="4" value="<%= heightDefault %>"/>
high.
<input name="solution" type="checkbox" <%= showSolution ? "checked=checked" : "" %> />
Show solution.
<input name="distance" type="checkbox" <%= showDistance ? "checked=checked" : "" %> />
Show distances.
<input name="seed" type="hidden" value="<%= seed %>" />
<input type="submit" value="Generate maze"/></p>
</form>
<%= buf %>
</body>
</html>
     */
}

// End MazeTable.java
