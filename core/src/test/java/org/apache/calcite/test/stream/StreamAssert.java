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
package org.apache.calcite.test.stream;

import java.util.HashMap;
import java.util.Map;

/**
 * Context for streaming tests.
 */
public class StreamAssert {
  /** Creates a StreamAssert. */
  public static StreamAssert with() {
    return new StreamAssert();
  }

  /** Binds a SQL statement to a StreamAssert. */
  public Sql sql(String sql) {
    return new Sql(sql);
  }

  /** Fluent API for testing streaming SQL. */
  public static class Sql {
    protected final String sql;
    protected final Map<Integer, String> inputs = new HashMap<>();

    public Sql(String sql) {
      this.sql = sql;
    }

    /** Puts a row into a given input stream. */
    public Sql send(int input, Object... row) {
      return this;
    }

    /** Tries to read a row from the query. */
    public Sql expect(Object... row) {
      return this;
    }

    /** Terminates executing the query, expecting no more rows. */
    public Sql end() {
      return this;
    }

    /** Defines an input stream, */
    public Sql input(int ordinal, String name) {
      inputs.put(ordinal, name);
      return this;
    }
  }
}

// End StreamAssert.java
