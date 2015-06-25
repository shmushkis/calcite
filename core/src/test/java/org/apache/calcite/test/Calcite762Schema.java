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

/** Schema for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-762">[CALCITE-762]
 * Trailing spaces on character literals</a>. */
public class Calcite762Schema {
  public String toString() {
    return "Calcite762Schema";
  }

  // CHECKSTYLE: IGNORE 1
  public final Table1[] T1 = {
    new Table1(0, "null value", null),
    new Table1(1, "a", "aa"),
    new Table1(2, "noBlank", ""),
    new Table1(3, "oneBlank", " "),
    new Table1(4, "ltrOneBlank", "aa "),
  };

  /** Record of table T1. */
  public static class Table1 {
    // CHECKSTYLE: IGNORE 3
    public final int ID;
    public final String V1;
    public final String V2;

    public Table1(int id, String v1, String v2) {
      this.ID = id;
      this.V1 = v1;
      this.V2 = v2;
    }

    public String toString() {
      return "Table1 [ID: " + ID
          + ", V1: " + V1
          + ", V2: " + V2 + "]";
    }
  }
}

// End Calcite762Schema.java
