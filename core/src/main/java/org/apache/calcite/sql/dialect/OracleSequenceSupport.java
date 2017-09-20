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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

/**
 * Sequence support that retrieves sequence definitions using Oracle's
 * {@code all_objects} view.
 */
public class OracleSequenceSupport extends SequenceSupportImpl {
  public static final SqlDialect.SequenceSupport INSTANCE =
      new OracleSequenceSupport();

  private OracleSequenceSupport() {
    super("select NULL, o.sequence_owner, o.sequence_name, 'DECIMAL',"
        + " o.increment_by from all_sequences o where 1=1",
        null,
        " and o.sequence_owner = ?");
  }

  /** {@inheritDoc}
   *
   * <p>Uses
   * <a href="https://docs.oracle.com/cd/B19306_01/server.102/b14200/pseudocolumns002.htm">
   * pseudo-columns</a>.
   */
  @Override public void unparseSequenceVal(SqlWriter writer, SqlKind kind,
      SqlNode sequenceNode) {
    sequenceNode.unparse(writer, 0, 0);
    writer.sep(kind == SqlKind.NEXT_VALUE ? ".NEXTVAL" : ".CURRVAL");
  }
}

// End OracleSequenceSupport.java
