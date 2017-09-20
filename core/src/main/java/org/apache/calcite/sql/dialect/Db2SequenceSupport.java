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
 * Sequence support that retrieves sequence definitions from the IBM DB2 system
 * catalog.
 *
 * <p>See
 * <a href="https://www.ibm.com/support/knowledgecenter/en/SSEPGG_10.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0004203.html">documentation</a>.
 */
public class Db2SequenceSupport extends SequenceSupportImpl {
  public static final SqlDialect.SequenceSupport INSTANCE =
      new Db2SequenceSupport();

  private Db2SequenceSupport() {
    super("select NULL, SEQSCHEMA, SEQNAME,\n"
            + " CASE WHEN PRECISION = 19 THEN 'BIGINT' ELSE 'INT' END,\n"
            + "INCREMENT from SYSCAT.SEQUENCES where 1=1",
        null,
        " and SEQSCHEMA = ?");
  }

  @Override public void unparseSequenceVal(SqlWriter writer, SqlKind kind,
      SqlNode sequenceNode) {
    // For reference also see:
    // https://www.ibm.com/support/knowledgecenter/en/SSEPEK_10.0.0/sqlref/
    // src/tpc/db2z_sequencereference.html
    writer.sep(kind == SqlKind.NEXT_VALUE ? "NEXT VALUE FOR" : "PREVIOUS VALUE FOR");
    sequenceNode.unparse(writer, 0, 0);
  }
}

// End Db2SequenceSupport.java
