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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * The name-resolution scope of virtual columns in an INSERT statement.
 * The objects visible are the columns of the source table.
 */
public class InsertScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlInsert insert;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a scope corresponding to an INSERT clause.
   *
   * @param parent    Parent scope, must not be null
   * @param insert    INSERT statement
   */
  InsertScope(SqlValidatorScope parent, SqlInsert insert) {
    super(parent);
    this.insert = insert;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlCall getNode() {
    return insert;
  }

  @Override public void resolve(List<String> names, SqlNameMatcher nameMatcher,
      boolean deep, Resolved resolved) {
    if (names.size() >= 1) {
      final SqlValidatorNamespace ns =
          validator.getNamespace(insert.getTargetTable());
      if (nameMatcher.field(ns.getRowType(), names.get(0)) != null) {
        resolved.found(ns, false, this, null, Util.skip(names));
        return;
      }
    }
    super.resolve(names, nameMatcher, deep, resolved);
  }

  @Override public SqlQualified fullyQualify(SqlIdentifier identifier) {
    final SqlValidatorNamespace ns =
        validator.getNamespace(insert.getTargetTable());
    return SqlQualified.create(this, 0, ns, identifier);
  }
}

// End InsertScope.java
