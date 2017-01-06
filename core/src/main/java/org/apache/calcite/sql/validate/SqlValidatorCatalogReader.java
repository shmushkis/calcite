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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlIdentifier;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

/**
 * Supplies catalog information for {@link SqlValidator}.
 *
 * <p>This interface only provides a thin API to the underlying repository, and
 * this is intentional. By only presenting the repository information of
 * interest to the validator, we reduce the dependency on exact mechanism to
 * implement the repository. It is also possible to construct mock
 * implementations of this interface for testing purposes.
 */
public interface SqlValidatorCatalogReader {
  //~ Methods ----------------------------------------------------------------

  /**
   * Finds a table or schema with the given name, possibly qualified.
   *
   * <p>The name matcher is not null, and one typically uses
   * {@link #nameMatcher()}.
   *
   * @param names Name of table, may be qualified or fully-qualified
   * @param nameMatcher Name matcher
   */
  void resolve(List<String> names, SqlNameMatcher nameMatcher,
      SqlValidatorScope.Path path, Resolved resolved);

  @Deprecated // to be removed before 2.0
  SqlValidatorTable getTable(List<String> names, SqlNameMatcher nameMatcher);

  /**
   * Finds a user-defined type with the given name, possibly qualified.
   *
   * <p>NOTE jvs 12-Feb-2005: the reason this method is defined here instead
   * of on RelDataTypeFactory is that it has to take into account
   * context-dependent information such as SQL schema path, whereas a type
   * factory is context-independent.
   *
   * @param typeName Name of type
   * @return named type, or null if not found
   */
  RelDataType getNamedType(SqlIdentifier typeName);

  /**
   * Given fully qualified schema name, returns schema object names as
   * specified. They can be schema, table, function, view.
   * When names array is empty, the contents of root schema should be returned.
   *
   * @param names the array contains fully qualified schema name or empty
   *              list for root schema
   * @return the list of all object (schema, table, function,
   *         view) names under the above criteria
   */
  List<SqlMoniker> getAllSchemaObjectNames(List<String> names);

  /**
   * Returns the name of the current schema.
   *
   * @return name of the current schema
   */
  List<String> getSchemaName();

  /** @deprecated Use
   * {@link #nameMatcher()}.{@link SqlNameMatcher#field(RelDataType, String)} */
  @Deprecated // to be removed before 2.0
  RelDataTypeField field(RelDataType rowType, String alias);

  /** Returns an implementation of
   * {@link org.apache.calcite.sql.validate.SqlNameMatcher}
   * that matches the case-sensitivity policy. */
  SqlNameMatcher nameMatcher();

  /** @deprecated Use
   * {@link #nameMatcher()}.{@link SqlNameMatcher#matches(String, String)} */
  @Deprecated // to be removed before 2.0
  boolean matches(String string, String name);

  RelDataType createTypeFromProjection(RelDataType type,
      List<String> columnNameList);

  /** @deprecated Use
   * {@link #nameMatcher()}.{@link SqlNameMatcher#isCaseSensitive()} */
  @Deprecated // to be removed before 2.0
  boolean isCaseSensitive();

  /** Callback from {@link SqlValidatorCatalogReader#resolve}. */
  interface Resolved {
    void table(RelOptTable table, SqlValidatorScope.Path path,
        List<String> remainingNames);
    void schema(Schema schema, SqlValidatorScope.Path path,
        List<String> remainingNames);
    int count();
  }

  /** Default implementation of {@link SqlValidatorCatalogReader.Resolved}. */
  class ResolvedImpl implements Resolved {
    final List<Resolve> resolves = new ArrayList<>();

    public void table(RelOptTable table, SqlValidatorScope.Path path,
        List<String> remainingNames) {
      resolves.add(new Resolve(table, null, path, remainingNames));
    }

    public void schema(Schema schema, SqlValidatorScope.Path path,
        List<String> remainingNames) {
      resolves.add(new Resolve(null, schema, path, remainingNames));
    }

    public int count() {
      return resolves.size();
    }

    public Resolve only() {
      return Iterables.getOnlyElement(resolves);
    }

    /** Resets all state. */
    public void clear() {
      resolves.clear();
    }

  }

  /** A match found when looking up a name. */
  class Resolve {
    public final RelOptTable table;
    public final Schema schema;
    public final SqlValidatorScope.Path path;
    /** Empty if it was a full match. */
    public final List<String> remainingNames;

    Resolve(RelOptTable table, Schema schema, SqlValidatorScope.Path path,
        List<String> remainingNames) {
      this.table = table;
      this.schema = schema;
      Preconditions.checkArgument((table == null) ^ (schema == null));
      this.path = Preconditions.checkNotNull(path);
      this.remainingNames = remainingNames == null ? ImmutableList.<String>of()
          : ImmutableList.copyOf(remainingNames);
    }
  }
}

// End SqlValidatorCatalogReader.java
