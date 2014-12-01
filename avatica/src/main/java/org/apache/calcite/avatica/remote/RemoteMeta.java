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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaPrepareResult;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link Meta} for the remote driver.
 */
class RemoteMeta extends MetaImpl {
  final Service service;

  public RemoteMeta(AvaticaConnection connection, Service service) {
    super(connection);
    this.service = service;
  }

  @Override public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    final Service.ResultSetResponse response =
        service.apply(new Service.SchemasRequest(catalog, schemaPattern));
    final AvaticaStatement statement = getOrCreateStatement(connection,
        response.statementId);
    return new MetaResultSet(statement, response.ownStatement,
        foo(response.prepareResult), null);
  }

  private AvaticaPrepareResult foo(final Service.PrepareResult prepareResult) {
    return new AvaticaPrepareResult() {
      public List<ColumnMetaData> getColumnList() {
        return prepareResult.columns;
      }

      public String getSql() {
        return prepareResult.sql;
      }

      public List<AvaticaParameter> getParameterList() {
        return prepareResult.parameters;
      }

      public Map<String, Object> getInternalParameters() {
        return prepareResult.internalParameters;
      }
    };
  }

  private AvaticaStatement getOrCreateStatement(AvaticaConnection connection,
      int statementId) {
    try {
      final AvaticaStatement statement = connection.createStatement();
      assert statement.id == statementId; // FIXME
      return statement;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

// End RemoteMeta.java
