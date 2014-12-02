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
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * API for request-response calls to an Avatica server.
 */
public interface Service {
  ResultSetResponse apply(CatalogsRequest request);
  ResultSetResponse apply(SchemasRequest request);

  /** Factory that creates a {@code Service}. */
  interface Factory {
    Service create(AvaticaConnection connection);
  }

  /** Base class for all service request messages. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "request",
      defaultImpl = SchemasRequest.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = CatalogsRequest.class, name = "getCatalogs"),
      @JsonSubTypes.Type(value = SchemasRequest.class, name = "getSchemas") })
  abstract class Request {
    abstract Response accept(Service service);
  }

  /** Base class for all service response messages. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "response",
      defaultImpl = ResultSetResponse.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = ResultSetResponse.class, name = "resultSet") })
  abstract class Response {
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getCatalogs()}. */
  class CatalogsRequest extends Request {
    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Request for
   * {@link Meta#getSchemas(String, org.apache.calcite.avatica.Meta.Pat)}. */
  class SchemasRequest extends Request {
    public final String catalog;
    public final Meta.Pat schemaPattern;

    @JsonCreator
    public SchemasRequest(@JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") Meta.Pat schemaPattern) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
    }

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Response that contains a result set. */
  class ResultSetResponse extends Response {
    public final int statementId;
    public final boolean ownStatement;
    public final String timeZone;
    public final PrepareResult prepareResult;
    public final List<List<Object>> rows;

    @JsonCreator
    public ResultSetResponse(@JsonProperty("statementId") int statementId,
        @JsonProperty("ownStatement") boolean ownStatement,
        @JsonProperty("timeZone") String timeZone,
        @JsonProperty("prepareResult") PrepareResult prepareResult,
        @JsonProperty("rows") List<List<Object>> rows) {
      this.statementId = statementId;
      this.ownStatement = ownStatement;
      this.timeZone = timeZone;
      this.prepareResult = prepareResult;
      this.rows = rows;
    }

    public static ResultSetResponse create(Meta.MetaResultSet resultSet) {
      return new ResultSetResponse(resultSet.statement.getId(),
          resultSet.ownStatement,
          resultSet.statement.connection.getTimeZone().getID(),
          new PrepareResult(
              resultSet.prepareResult.getColumnList(),
              resultSet.prepareResult.getSql(),
              resultSet.prepareResult.getParameterList(),
              resultSet.prepareResult.getInternalParameters()),
          Collections.<List<Object>>emptyList());
    }
  }

  /** Result of preparing a statement. */
  class PrepareResult {
    public final List<ColumnMetaData> columns;
    public final String sql;
    public final List<AvaticaParameter> parameters;
    public final Map<String, Object> internalParameters;

    @JsonCreator
    public PrepareResult(@JsonProperty("columns") List<ColumnMetaData> columns,
        @JsonProperty("sql") String sql,
        @JsonProperty("parameters") List<AvaticaParameter> parameters,
        @JsonProperty("internalParameters") Map<String, Object>
            internalParameters) {
      this.columns = columns;
      this.sql = sql;
      this.parameters = parameters;
      this.internalParameters = internalParameters;
    }
  }
}

// End Service.java
