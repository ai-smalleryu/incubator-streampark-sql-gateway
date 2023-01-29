/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.gateway.service;

import org.apache.streampark.gateway.ExecutionConfiguration;
import org.apache.streampark.gateway.OperationHandle;
import org.apache.streampark.gateway.OperationStatus;
import org.apache.streampark.gateway.exception.SqlGatewayException;
import org.apache.streampark.gateway.results.FunctionInfo;
import org.apache.streampark.gateway.results.GatewayInfo;
import org.apache.streampark.gateway.results.OperationInfo;
import org.apache.streampark.gateway.results.ResultQueryCondition;
import org.apache.streampark.gateway.results.ResultSchemaInfo;
import org.apache.streampark.gateway.results.ResultSet;
import org.apache.streampark.gateway.results.TableInfo;
import org.apache.streampark.gateway.results.TableKind;
import org.apache.streampark.gateway.session.SessionEnvironment;
import org.apache.streampark.gateway.session.SessionHandle;

import java.util.List;
import java.util.Set;

/** A service of SQL gateway is responsible for handling requests from streampark console. */
public interface SqlGatewayService {

  // -------------------------------------------------------------------------------------------
  // Info API
  // -------------------------------------------------------------------------------------------

  GatewayInfo getGatewayInfo() throws SqlGatewayException;

  // -------------------------------------------------------------------------------------------
  // Session Management
  // -------------------------------------------------------------------------------------------

  /**
   * Open the {@code Session}.
   *
   * @param environment Environment to initialize the Session.
   * @return Returns a handle that used to identify the Session.
   */
  SessionHandle openSession(SessionEnvironment environment) throws SqlGatewayException;

  /**
   * Close the {@code Session}.
   *
   * @param sessionHandle handle to identify the Session needs to be closed.
   */
  void closeSession(SessionHandle sessionHandle) throws SqlGatewayException;

  // -------------------------------------------------------------------------------------------
  // Operation Management
  // -------------------------------------------------------------------------------------------

  /**
   * Cancel the operation when it is not in terminal status.
   *
   * <p>It can't cancel an Operation if it is terminated.
   *
   * @param sessionHandle handle to identify the session.
   * @param operationHandle handle to identify the operation.JarURLConnection
   */
  void cancelOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
      throws SqlGatewayException;

  /**
   * Close the operation and release all used resource by the operation.
   *
   * @param sessionHandle handle to identify the session.
   * @param operationHandle handle to identify the operation.
   */
  void closeOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
      throws SqlGatewayException;

  /**
   * Get the {@link OperationInfo} of the operation.
   *
   * @param sessionHandle handle to identify the session.
   * @param operationHandle handle to identify the operation.
   */
  OperationInfo getOperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle)
      throws SqlGatewayException;

  /**
   * Get the result schema for the specified Operation.
   *
   * <p>Note: The result schema is available when the Operation is in the {@link
   * OperationStatus#FINISHED}.
   *
   * @param sessionHandle handle to identify the session.
   * @param operationHandle handle to identify the operation.
   */
  ResultSchemaInfo getOperationResultSchema(
      SessionHandle sessionHandle, OperationHandle operationHandle) throws SqlGatewayException;

  // -------------------------------------------------------------------------------------------
  // Statements API
  // -------------------------------------------------------------------------------------------

  /**
   * Execute the submitted statement.
   *
   * @param sessionHandle handle to identify the session.
   * @param statement the SQL to execute.
   * @param executionTimeoutMs the execution timeout. Please use non-positive value to forbid the
   *     timeout mechanism.
   * @param executionConfig execution config for the statement.
   * @return handle to identify the operation.
   */
  OperationHandle executeStatement(
      SessionHandle sessionHandle,
      String statement,
      long executionTimeoutMs,
      ExecutionConfiguration executionConfig)
      throws SqlGatewayException;

  /**
   * Fetch the results from the operation. When maxRows is Integer.MAX_VALUE, it means to fetch all
   * available data.
   *
   * @param sessionHandle handle to identify the session.
   * @param operationHandle handle to identify the operation.
   * @param resultQueryCondition condition of result query.
   * @return Returns the results.
   */
  ResultSet fetchResults(
      SessionHandle sessionHandle,
      OperationHandle operationHandle,
      ResultQueryCondition resultQueryCondition)
      throws SqlGatewayException;

  // -------------------------------------------------------------------------------------------
  // Catalog API
  // -------------------------------------------------------------------------------------------

  /**
   * Return current catalog name.
   *
   * @param sessionHandle handle to identify the session.
   * @return name of the current catalog.
   */
  String getCurrentCatalog(SessionHandle sessionHandle) throws SqlGatewayException;

  /**
   * Return all available catalogs in the current session.
   *
   * @param sessionHandle handle to identify the session.
   * @return names of the registered catalogs.
   */
  Set<String> listCatalogs(SessionHandle sessionHandle) throws SqlGatewayException;

  /**
   * Return all available schemas in the given catalog.
   *
   * @param sessionHandle handle to identify the session.
   * @param catalogName name string of the given catalog.
   * @return names of the registered schemas.
   */
  Set<String> listDatabases(SessionHandle sessionHandle, String catalogName)
      throws SqlGatewayException;

  /**
   * Return all available tables/views in the given catalog and database.
   *
   * @param sessionHandle handle to identify the session.
   * @param catalogName name of the given catalog.
   * @param databaseName name of the given database.
   * @param tableKinds used to specify the type of return values.
   * @return table info of the registered tables/views.
   */
  Set<TableInfo> listTables(
      SessionHandle sessionHandle,
      String catalogName,
      String databaseName,
      Set<TableKind> tableKinds)
      throws SqlGatewayException;

  /*
   */
  /**
   * Return table of the given fully qualified name.
   *
   * @param sessionHandle handle to identify the session.
   * @param tableIdentifier fully qualified name of the table.
   * @return information of the table.
   */
  /*

      ResolvedCatalogBaseTable<?> getTable(
          SessionHandle sessionHandle, ObjectIdentifier tableIdentifier)
          throws SqlGatewayException;
  */

  /**
   * List functions.
   *
   * @param sessionHandle handle to identify the session.
   * @param catalogName name string of the given catalog.
   * @param databaseName name string of the given database.
   * @param functionName name string of the given functionName.
   * @return user defined functions info.
   */
  Set<FunctionInfo> listFunctions(
      SessionHandle sessionHandle, String catalogName, String databaseName, String functionName)
      throws SqlGatewayException;

  // -------------------------------------------------------------------------------------------
  // Utilities
  // -------------------------------------------------------------------------------------------

  /**
   * Returns a list of completion hints for the given statement at the given position.
   *
   * @param sessionHandle handle to identify the session.
   * @param statement sql statement to be completed.
   * @param position position of where need completion hints.
   * @return completion hints.
   */
  List<String> completeStatement(SessionHandle sessionHandle, String statement, int position)
      throws SqlGatewayException;
}
