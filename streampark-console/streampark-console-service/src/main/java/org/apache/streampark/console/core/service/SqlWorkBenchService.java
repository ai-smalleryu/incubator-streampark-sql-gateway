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

package org.apache.streampark.console.core.service;

import org.apache.streampark.gateway.OperationHandle;
import org.apache.streampark.gateway.results.Column;
import org.apache.streampark.gateway.results.GatewayInfo;
import org.apache.streampark.gateway.results.OperationInfo;
import org.apache.streampark.gateway.results.ResultQueryCondition;
import org.apache.streampark.gateway.results.ResultSet;
import org.apache.streampark.gateway.session.SessionHandle;

public interface SqlWorkBenchService {

  SessionHandle openSession(Long flinkClusterId);

  void closeSession(Long flinkClusterId, String sessionHandleUUIDStr);

  GatewayInfo getGatewayInfo(Long flinkClusterId);

  void cancelOperation(Long flinkClusterId, String sessionHandleUUIDStr, String operationId);

  void closeOperation(Long flinkClusterId, String sessionHandleUUIDStr, String operationId);

  OperationInfo getOperationInfo(
      Long flinkClusterId, String sessionHandleUUIDStr, String operationId);

  Column getOperationResultSchema(
      Long flinkClusterId, String sessionHandleUUIDStr, String operationId);

  OperationHandle executeStatement(
      Long flinkClusterId, String sessionHandleUUIDStr, String statement);

  ResultSet fetchResults(
      Long flinkClusterId,
      String sessionHandleUUIDStr,
      String operationId,
      ResultQueryCondition resultQueryCondition);

  void heartbeat(Long flinkClusterId, String sessionHandle);
}
