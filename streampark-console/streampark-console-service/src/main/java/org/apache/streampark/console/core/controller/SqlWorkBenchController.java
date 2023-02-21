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

package org.apache.streampark.console.core.controller;

import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.core.service.SqlWorkBenchService;
import org.apache.streampark.gateway.results.ResultQueryCondition;
import org.apache.streampark.gateway.session.SessionHandle;

import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Validated
@RestController
@RequestMapping("flink/sqlWorkBench/{flinkClusterId}")
public class SqlWorkBenchController {
  private final SqlWorkBenchService sqlWorkBenchService;

  public SqlWorkBenchController(SqlWorkBenchService sqlWorkBenchService) {
    this.sqlWorkBenchService = sqlWorkBenchService;
  }

  // -------------------------------------------------------------------------------------------
  // Info API
  // -------------------------------------------------------------------------------------------

  @PostMapping("getGatewayInfo")
  public RestResponse getGatewayInfo(@PathVariable Long flinkClusterId) {
    return RestResponse.success(sqlWorkBenchService.getGatewayInfo(flinkClusterId));
  }

  // -------------------------------------------------------------------------------------------
  // Session Management
  // -------------------------------------------------------------------------------------------

  @PostMapping("sessions")
  public RestResponse openSession(@PathVariable Long flinkClusterId, String serviceType) {
    SessionHandle sessionHandle = sqlWorkBenchService.openSession(flinkClusterId);
    return RestResponse.success(sessionHandle);
  }

  @PostMapping("sessions/{sessionHandle}/heartbeat")
  public RestResponse heartbeat(
      @PathVariable Long flinkClusterId, @PathVariable String sessionHandle) {
    sqlWorkBenchService.heartbeat(flinkClusterId, sessionHandle);
    return RestResponse.success();
  }

  @DeleteMapping("sessions/{sessionHandle}")
  public RestResponse closeSession(
      @PathVariable Long flinkClusterId, @PathVariable String sessionHandle) {
    sqlWorkBenchService.closeSession(flinkClusterId, sessionHandle);
    return RestResponse.success();
  }

  // -------------------------------------------------------------------------------------------
  // Operation Management
  // -------------------------------------------------------------------------------------------

  @PostMapping("sessions/{sessionHandle}/operations/{operationHandle}/cancel")
  public RestResponse cancelOperation(
      @PathVariable Long flinkClusterId,
      @PathVariable String sessionHandle,
      @PathVariable String operationHandle) {
    sqlWorkBenchService.cancelOperation(flinkClusterId, sessionHandle, operationHandle);
    return RestResponse.success();
  }

  @DeleteMapping("sessions/{sessionHandle}/operations/{operationHandle}/close")
  public RestResponse closeOperation(
      @PathVariable Long flinkClusterId,
      @PathVariable String sessionHandle,
      @PathVariable String operationHandle) {
    sqlWorkBenchService.closeOperation(flinkClusterId, sessionHandle, operationHandle);
    return RestResponse.success();
  }

  @PostMapping("sessions/{sessionHandle}/operations/{operationHandle}/info")
  public RestResponse getOperationInfo(
      @PathVariable Long flinkClusterId,
      @PathVariable String sessionHandle,
      @PathVariable String operationHandle) {
    return RestResponse.success(
        sqlWorkBenchService.getOperationInfo(flinkClusterId, sessionHandle, operationHandle));
  }

  @PostMapping("sessions/{sessionHandle}/operations/{operationHandle}/resultSchema")
  public RestResponse getOperationResultSchema(
      @PathVariable Long flinkClusterId,
      @PathVariable String sessionHandle,
      @PathVariable String operationHandle) {
    return RestResponse.success(
        sqlWorkBenchService.getOperationResultSchema(
            flinkClusterId, sessionHandle, operationHandle));
  }

  // -------------------------------------------------------------------------------------------
  // Statements API
  // -------------------------------------------------------------------------------------------

  @PostMapping("sessions/{sessionHandle}/statements")
  public RestResponse executeStatement(
      @PathVariable Long flinkClusterId,
      @PathVariable String sessionHandle,
      @RequestParam String statement) {
    return RestResponse.success(
        sqlWorkBenchService.executeStatement(flinkClusterId, sessionHandle, statement));
  }

  @PostMapping("sessions/{sessionHandle}/statements/{operationHandle}/info")
  public RestResponse fetchResults(
      @PathVariable Long flinkClusterId,
      @PathVariable String sessionHandle,
      @PathVariable String operationHandle,
      @RequestParam ResultQueryCondition resultQueryCondition) {
    return RestResponse.success(
        sqlWorkBenchService.fetchResults(
            flinkClusterId, sessionHandle, operationHandle, resultQueryCondition));
  }

  // -------------------------------------------------------------------------------------------
  // Catalog API
  // -------------------------------------------------------------------------------------------
  // TODO: 2023/2/17  need to be implemented

}
