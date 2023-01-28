/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.streampark.gateway.factories;

import org.apache.streampark.gateway.exception.ValidationException;
import org.apache.streampark.gateway.service.SqlGatewayService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.streampark.gateway.factories.FactoryUtil.SQL_GATEWAY_ENDPOINT_TYPE;

/** Util to discover the {@link SqlGatewayService}. */
public class SqlGatewayServiceFactoryUtils {

  /**
   * Attempts to discover the appropriate endpoint factory and creates the instance of the
   * endpoints.
   */
  public static List<SqlGatewayService> createSqlGatewayService(
      Map<String, String> configuration) {

    String identifiersStr =
        Optional.ofNullable(configuration.get(SQL_GATEWAY_ENDPOINT_TYPE.key()))
            .map(
                idStr -> {
                  if (idStr.trim().isEmpty()) {
                    return null;
                  }
                  return idStr.trim();
                })
            .orElseThrow(
                () ->
                    new ValidationException(
                        String.format(
                            "Endpoint options do not contain an option key '%s' for discovering an endpoint.",
                            SQL_GATEWAY_ENDPOINT_TYPE.key())));

    List<String> identifiers = Arrays.asList(identifiersStr.split(";"));

    if (identifiers.isEmpty()) {
      throw new ValidationException(
          String.format(
              "Endpoint options do not contain an option key '%s' for discovering an endpoint.",
              SQL_GATEWAY_ENDPOINT_TYPE.key()));
    }
    validateSpecifiedServicesAreUnique(identifiers);

    List<SqlGatewayService> endpoints = new ArrayList<>();
    for (String identifier : identifiers) {
      final SqlGatewayServiceFactory factory =
          FactoryUtil.discoverFactory(
              Thread.currentThread().getContextClassLoader(),
              SqlGatewayServiceFactory.class,
              identifier);

      endpoints.add(
          factory.createSqlGatewayService(new DefaultEndpointFactoryContext(getEndpointConfig())));
    }
    return endpoints;
  }

  public static Map<String, String> getEndpointConfig() {
    return new HashMap<>();
  }

  /** The default context of {@link SqlGatewayServiceFactory}. */
  public static class DefaultEndpointFactoryContext implements SqlGatewayServiceFactory.Context {

    private final Map<String, String> gateWayServiceOptions;

    public DefaultEndpointFactoryContext(Map<String, String> endpointConfig) {
      this.gateWayServiceOptions = endpointConfig;
    }

    @Override
    public Map<String, String> getGateWayServiceOptions() {
      return gateWayServiceOptions;
    }
  }

  private static void validateSpecifiedServicesAreUnique(List<String> identifiers) {
    Set<String> uniqueIdentifiers = new HashSet<>();

    for (String identifier : identifiers) {
      if (uniqueIdentifiers.contains(identifier)) {
        throw new ValidationException(
            String.format(
                "Get the duplicate endpoint identifier '%s' for the option '%s'. "
                    + "Please keep the specified endpoint identifier unique.",
                identifier, SQL_GATEWAY_ENDPOINT_TYPE.key()));
      }
      uniqueIdentifiers.add(identifier);
    }
  }
}
