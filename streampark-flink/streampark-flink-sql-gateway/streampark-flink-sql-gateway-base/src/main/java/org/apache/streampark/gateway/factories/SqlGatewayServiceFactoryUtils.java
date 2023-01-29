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

import static org.apache.streampark.gateway.factories.FactoryUtil.SQL_GATEWAY_SERVICE_TYPE;

/** Util to discover the {@link SqlGatewayService}. */
public class SqlGatewayServiceFactoryUtils {

  /**
   * Attempts to discover the appropriate service factory and creates the instance of the services.
   */
  public static List<SqlGatewayService> createSqlGatewayService(Map<String, String> configuration) {

    String identifiersStr =
        Optional.ofNullable(configuration.get(SQL_GATEWAY_SERVICE_TYPE.key()))
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
                            "Service options do not contain an option key '%s' for discovering an service.",
                            SQL_GATEWAY_SERVICE_TYPE.key())));

    List<String> identifiers = Arrays.asList(identifiersStr.split(";"));

    if (identifiers.isEmpty()) {
      throw new ValidationException(
          String.format(
              "Service options do not contain an option key '%s' for discovering an service.",
              SQL_GATEWAY_SERVICE_TYPE.key()));
    }
    validateSpecifiedServicesAreUnique(identifiers);

    List<SqlGatewayService> services = new ArrayList<>();
    for (String identifier : identifiers) {
      final SqlGatewayServiceFactory factory =
          FactoryUtil.discoverFactory(
              Thread.currentThread().getContextClassLoader(),
              SqlGatewayServiceFactory.class,
              identifier);

      services.add(
          factory.createSqlGatewayService(new DefaultServiceFactoryContext(getServiceConfig())));
    }
    return services;
  }

  public static Map<String, String> getServiceConfig() {
    return new HashMap<>();
  }

  /** The default context of {@link SqlGatewayServiceFactory}. */
  public static class DefaultServiceFactoryContext implements SqlGatewayServiceFactory.Context {

    private final Map<String, String> gateWayServiceOptions;

    public DefaultServiceFactoryContext(Map<String, String> endpointConfig) {
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
                "Get the duplicate service identifier '%s' for the option '%s'. "
                    + "Please keep the specified service identifier unique.",
                identifier, SQL_GATEWAY_SERVICE_TYPE.key()));
      }
      uniqueIdentifiers.add(identifier);
    }
  }
}
