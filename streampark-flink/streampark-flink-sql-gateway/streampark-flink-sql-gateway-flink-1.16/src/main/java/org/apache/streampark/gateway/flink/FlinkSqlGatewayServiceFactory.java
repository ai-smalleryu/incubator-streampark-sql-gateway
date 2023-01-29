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

package org.apache.streampark.gateway.flink;

import org.apache.streampark.common.conf.ConfigOption;
import org.apache.streampark.gateway.factories.SqlGatewayServiceFactory;
import org.apache.streampark.gateway.service.SqlGatewayService;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Flink sql gateway's Factory for {@link SqlGatewayService}. */
public class FlinkSqlGatewayServiceFactory implements SqlGatewayServiceFactory {

  public static final ConfigOption<String> BASE_URI =
      new ConfigOption<String>(
          "baseUri",
          "localhost",
          true,
          String.class,
          "The host of the Flink native SQL gateway service",
          null,
          null,
          null);

  @Override
  public String factoryIdentifier() {
    return "flink1.16";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(BASE_URI);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

  @Override
  public SqlGatewayService createSqlGatewayService(Context context) {
    return new FlinkSqlGatewayImpl("localhost:8080");
  }
}
