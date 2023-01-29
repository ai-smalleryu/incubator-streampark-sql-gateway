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

package org.apache.streampark.gateway.utils;

import org.apache.streampark.common.conf.ConfigOption;

public class SqlGatewayRestOptions {

  /** The address that should be used by clients to connect to the sql gateway server. */
  public static final ConfigOption<String> ADDRESS =
      new ConfigOption<String>(
          "streampark.sql-gateway.service",
          "org.apache.streampark.gateway.service.SqlGatewayService",
          true,
          String.class,
          "The service to execute the request.",
          null,
          null,
          null);

  /** The port that the client connects to. */
  public static final ConfigOption<Integer> PORT =
      new ConfigOption<Integer>(
          "streampark.sql-gateway.service",
          8080,
          true,
          Integer.class,
          "The service to execute the request.",
          null,
          null,
          null);
}
