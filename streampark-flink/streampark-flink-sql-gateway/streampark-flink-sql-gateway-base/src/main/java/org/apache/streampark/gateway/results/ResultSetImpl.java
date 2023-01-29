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

package org.apache.streampark.gateway.results;

import lombok.Data;

import javax.annotation.Nullable;

import java.util.List;

/** An implementation of {@link ResultSet}. */
@Data
public class ResultSetImpl implements ResultSet {

  private final ResultType resultType;

  @Nullable private final Long nextToken;

  private final ResultSchemaInfo resultSchema;
  private final List<RowData> data;
  private final boolean isQueryResult;

  @Nullable private final JobID jobID;

  private final ResultKind resultKind;
}
