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

import javax.annotation.Nullable;

import java.util.List;

/**
 * A {@code ResultSet} represents the collection of the results. This interface defines the methods
 * that can be used on the ResultSet.
 */
public interface ResultSet {

  /** Get the type of the results, which may indicate the result is EOS or has data. */
  ResultType getResultType();

  /**
   * The token indicates the next batch of the data.
   *
   * <p>When the token is null, it means all the data has been fetched.
   */
  @Nullable
  Long getNextToken();

  /**
   * The schema of the data.
   *
   * <p>The schema of the DDL, USE, EXPLAIN, SHOW and DESCRIBE align with the schema of the {@link
   * TableResult#getResolvedSchema()}. The only differences is the schema of the `INSERT` statement.
   *
   * <p>The schema of INSERT:
   *
   * <pre>
   * +-------------+-------------+----------+
   * | column name | column type | comments |
   * +-------------+-------------+----------+
   * |   job id    |    string   |          |
   * +- -----------+-------------+----------+
   * </pre>
   */
  ResultSchemaInfo getResultSchema();

  /** All the data in the current results. */
  List<RowData> getData();

  /** Indicates that whether the result is for a query. */
  boolean isQueryResult();

  /**
   * If the statement was submitted to a client, returns the JobID which uniquely identifies the
   * job. Otherwise, returns null.
   */
  @Nullable
  JobID getJobID();

  /** Gets the result kind of the result. */
  ResultKind getResultKind();

  /** Describe the kind of the result. */
  enum ResultType {
    /** Indicate the result is not ready. */
    NOT_READY,

    /** Indicate the result has data. */
    PAYLOAD,

    /** Indicate all results have been fetched. */
    EOS
  }
}
