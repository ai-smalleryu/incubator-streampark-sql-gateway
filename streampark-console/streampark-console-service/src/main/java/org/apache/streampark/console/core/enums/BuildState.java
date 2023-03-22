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

package org.apache.streampark.console.core.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

public enum BuildState implements Serializable {

  /** has changed, need rebuild */
  NEED_REBUILD(-2),
  /** has cancelled, not build */
  NOT_BUILD(-1),

  /** building */
  BUILDING(0),

  /** build successful */
  SUCCESSFUL(1),

  /** build failed */
  FAILED(2);

  @JsonValue @EnumValue private final int value;

  BuildState(int value) {
    this.value = value;
  }

  public int get() {
    return this.value;
  }
}
