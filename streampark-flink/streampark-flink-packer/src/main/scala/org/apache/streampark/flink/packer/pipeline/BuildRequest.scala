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

package org.apache.streampark.flink.packer.pipeline

import scala.collection.mutable.ArrayBuffer

import org.apache.streampark.common.conf.{FlinkVersion, Workspace}
import org.apache.streampark.common.enums.{JobType, ExecutionMode}
import org.apache.streampark.flink.kubernetes.model.K8sPodTemplates
import org.apache.streampark.flink.packer.docker.DockerConf
import org.apache.streampark.flink.packer.maven.DependencyInfo

/**
 * Params of a BuildPipeline instance.
 */
sealed trait BuildParam {
  def appName: String

  def mainClass: String
}

sealed trait FlinkBuildParam extends BuildParam {

  private[this] val localWorkspace = Workspace.local

  def workspace: String

  def executionMode: ExecutionMode

  def developmentMode: JobType

  def flinkVersion: FlinkVersion

  def dependencyInfo: DependencyInfo

  def customFlinkUserJar: String

  lazy val providedLibs: DependencyInfo = {
    val providedLibs = ArrayBuffer(
      localWorkspace.APP_JARS,
      localWorkspace.APP_PLUGINS,
      customFlinkUserJar)
    if (developmentMode == JobType.FLINK_SQL) {
      providedLibs += {
        val version = flinkVersion.version.split("\\.").map(_.trim.toInt)
        version match {
          case Array(1, 12, _) => s"${localWorkspace.APP_SHIMS}/flink-1.12"
          case Array(1, 13, _) => s"${localWorkspace.APP_SHIMS}/flink-1.13"
          case Array(1, 14, _) => s"${localWorkspace.APP_SHIMS}/flink-1.14"
          case Array(1, 15, _) => s"${localWorkspace.APP_SHIMS}/flink-1.15"
          case Array(1, 16, _) => s"${localWorkspace.APP_SHIMS}/flink-1.16"
          case _ => throw new UnsupportedOperationException(s"Unsupported flink version: $flinkVersion")
        }
      }
    }
    dependencyInfo.merge(providedLibs.toSet)
  }

  def getShadedJarPath(rootWorkspace: String): String = {
    val safeAppName: String = appName.replaceAll("\\s+", "_")
    s"$rootWorkspace/streampark-flinkjob_$safeAppName.jar"
  }

}

sealed trait FlinkK8sBuildParam extends FlinkBuildParam {

  def clusterId: String

  def k8sNamespace: String
}

case class FlinkK8sSessionBuildRequest(
                                        appName: String,
                                        workspace: String,
                                        mainClass: String,
                                        customFlinkUserJar: String,
                                        executionMode: ExecutionMode,
                                        developmentMode: JobType,
                                        flinkVersion: FlinkVersion,
                                        dependencyInfo: DependencyInfo,
                                        clusterId: String,
                                        k8sNamespace: String) extends FlinkK8sBuildParam

case class FlinkK8sApplicationBuildRequest(
                                            appName: String,
                                            workspace: String,
                                            mainClass: String,
                                            customFlinkUserJar: String,
                                            executionMode: ExecutionMode,
                                            developmentMode: JobType,
                                            flinkVersion: FlinkVersion,
                                            dependencyInfo: DependencyInfo,
                                            clusterId: String,
                                            k8sNamespace: String,
                                            flinkBaseImage: String,
                                            flinkPodTemplate: K8sPodTemplates,
                                            integrateWithHadoop: Boolean = false,
                                            dockerConfig: DockerConf,
                                            ingressTemplate: String) extends FlinkK8sBuildParam

case class FlinkRemotePerJobBuildRequest(
                                          appName: String,
                                          workspace: String,
                                          mainClass: String,
                                          customFlinkUserJar: String,
                                          skipBuild: Boolean,
                                          executionMode: ExecutionMode,
                                          developmentMode: JobType,
                                          flinkVersion: FlinkVersion,
                                          dependencyInfo: DependencyInfo) extends FlinkBuildParam

case class FlinkYarnApplicationBuildRequest(
                                             appName: String,
                                             mainClass: String,
                                             localWorkspace: String,
                                             yarnProvidedPath: String,
                                             developmentMode: JobType,
                                             dependencyInfo: DependencyInfo) extends BuildParam
