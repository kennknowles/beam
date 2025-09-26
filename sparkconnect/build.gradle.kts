/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.gradle.kotlin.dsl.*

plugins {
  groovy
  id("java")
  id("org.apache.beam.module")
}

val applyJavaNature: groovy.lang.Closure<Any?> by extra
val library : Map<String, Any> by extra
val library_java: Map<String, String> = library.getValue("java") as Map<String, String>

applyJavaNature(mapOf(
  "exportJavadoc" to false,
  "automaticModuleName" to "org.apache.beam.sparkconnect"))

description = "Apache Beam :: Spark Connect"

val blockingServer = configurations.register("blockingServer") {
  description = "Configuration & classpath for :blockingServer exec task"
  isCanBeConsumed = false
  isCanBeResolved = true
  isVisible = false
}

dependencies {
  implementation(enforcedPlatform(library_java.getValue("google_cloud_platform_libraries_bom")))

  //  implementation project(path: ":sdks:java:core", configuration: "shadow")
  //  runtimeOnly project(path: ":runners:direct-java", configuration: "shadow")

  implementation("org.apache.spark:spark-connect-common_2.13:4.0.1") {
    // Spark Connect pulls in a concrete logging implementation that we do not want
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j2-impl")
  }
  implementation(library_java.getValue("grpc_stub"))
  implementation(library_java.getValue("grpc_api"))

  blockingServer(library_java.getValue("slf4j_simple"))
  //implementation library.java.grpc_netty

//  implementation library.java.vendored_guava_32_1_2_jre
//  implementation library.java.joda_time
// implementation library.java.protobuf_java
//  implementation library.java.slf4j_api
//  implementation library.java.vendored_grpc_1_69_0
//  testImplementation library.java.hamcrest
//  testImplementation library.java.junit
}

// Run this task to validate the Java environment setup for contributors
tasks.register<JavaExec>("blockingServer") {
  group = "Execution"
  description = "Run a SparkConnect server in the foreground (terminate it with SIGINT)"

  jvmArgs = listOf(
    "-Djava.util.logging.ConsoleHandler.level=FINEST",
    "-Dio.grpc.level=FINEST"
  )

  doFirst {
    println("--- Starting blocking SparkConnect server with JVM Args: ${jvmArgs} ---")
  }

  mainClass.set("org.apache.beam.sparkconnect.SparkConnectServer")
  classpath = project.files(sourceSets.getByName("main").runtimeClasspath, blockingServer)
}
