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

val blockingServerRuntime = configurations.register("blockingServerRuntime") {
  description = "Configuration & classpath for :blockingServer exec task"
  isCanBeConsumed = false
  isCanBeResolved = true
  isVisible = false
}

dependencies {
  // TODO(https://github.com/apache/beam/issues/21156): Determine how to build without this dependency
  // also discussed at https://github.com/immutables/immutables/issues/291
  compileOnly("org.immutables:value:2.8.8")

  implementation(enforcedPlatform(library_java.getValue("google_cloud_platform_libraries_bom")))

  implementation(project(":sdks:java:core", configuration = "shadow"))
  implementation(project(":sdks:java:extensions:sql"))
  implementation(project(":sdks:java:extensions:avro"))
  implementation(project(":sdks:java:io:parquet"))
  implementation(project(":sdks:java:io:csv"))
  runtimeOnly(project(":runners:direct-java"))
  runtimeOnly(project(":runners:google-cloud-dataflow-java"))

  implementation("org.apache.spark:spark-connect-common_2.13:4.1.0-preview2") {
    // Spark Connect pulls in a concrete logging implementation that we do not want
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j2-impl")
  }
  implementation(library_java.getValue("grpc_stub"))
  implementation(library_java.getValue("grpc_api"))
  implementation(library_java.getValue("vendored_calcite_1_40_0"))
  implementation(library_java.getValue("vendored_guava_32_1_2_jre"))

  implementation(library_java.getValue("arrow_vector"))
  implementation(library_java.getValue("arrow_memory_core"))
  implementation(library_java.getValue("arrow_memory_netty"))

  implementation(library_java.getValue("avro"))
  implementation(library_java.getValue("commons_csv"))

  blockingServerRuntime(library_java.getValue("slf4j_simple"))
  //blockingServer(library_java.getValue("log4j_slf4j_impl"))

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
    "-Dio.grpc.level=FINEST",
    "-Djava.util.logging.config.file=" + file("src/main/resources/logging.properties").absolutePath,
    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
  )

  doFirst {
    println("--- Starting blocking SparkConnect server with JVM Args: ${jvmArgs} ---")
  }

  mainClass.set("org.apache.beam.sparkconnect.SparkConnectServer")
  classpath = project.files(sourceSets.getByName("main").runtimeClasspath, blockingServerRuntime)
}

tasks.register("printClasspath") {
  doLast {
    val cp = project.files(sourceSets.getByName("main").runtimeClasspath, blockingServerRuntime).asPath
    println("SPARK_CONNECT_CLASSPATH=" + cp)
  }
}

tasks.register<Exec>("complianceTests") {
  group = "Verification"
  description = "Runs the Apache Spark python connect compliance tests against the blockingServer"
  
  workingDir = projectDir
  
  val argsList = mutableListOf("./run_compliance_tests.sh")
  if (project.hasProperty("noIgnoreList")) {
      argsList.add("--no-ignore")
  }
  if (project.hasProperty("testTarget")) {
      argsList.add(project.property("testTarget").toString())
  }
  commandLine(argsList)
}

tasks.register<Exec>("computeComplianceStats") {
  group = "Verification"
  description = "Computes the current compliance stats over Spark Connect tests"
  workingDir = projectDir
  commandLine("./compute_compliance_stats.py")
}

tasks.register<Exec>("updateIgnoreList") {
  group = "Verification"
  description = "Updates the ignored_tests.txt file based on test failures"
  workingDir = projectDir
  commandLine("./update_ignore_list.py")
}
