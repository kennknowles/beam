---
name: gradle-invocation
description: Guide on how to invoke Gradle in the Apache Beam repository, ensuring the correct Java version is used.
---

# Invoking Gradle in Apache Beam

**Purpose**: Guide the agent on how to run Gradle commands successfully, avoiding environment issues like unsupported Java versions.

**Key Requirement**:
Always set `JAVA_HOME` to Java 21 and override `PATH` to ensure the correct `java` executable is picked up, avoiding the default system Java (e.g., 25.0.1).

**Usage**:
Prepend the environment variables to your Gradle command:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
./gradlew <task_name> [options]
```

Or inline:
```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 PATH=/usr/lib/jvm/java-21-openjdk-amd64/bin:$PATH ./gradlew <task_name>
```

**Why**:
The default Java version on some systems (like `25.0.1`) may not be supported by the version of Gradle or Beam used in this project. Forcing Java 21 ensures compatibility.
