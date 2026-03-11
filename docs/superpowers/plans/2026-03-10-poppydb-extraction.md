# PoppyDB Extraction — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract MorphiumServer into a separate Maven module named PoppyDB with its own identity, package structure, and Maven Central artifact.

**Architecture:** Convert the single-module Morphium project into a multi-module Maven project. The existing code stays in `morphium-core/` (artifactId unchanged: `morphium`). The server code moves to `poppydb/` (new artifactId: `poppydb`) with package `de.caluga.poppydb`. PoppyDB depends on morphium-core.

**Tech Stack:** Java 21, Maven Multi-Module, Netty 4.2.9, JUnit 5

**Branch:** `feature/poppydb-extraction` (basiert auf `master`)

**Spec:** `docs/superpowers/specs/2026-03-10-poppydb-rename-design.md`

---

## Chunk 1: Maven Multi-Module Umbau

### Task 1: Branch erstellen

- [ ] **Step 1: Feature-Branch erstellen**

```bash
git checkout master
git pull
git checkout -b feature/poppydb-extraction
```

- [ ] **Step 2: Commit — Branch-Startpunkt**

```bash
git commit --allow-empty -m "Start PoppyDB extraction from MorphiumServer"
```

### Task 2: Verzeichnisstruktur anlegen

**Files:**
- Create: `morphium-core/` (Verzeichnis)
- Create: `poppydb/` (Verzeichnis)

- [ ] **Step 1: morphium-core Verzeichnis anlegen und Source-Code verschieben**

```bash
mkdir -p morphium-core
# Alles außer pom.xml und die neuen Modulverzeichnisse verschieben
git mv src morphium-core/
```

- [ ] **Step 2: Server-Package aus morphium-core nach poppydb verschieben**

```bash
mkdir -p poppydb/src/main/java/de/caluga/poppydb
mkdir -p poppydb/src/main/assembly
mkdir -p poppydb/src/test/java/de/caluga/test/poppydb/election

# Source-Dateien verschieben
git mv morphium-core/src/main/java/de/caluga/morphium/server/MorphiumServer.java poppydb/src/main/java/de/caluga/poppydb/PoppyDB.java
git mv morphium-core/src/main/java/de/caluga/morphium/server/MorphiumServerCLI.java poppydb/src/main/java/de/caluga/poppydb/PoppyDBCLI.java
git mv morphium-core/src/main/java/de/caluga/morphium/server/ReplicationCoordinator.java poppydb/src/main/java/de/caluga/poppydb/
git mv morphium-core/src/main/java/de/caluga/morphium/server/ReplicationManager.java poppydb/src/main/java/de/caluga/poppydb/

# Subpackages verschieben
git mv morphium-core/src/main/java/de/caluga/morphium/server/election poppydb/src/main/java/de/caluga/poppydb/
git mv morphium-core/src/main/java/de/caluga/morphium/server/messaging poppydb/src/main/java/de/caluga/poppydb/
git mv morphium-core/src/main/java/de/caluga/morphium/server/netty poppydb/src/main/java/de/caluga/poppydb/

# Assembly Descriptor verschieben
git mv morphium-core/src/main/assembly poppydb/src/main/

# Leeres server-Package entfernen
rm -rf morphium-core/src/main/java/de/caluga/morphium/server
```

- [ ] **Step 3: Test-Dateien verschieben**

```bash
# Server-spezifische Tests nach poppydb
git mv morphium-core/src/test/java/de/caluga/test/morphium/server/ServerCompressionTest.java poppydb/src/test/java/de/caluga/test/poppydb/
git mv morphium-core/src/test/java/de/caluga/test/morphium/server/election/MultiNodeElectionTest.java poppydb/src/test/java/de/caluga/test/poppydb/election/
git mv morphium-core/src/test/java/de/caluga/test/morphium/server/election/ElectionManagerTest.java poppydb/src/test/java/de/caluga/test/poppydb/election/
git mv morphium-core/src/test/java/de/caluga/test/mongo/suite/base/MorphiumServerTest.java poppydb/src/test/java/de/caluga/test/poppydb/PoppyDBTest.java
git mv morphium-core/src/test/java/de/caluga/test/FailoverTests.java poppydb/src/test/java/de/caluga/test/poppydb/

# Leere Verzeichnisse aufräumen
rm -rf morphium-core/src/test/java/de/caluga/test/morphium/server
```

- [ ] **Step 4: Test-Resources kopieren (falls vorhanden)**

```bash
# Falls morphium-core test-resources hat, poppydb braucht sie evtl. auch
if [ -d morphium-core/src/test/resources ]; then
    cp -r morphium-core/src/test/resources poppydb/src/test/
fi
```

- [ ] **Step 5: Commit — Verzeichnisstruktur**

```bash
git add -A
git commit -m "Move server code to poppydb module directory structure"
```

### Task 3: Parent POM erstellen

**Files:**
- Modify: `pom.xml` (wird Parent POM)
- Create: `morphium-core/pom.xml`
- Create: `poppydb/pom.xml`

- [ ] **Step 1: Aktuelles pom.xml zum Parent POM umbauen**

Das aktuelle `pom.xml` wird umgebaut zu:
- `packaging: pom`
- Properties, dependency-management, plugin-management bleiben
- Konkrete dependencies und plugins wandern in die Module
- `<modules>` Block hinzufügen

```xml
<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>de.caluga</groupId>
    <artifactId>morphium-parent</artifactId>
    <version>6.2.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>Morphium Parent</name>
    <url>http://caluga.de</url>
    <description>Morphium - Parent POM for Morphium and PoppyDB</description>

    <modules>
        <module>morphium-core</module>
        <module>poppydb</module>
    </modules>

    <licenses>
        <license>
            <name>The Apache Software License, Version 2.0</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
            <distribution>repo</distribution>
        </license>
    </licenses>
    <scm>
        <url>https://github.com/sboesebeck/morphium</url>
        <connection>scm:git:git://github.com/sboesebeck/morphium.git</connection>
        <developerConnection>scm:git:git@github.com:sboesebeck/morphium.git</developerConnection>
        <tag>v6.1.9</tag>
    </scm>
    <developers>
        <developer>
            <id>sBoesebeck</id>
            <name>Stephan Bösebeck</name>
            <email>sb@caluga.de</email>
        </developer>
    </developers>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <maven.compiler.release>21</maven.compiler.release>
        <maven.compiler.skip>false</maven.compiler.skip>
        <java.version>21</java.version>
        <jacoco.percentage.instruction>0.40</jacoco.percentage.instruction>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <mongodbDriver.version>4.11.5</mongodbDriver.version>
        <netty.version>4.2.9.Final</netty.version>
        <argLine/>
        <test.includeTags/>
        <test.excludeTags>external</test.excludeTags>
    </properties>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.12.1</version>
                    <configuration>
                        <source>${maven.compiler.source}</source>
                        <target>${maven.compiler.target}</target>
                        <release>${maven.compiler.release}</release>
                        <skip>${maven.compiler.skip}</skip>
                        <compilerArgs>
                            <arg>-Xlint:deprecation,unchecked</arg>
                        </compilerArgs>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <version>3.0.0</version>
                    <configuration>
                        <argLine>${argLine} --add-opens java.base/java.lang=ALL-UNNAMED</argLine>
                        <groups>${test.includeTags}</groups>
                        <excludedGroups>${test.excludeTags}</excludedGroups>
                        <properties>
                            <configurationParameters>
                                junit.jupiter.extensions.autodetection.enabled=true
                            </configurationParameters>
                        </properties>
                        <forkCount>1</forkCount>
                        <reuseForks>false</reuseForks>
                        <runOrder>filesystem</runOrder>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-source-plugin</artifactId>
                    <version>3.1.0</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-javadoc-plugin</artifactId>
                    <version>3.4.1</version>
                    <configuration>
                        <failOnError>false</failOnError>
                        <additionalJOption>-J-Xmx2048m</additionalJOption>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-jar-plugin</artifactId>
                    <version>3.2.2</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-release-plugin</artifactId>
                    <version>2.5.3</version>
                    <configuration>
                        <tagNameFormat>v@{project.version}</tagNameFormat>
                        <autoVersionSubmodules>true</autoVersionSubmodules>
                        <releaseProfiles>release-sign-artifacts</releaseProfiles>
                        <arguments>-Dmaven.javadoc.skip=false -Dmaven.test.skipTests=true -Dmaven.test.skip=true</arguments>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <version>3.7.1</version>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.xerial.snappy</groupId>
                <artifactId>snappy-java</artifactId>
                <version>1.1.10.8</version>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>2.0.17</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-core</artifactId>
                <version>1.5.25</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>1.5.25</version>
            </dependency>
            <dependency>
                <groupId>org.mongodb</groupId>
                <artifactId>bson</artifactId>
                <version>${mongodbDriver.version}</version>
            </dependency>
            <dependency>
                <groupId>io.netty</groupId>
                <artifactId>netty-all</artifactId>
                <version>${netty.version}</version>
            </dependency>
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter-params</artifactId>
                <version>5.9.0</version>
            </dependency>
            <dependency>
                <groupId>org.assertj</groupId>
                <artifactId>assertj-core</artifactId>
                <version>3.27.7</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <profiles>
        <profile>
            <id>release-nosign-artifacts</id>
        </profile>
        <profile>
            <id>release-sign-artifacts</id>
            <activation>
                <activeByDefault>true</activeByDefault>
                <property>
                    <name>performRelease</name>
                    <value>true</value>
                </property>
            </activation>
        </profile>
        <profile>
            <id>external</id>
            <properties>
                <test.excludeTags/>
            </properties>
        </profile>
        <profile>
            <id>inmem</id>
            <properties>
                <morphium.driver>inmem</morphium.driver>
            </properties>
        </profile>
        <profile>
            <id>pooled</id>
            <properties>
                <morphium.driver>pooled</morphium.driver>
            </properties>
        </profile>
        <profile>
            <id>single</id>
            <properties>
                <morphium.driver>single</morphium.driver>
            </properties>
        </profile>
    </profiles>
</project>
```

- [ ] **Step 2: morphium-core/pom.xml erstellen**

Enthält alle bisherigen Dependencies (außer Netty) und Plugins. ArtifactId bleibt `morphium` für Abwärtskompatibilität.

```xml
<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>de.caluga</groupId>
        <artifactId>morphium-parent</artifactId>
        <version>6.2.0-SNAPSHOT</version>
    </parent>

    <artifactId>morphium</artifactId>
    <packaging>jar</packaging>
    <name>Morphium</name>
    <description>Morphium - a Caching Object Mapper for MongoDB</description>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <systemPropertyVariables>
                        <morphium.tests.verbose>${morphium.tests.verbose}</morphium.tests.verbose>
                    </systemPropertyVariables>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-source-plugin</artifactId>
                <executions>
                    <execution>
                        <id>attach-sources</id>
                        <goals><goal>jar</goal></goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-javadoc-plugin</artifactId>
                <executions>
                    <execution>
                        <id>attach-javadocs</id>
                        <goals><goal>jar</goal></goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>log4j*.xml</exclude>
                        <exclude>logging*.properties</exclude>
                        <exclude>logback*.xml</exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
        <testResources>
            <testResource>
                <directory>${project.basedir}/src/test/resources</directory>
            </testResource>
        </testResources>
        <resources>
            <resource>
                <directory>${project.basedir}/src/main/resources</directory>
            </resource>
        </resources>
    </build>

    <dependencies>
        <!-- Alle bisherigen Dependencies OHNE Netty -->
        <dependency>
            <groupId>org.xerial.snappy</groupId>
            <artifactId>snappy-java</artifactId>
        </dependency>
        <dependency>
            <groupId>org.openjdk.jol</groupId>
            <artifactId>jol-core</artifactId>
            <version>0.17</version>
        </dependency>
        <dependency>
            <groupId>org.graalvm.js</groupId>
            <artifactId>js-scriptengine</artifactId>
            <version>25.0.1</version>
        </dependency>
        <dependency>
            <groupId>org.graalvm.polyglot</groupId>
            <artifactId>polyglot</artifactId>
            <version>25.0.1</version>
        </dependency>
        <dependency>
            <groupId>org.graalvm.polyglot</groupId>
            <artifactId>js</artifactId>
            <version>25.0.1</version>
            <type>pom</type>
            <exclusions>
                <exclusion>
                    <groupId>org.graalvm.truffle</groupId>
                    <artifactId>truffle-enterprise</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>javax.jms</groupId>
            <artifactId>javax.jms-api</artifactId>
            <version>2.0.1</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-core</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-core</artifactId>
            <version>5.3.39</version>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>bson</artifactId>
        </dependency>
        <dependency>
            <groupId>com.ongres.scram</groupId>
            <artifactId>client</artifactId>
            <version>2.1</version>
        </dependency>
        <dependency>
            <groupId>com.googlecode.json-simple</groupId>
            <artifactId>json-simple</artifactId>
            <version>1.1.1</version>
            <exclusions>
                <exclusion>
                    <groupId>junit</groupId>
                    <artifactId>junit</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>javax.validation</groupId>
            <artifactId>validation-api</artifactId>
            <version>2.0.1.Final</version>
        </dependency>
        <dependency>
            <groupId>javax.cache</groupId>
            <artifactId>cache-api</artifactId>
            <version>1.1.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-lang3</artifactId>
            <version>3.20.0</version>
        </dependency>
        <dependency>
            <groupId>io.github.classgraph</groupId>
            <artifactId>classgraph</artifactId>
            <version>4.8.184</version>
        </dependency>
        <dependency>
            <groupId>de.caluga</groupId>
            <artifactId>rsa</artifactId>
            <version>1.1</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.logging.log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <!-- Test dependencies -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-params</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-sync</artifactId>
            <version>${mongodbDriver.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-core</artifactId>
            <version>${mongodbDriver.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.colofabrix.scala</groupId>
            <artifactId>figlet4s-java</artifactId>
            <version>0.3.1</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.ehcache</groupId>
            <artifactId>jcache</artifactId>
            <version>1.0.1</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.hibernate.validator</groupId>
            <artifactId>hibernate-validator</artifactId>
            <version>8.0.0.Final</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: poppydb/pom.xml erstellen**

```xml
<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>de.caluga</groupId>
        <artifactId>morphium-parent</artifactId>
        <version>6.2.0-SNAPSHOT</version>
    </parent>

    <artifactId>poppydb</artifactId>
    <packaging>jar</packaging>
    <name>PoppyDB</name>
    <description>PoppyDB - A lightweight, in-memory MongoDB-compatible server, optimized for messaging</description>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-source-plugin</artifactId>
                <executions>
                    <execution>
                        <id>attach-sources</id>
                        <goals><goal>jar</goal></goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-javadoc-plugin</artifactId>
                <executions>
                    <execution>
                        <id>attach-javadocs</id>
                        <goals><goal>jar</goal></goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>log4j*.xml</exclude>
                        <exclude>logging*.properties</exclude>
                        <exclude>logback*.xml</exclude>
                    </excludes>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptors>
                        <descriptor>src/main/assembly/server-cli.xml</descriptor>
                    </descriptors>
                    <archive>
                        <manifest>
                            <mainClass>de.caluga.poppydb.PoppyDBCLI</mainClass>
                        </manifest>
                        <manifestEntries>
                            <Multi-Release>true</Multi-Release>
                        </manifestEntries>
                    </archive>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
        <testResources>
            <testResource>
                <directory>${project.basedir}/src/test/resources</directory>
            </testResource>
        </testResources>
    </build>

    <dependencies>
        <!-- Morphium core -->
        <dependency>
            <groupId>de.caluga</groupId>
            <artifactId>morphium</artifactId>
            <version>${project.version}</version>
        </dependency>
        <!-- Netty — required (not optional) for PoppyDB -->
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-all</artifactId>
        </dependency>
        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-core</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </dependency>
        <!-- Test dependencies -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-params</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-sync</artifactId>
            <version>${mongodbDriver.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-core</artifactId>
            <version>${mongodbDriver.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 4: Commit — Maven-Struktur**

```bash
git add pom.xml morphium-core/pom.xml poppydb/pom.xml
git commit -m "Convert to multi-module Maven project with morphium-core and poppydb"
```

### Task 4: Kompilierung prüfen (ohne Package-Umbau)

- [ ] **Step 1: morphium-core kompiliert**

```bash
cd morphium-core && mvn compile -q && echo "OK" || echo "FAIL"
```

Expected: OK (keine server-Klassen mehr, keine Netty-Dependency)

- [ ] **Step 2: Fehler beheben falls nötig**

Falls morphium-core nicht kompiliert: Prüfen ob noch Imports auf `de.caluga.morphium.server` existieren und entfernen.

---

## Chunk 2: Package-Umbau und Rename

### Task 5: Package-Deklarationen in PoppyDB-Quellen anpassen

**Files:** Alle 18 Java-Dateien in `poppydb/src/main/java/de/caluga/poppydb/`

- [ ] **Step 1: Package-Deklarationen per find/sed anpassen**

In allen Source-Dateien unter `poppydb/src/main/java/`:

```bash
# Package-Deklaration ändern
find poppydb/src/main/java -name "*.java" -exec sed -i '' \
    's/package de\.caluga\.morphium\.server/package de.caluga.poppydb/g' {} +

# Subpackage-Deklarationen anpassen
find poppydb/src/main/java -name "*.java" -exec sed -i '' \
    's/package de\.caluga\.poppydb\.election/package de.caluga.poppydb.election/g' {} +
find poppydb/src/main/java -name "*.java" -exec sed -i '' \
    's/package de\.caluga\.poppydb\.netty/package de.caluga.poppydb.netty/g' {} +
find poppydb/src/main/java -name "*.java" -exec sed -i '' \
    's/package de\.caluga\.poppydb\.messaging/package de.caluga.poppydb.messaging/g' {} +
```

(Hinweis: Die Subpackage-Deklarationen sind nach dem ersten sed schon korrekt, da `de.caluga.morphium.server.election` zu `de.caluga.poppydb.election` wird. Trotzdem zur Sicherheit prüfen.)

- [ ] **Step 2: Imports innerhalb PoppyDB anpassen**

```bash
# Alle internen Server-Imports anpassen
find poppydb/src/main/java -name "*.java" -exec sed -i '' \
    's/import de\.caluga\.morphium\.server\./import de.caluga.poppydb./g' {} +
```

- [ ] **Step 3: Klasse MorphiumServer → PoppyDB umbenennen**

In `poppydb/src/main/java/de/caluga/poppydb/PoppyDB.java`:
- Klassenname: `MorphiumServer` → `PoppyDB`
- Konstruktor(en) umbenennen
- Logger-Name anpassen
- Alle internen Referenzen auf `MorphiumServer` → `PoppyDB`

```bash
# In PoppyDB.java
sed -i '' 's/class MorphiumServer/class PoppyDB/g' poppydb/src/main/java/de/caluga/poppydb/PoppyDB.java
sed -i '' 's/MorphiumServer(/PoppyDB(/g' poppydb/src/main/java/de/caluga/poppydb/PoppyDB.java
sed -i '' 's/MorphiumServer\./PoppyDB./g' poppydb/src/main/java/de/caluga/poppydb/PoppyDB.java
sed -i '' 's/MorphiumServer /PoppyDB /g' poppydb/src/main/java/de/caluga/poppydb/PoppyDB.java
sed -i '' 's/"MorphiumServer"/"PoppyDB"/g' poppydb/src/main/java/de/caluga/poppydb/PoppyDB.java

# Alle anderen Dateien die MorphiumServer referenzieren
find poppydb/src/main/java -name "*.java" ! -name "PoppyDB.java" -exec sed -i '' \
    's/MorphiumServer/PoppyDB/g' {} +
```

- [ ] **Step 4: Klasse MorphiumServerCLI → PoppyDBCLI umbenennen**

In `poppydb/src/main/java/de/caluga/poppydb/PoppyDBCLI.java`:

```bash
sed -i '' 's/class MorphiumServerCLI/class PoppyDBCLI/g' poppydb/src/main/java/de/caluga/poppydb/PoppyDBCLI.java
sed -i '' 's/MorphiumServerCLI/PoppyDBCLI/g' poppydb/src/main/java/de/caluga/poppydb/PoppyDBCLI.java
sed -i '' 's/MorphiumServer/PoppyDB/g' poppydb/src/main/java/de/caluga/poppydb/PoppyDBCLI.java
```

- [ ] **Step 5: Commit — Package-Umbau Source**

```bash
git add -A
git commit -m "Rename packages and classes: MorphiumServer -> PoppyDB"
```

### Task 6: Test-Dateien anpassen

**Files:** Alle Test-Dateien in `poppydb/src/test/java/`

- [ ] **Step 1: Package-Deklarationen und Imports in Tests anpassen**

```bash
# Package-Deklaration
find poppydb/src/test/java -name "*.java" -exec sed -i '' \
    's/package de\.caluga\.test\.morphium\.server/package de.caluga.test.poppydb/g' {} +
find poppydb/src/test/java -name "*.java" -exec sed -i '' \
    's/package de\.caluga\.test\.mongo\.suite\.base/package de.caluga.test.poppydb/g' {} +
find poppydb/src/test/java -name "*.java" -exec sed -i '' \
    's/package de\.caluga\.test;/package de.caluga.test.poppydb;/g' {} +

# Imports auf Server-Package → PoppyDB
find poppydb/src/test/java -name "*.java" -exec sed -i '' \
    's/import de\.caluga\.morphium\.server\.MorphiumServer/import de.caluga.poppydb.PoppyDB/g' {} +
find poppydb/src/test/java -name "*.java" -exec sed -i '' \
    's/import de\.caluga\.morphium\.server\./import de.caluga.poppydb./g' {} +

# Klassenreferenzen in Tests
find poppydb/src/test/java -name "*.java" -exec sed -i '' \
    's/MorphiumServer/PoppyDB/g' {} +
find poppydb/src/test/java -name "*.java" -exec sed -i '' \
    's/MorphiumServerTest/PoppyDBTest/g' {} +
```

- [ ] **Step 2: PoppyDBTest.java Klassenname prüfen**

Manuell prüfen: `poppydb/src/test/java/de/caluga/test/poppydb/PoppyDBTest.java`
- Klasse heißt `PoppyDBTest`
- Imports korrekt
- `@Tag("server")` bleibt erhalten

- [ ] **Step 3: Commit — Tests angepasst**

```bash
git add -A
git commit -m "Update test packages and class references for PoppyDB"
```

### Task 7: Kompilierung beider Module prüfen

- [ ] **Step 1: Gesamter Build**

```bash
mvn compile -pl morphium-core,poppydb -q
```

Expected: BUILD SUCCESS

- [ ] **Step 2: Compilefehler fixen**

Falls Fehler auftreten: Vermutlich fehlende/falsche Imports. Einzeln durchgehen und fixen. Typische Probleme:
- Imports auf `de.caluga.morphium.server.*` die noch nicht umgeschrieben sind
- Klassenreferenzen `MorphiumServer` die in String-Literalen stecken (Logs, Error Messages)

- [ ] **Step 3: Commit — Build fixes**

```bash
git add -A
git commit -m "Fix compilation issues after package move"
```

---

## Chunk 3: Assembly, Scripts und Dokumentation

### Task 8: Assembly Descriptor anpassen

**Files:**
- Modify: `poppydb/src/main/assembly/server-cli.xml`

- [ ] **Step 1: Assembly Descriptor prüfen**

Der Descriptor sollte unverändert funktionieren, da er nur Dependencies packt. Prüfen dass `<id>server-cli</id>` passt — ggf. ändern zu `cli`:

```xml
<id>cli</id>
```

Damit wird das Fat-JAR `poppydb-6.2.0-SNAPSHOT-cli.jar` heißen.

- [ ] **Step 2: Fat-JAR bauen und testen**

```bash
mvn package -pl poppydb -am -DskipTests -q
ls -la poppydb/target/poppydb-*-cli.jar
```

Expected: Fat-JAR existiert

- [ ] **Step 3: Fat-JAR Smoke-Test**

```bash
java -jar poppydb/target/poppydb-*-cli.jar --help
```

Expected: Hilfetext wird angezeigt (ohne Crash)

- [ ] **Step 4: Commit — Assembly**

```bash
git add -A
git commit -m "Configure PoppyDB CLI fat-jar assembly"
```

### Task 9: Scripts anpassen

**Files:**
- Rename: `scripts/startMorphiumServer.sh` → `scripts/startPoppyDB.sh`
- Rename: `scripts/morphium_server.sh` → `scripts/poppydb.sh`

- [ ] **Step 1: Scripts umbenennen**

```bash
git mv scripts/startMorphiumServer.sh scripts/startPoppyDB.sh
git mv scripts/morphium_server.sh scripts/poppydb.sh
```

- [ ] **Step 2: Inhalte der Scripts anpassen**

In `scripts/startPoppyDB.sh`:
- Referenzen auf `morphium-server-cli` → `poppydb-cli`
- Referenzen auf `MorphiumServerCLI` → `PoppyDBCLI`
- Referenzen auf `morphium_server` → `poppydb`

In `scripts/poppydb.sh`:
- Funktionsnamen `_ms_local_*` → `_pdb_*` (oder beibehalten falls CI-Scripts sie referenzieren)
- Interne Referenzen auf JAR-Namen und Klassen anpassen
- Referenz auf `MorphiumServer` → `PoppyDB`

- [ ] **Step 3: CI-Script Referenzen prüfen**

Prüfen ob `ci/run-morphium-tests.sh` die Scripts sourced und ggf. die Source-Zeile anpassen:

```bash
grep -n "morphium_server\|startMorphiumServer" ci/run-morphium-tests.sh
```

Anpassen falls nötig.

- [ ] **Step 4: Commit — Scripts**

```bash
git add -A
git commit -m "Rename and update scripts for PoppyDB"
```

### Task 10: Dokumentation aktualisieren

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/morphium-server.md` → `docs/poppydb.md`
- Modify: `docs/security-guide.md`
- Modify: `planned_features.md`

- [ ] **Step 1: README.md — MorphiumServer-Abschnitt umbenennen**

Den Abschnitt "MorphiumServer – The 'Drop-in' Replacement" umschreiben zu "PoppyDB":
- Titel: "PoppyDB — A lightweight, in-memory MongoDB-compatible server"
- Neue Maven-Koordinaten erwähnen
- Fat-JAR-Name aktualisieren
- Messaging-Optimierung hervorheben

- [ ] **Step 2: docs/morphium-server.md → docs/poppydb.md**

```bash
git mv docs/morphium-server.md docs/poppydb.md
```

Inhalt anpassen: alle Referenzen MorphiumServer → PoppyDB.

- [ ] **Step 3: docs/security-guide.md anpassen**

Referenzen auf MorphiumServer durch PoppyDB ersetzen.

- [ ] **Step 4: CHANGELOG.md — Eintrag für 6.2.0**

Neuen Eintrag hinzufügen:

```markdown
## 6.2.0
- **BREAKING (server only):** MorphiumServer extracted into separate module `de.caluga:poppydb` and renamed to PoppyDB
  - Package: `de.caluga.poppydb` (was `de.caluga.morphium.server`)
  - CLI JAR: `poppydb-<version>-cli.jar` (was `morphium-<version>-server-cli.jar`)
  - Main classes: `PoppyDB` / `PoppyDBCLI` (were `MorphiumServer` / `MorphiumServerCLI`)
  - Morphium core library (`de.caluga:morphium`) is unaffected
```

- [ ] **Step 5: planned_features.md anpassen**

Referenzen auf MorphiumServer → PoppyDB.

- [ ] **Step 6: Commit — Dokumentation**

```bash
git add -A
git commit -m "Update documentation for PoppyDB rename"
```

---

## Chunk 4: Abschluss und Verifikation

### Task 11: Gesamter Build + Tests

- [ ] **Step 1: Clean Build beider Module**

```bash
mvn clean compile -q
```

Expected: BUILD SUCCESS

- [ ] **Step 2: morphium-core Tests laufen lassen**

```bash
mvn test -pl morphium-core -Dtest.excludeTags=external,server -q
```

Expected: Tests laufen wie bisher (ohne Server-Tests)

- [ ] **Step 3: poppydb kompiliert und Fat-JAR baut**

```bash
mvn package -pl poppydb -am -DskipTests -q
java -jar poppydb/target/poppydb-*-cli.jar --help
```

Expected: Hilfetext ohne Fehler

- [ ] **Step 4: Commit — Verified build**

```bash
git add -A
git commit -m "Verify clean build of both modules"
```

### Task 12: Aufräumen

- [ ] **Step 1: Prüfen dass keine Referenzen auf MorphiumServer übrig sind**

```bash
# In Source-Dateien (nicht in docs/git-history)
grep -r "MorphiumServer" --include="*.java" morphium-core/src/ poppydb/src/ || echo "Clean!"
grep -r "de\.caluga\.morphium\.server" --include="*.java" poppydb/src/ || echo "Clean!"
grep -r "morphium-server-cli" pom.xml morphium-core/pom.xml poppydb/pom.xml scripts/ || echo "Clean!"
```

Expected: "Clean!" für alle drei

- [ ] **Step 2: Verwaiste Dateien/Verzeichnisse entfernen**

```bash
# Prüfen ob leere Verzeichnisse existieren
find . -type d -empty -not -path './.git/*' | head -20
```

- [ ] **Step 3: Finaler Commit**

```bash
git add -A
git commit -m "Clean up remaining MorphiumServer references"
```

### Task 13: Branch bereit für Review

- [ ] **Step 1: Übersicht der Änderungen**

```bash
git log --oneline master..HEAD
git diff --stat master..HEAD
```

- [ ] **Step 2: Rücksprache vor Push**

Branch `feature/poppydb-extraction` ist fertig. Vor Push Rücksprache mit Stephan halten.
