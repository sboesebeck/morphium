#!/bin/bash
mvn package verify -DskipTests || exit 1
echo "creating bundle"
version=$(grep '<version>' pom.xml | head -n1 | tr -d ' a-z<>/')
echo "Version $version"
cd target
jar -cvf bundle_${version}.jar morphium-${version}.pom morphium-${version}.pom.asc morphium-${version}.jar.asc morphium-${version}.jar morphium-${version}-javadoc.jar morphium-${version}-javadoc.jar.asc morphium-${version}-sources.jar.asc morphium-${version}-sources.jar

