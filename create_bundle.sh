#!/bin/bash
mvn package gpg:sign -Dgpg.passphrase="$GPGPWD" -Dmaven.test.skip=true $@ || exit 1
echo "creating bundle"
version=$(grep '<version>' pom.xml | head -n1 | tr -d ' a-z<>/')
echo "Version $version"
cd target
jar -cvf bundle_${version}.jar morphium-${version}*
