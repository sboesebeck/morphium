#!/bin/bash
#echo "Checking for JDK 1.8"
#java -version 2>&1 | grep 11. || { echo "Wrong java version, exiting"; exit 2; }

branch=$(git symbolic-ref --short HEAD)

if [ $branch == "master" ]; then
  echo "Do not release from master branch"
  exit 1
fi

if [ $branch != "develop" ]; then
  echo "you should only run publish from develop branch? Continue? <enter> | CTRL-C"
  read
fi
mvn clean compile >/dev/null || exit 1

mvn clean release:clean release:prepare || exit 1
version=$(grep "project.rel.de.caluga\\\\\\:morphium" release.properties | cut -f2 -d=)
tag=$(grep "scm.tag=" release.properties | cut -f2 -d=)

echo "Releasing $version - tagging as $tag"
mvn release:perform >>test.log/release.log
# mvn nexus-staging:release -Ddescription="Latest release" -DstagingRepositoryId="sonatype-nexus-staging" ||

git checkout master
git merge $tag
git push

./publish_to_central.sh

#./create_bundle >> test.log/release.log
git checkout develop
git push
#. ./slackurl.inc
#curl -X POST -H 'Content-type: application/json' --data "{'text':'Deployed current $version to sonatype oss and maven central'}" $SLACKURL
# mvn nexus-staging:release -Ddescription="Latest release" -DstagingRepositoryId="sonatype-nexus-staging" ||
