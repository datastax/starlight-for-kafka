#!/bin/bash
IMAGETAG=$1

set -x -e

if [[ ! -f "async-profiler-2.9-linux-x64.tar.gz" ]] ; then
    curl -L -O https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz
fi

mvn clean install -DskipTests -pl kafka-impl,proxy -f ../../pom.xml
rm -f *.nar
cp ../../proxy/target/*.nar .
cp ../../kafka-impl/target/*.nar .
docker build . -t $IMAGETAG
docker push $IMAGETAG
