#!/bin/bash

CLASS=com.hazelblast.server.ServerMain

ROOT=$(cd $(dirname $0) && pwd)
cd $ROOT

JAVA_OPTS="-Dhazelcast.lite.member=false -DpuFactory=com.hazelblast.server.spring.SpringPuFactory"
export CP=`ls target/employee-management-server-*.jar | awk '{print $1}'`

if [ -z "$CP" ]; then
	echo "Cannot find the target/employee-management-server-*.jar"
	exit 1
fi

echo running demo for $CLASS from $CP at $@

pwd
echo java $JAVA_OPTS -cp "$CP" $CLASS $@
java $JAVA_OPTS -cp "$CP" $CLASS $@