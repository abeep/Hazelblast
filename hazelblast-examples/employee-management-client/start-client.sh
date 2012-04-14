#!/bin/bash

CLASS=com.employee.ClientMain

ROOT=$(cd $(dirname $0) && pwd)
cd $ROOT

JAVA_OPTS="-Dhazelcast.lite.member=true"
export CP=`ls target/employee-management-client-*.jar | awk '{print $1}'`

if [ -z "$CP" ]; then
	echo "Cannot find the target/employee-management-client-*.jar"
	exit 1
fi

echo running demo for $CLASS from $CP at $@

pwd

echo java $JAVA_OPTS -cp "$CP" $CLASS $@
java $JAVA_OPTS -cp "$CP" $CLASS $@