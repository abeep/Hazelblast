#!/bin/bash

CLASS=com.hazelblast.server.ServiceContextServer

ROOT=$(cd $(dirname $0) && pwd)
cd $ROOT

JAVA_OPTS="-Dhazelcast.lite.member=false -Dhazelcast.map.partition.count=25 -Dhazelcast.logging.type=log4j -Dlog4j.configuration=file:log4j.xml -Djava.net.preferIPv4Stack=true"

export CP=$CP:target/dependency/*:target/*

echo running demo for $CLASS from $CP at $@

pwd
echo java $JAVA_OPTS -cp "$CP" $CLASS $@
java $JAVA_OPTS -cp "$CP" $CLASS -serviceContextFactory com.hazelblast.server.spring.SpringServiceContextFactory
