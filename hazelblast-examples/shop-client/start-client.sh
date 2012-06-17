#!/bin/bash

CLASS=com.shop.ClientMain

ROOT=$(cd $(dirname $0) && pwd)
cd $ROOT

JAVA_OPTS="-Dhazelcast.lite.member=true -Djava.net.preferIPv4Stack=true -Dhazelcast.map.partition.count=25 -Dlog4j.configuration=file:log4j.xml"

export CP=$CP:target/dependency/*:target/*

echo running demo for $CLASS from $CP at $@
pwd

echo java $JAVA_OPTS -cp "$CP" $CLASS $@
java $JAVA_OPTS -cp "$CP" $CLASS $@