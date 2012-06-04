#!/bin/bash

CLASS=com.shop.ClientMain

ROOT=$(cd $(dirname $0) && pwd)
cd $ROOT

JAVA_OPTS="-Dhazelcast.lite.member=true"

export CP=$CP:target/dependency/*

echo running demo for $CLASS from $CP at $@
pwd

echo java $JAVA_OPTS -cp "$CP" $CLASS $@
java $JAVA_OPTS -cp "$CP" $CLASS $@