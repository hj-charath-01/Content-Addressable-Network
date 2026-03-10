#!/bin/bash
set -e

SRC=src/main/java
OUT=out

mkdir -p $OUT
find $SRC -name "*.java" > /tmp/srcs.txt
javac -d $OUT @/tmp/srcs.txt
echo "compiled ok"
java -cp $OUT com.can.simulation.CANSimulation