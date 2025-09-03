#!/usr/bin/env bash

UTIL_DIR=/home/jiafuzha/working
JAR_DIR=/home/jiafuzha/jars
HIBENCH_DIR=/home/jiafuzha/HiBench

check_file() {
	if [ ! -f "$1" ]; then
        	echo "file $1 not exists. Exiting..."
        	exit 1
	fi
}

check_dir() {
        if [ ! -d "$1" ]; then
                echo "directory $1 not exists. Exiting..."
                exit 1
        fi
}


check_file "$UTIL_DIR/loop.sh"
check_file "$JAR_DIR/sparkbench-assembly-8.0-SNAPSHOT-dist.jar"
check_dir "$HIBENCH_DIR"

$UTIL_DIR/loop.sh "pdsh -w ?? mkdir -p /var/tmp/spark/hibench_jars/"
$UTIL_DIR/loop.sh "scp $JAR_DIR/sparkbench-assembly-8.0-SNAPSHOT-dist.jar ?:/var/tmp/spark/hibench_jars/"

cd $HIBENCH_DIR/bin/workloads/micro/repartition/spark

check_file "$PWD/run.sh"

./run.sh
