#!/usr/bin/env bash

JAR_DIR=/lus/flare/projects/Aurora_deployment/spark/HiBench/jars
HIBENCH_DIR=/lus/flare/projects/Aurora_deployment/spark/HiBench

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


check_file "$SPARKJOB_CONFIG_DIR/loop.sh"
check_file "$JAR_DIR/sparkbench-assembly-8.0-SNAPSHOT-dist.jar"
check_dir "$HIBENCH_DIR"

$SPARKJOB_CONFIG_DIR/loop.sh "pdsh -w ?? mkdir -p /var/tmp/spark/hibench_jars/"
$SPARKJOB_CONFIG_DIR/loop.sh "scp $JAR_DIR/sparkbench-assembly-8.0-SNAPSHOT-dist.jar ?:/var/tmp/spark/hibench_jars/"

cd $HIBENCH_DIR/bin/workloads/micro/repartition/spark

check_file "$PWD/run.sh"

./run.sh
