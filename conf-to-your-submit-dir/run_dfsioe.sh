#!/usr/bin/env bash
#

usage="Usage:
1. no argument: '... -s ./run_dfsioe.sh', execute 'run.sh' under <HiBench Dir>/bin/workloads/micro/dfsioe/spark
2. dfs write only: '... -s ./run_dfsioe.sh write', execute 'run_write.sh' under <HiBench Dir>/bin/workloads/micro/dfsioe/spark
3. dfs read only: '... -s ./run_dfsioe.sh read', execute 'run_read.sh' under <HiBench Dir>/bin/workloads/micro/dfsioe/spark
"


UTIL_DIR=/home/jiafuzha/working
JAR_DIR=/home/jiafuzha/jars
HIBENCH_DIR=/home/jiafuzha/HiBench

check_file() {
        if [ ! -f "$1" ]; then
                echo "$1 not exists. Exiting..."
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

# give spark components more time to be ready
sleep 10

cd $HIBENCH_DIR/bin/workloads/micro/dfsioe/spark

if [[ "$#" == 0 ]]; then
	check_file "$PWD/run.sh"
	./run.sh
elif [[ "$#" == 1 ]]; then
	if [[ "$1" == "write" ]]; then
		check_file "$PWD/run_write.sh"
		./run_write.sh
	elif [[ "$1" == "read" ]]; then
		check_file "$PWD/run_read.sh"
		./run_read.sh
	else
		echo "invalid argument $1"
		echo "$usage"
	fi
else
	echo "illegal arguments $@"
	echo "$usage"
fi

