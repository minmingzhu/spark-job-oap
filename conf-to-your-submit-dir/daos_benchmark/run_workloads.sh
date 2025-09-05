#!/usr/bin/env bash
#

usage="Usage:
'... run_workloads.sh <apps_dir> <pool> <workload> <true|false, 1|0> <optional sleep seconds after container recreation>
workload==dfsioe, execute 'run_write.sh' and 'run_read.sh' under <apps_dir>/HiBench/bin/workloads/micro/dfsioe/spark
workload==repartition, execute 'run.sh' under <apps_dir>/HiBench/bin/workloads/micro/repartition/spark

The fourth argument, <true|false, 1|0>, is option for use clean containers by recreating them.
"

APPS_DIR=$1

JAR_DIR=/lus/flare/projects/Aurora_deployment/spark/HiBench/jars
HIBENCH_DIR=/lus/flare/projects/Aurora_deployment/spark/HiBench

# set python path
export PATH=$APPS_DIR/python2/bin:$PATH
python_path=$(which python)
echo "using python $python_path"

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

recreate_container() {
	p=$1
	c=$2
	t=$3
	msg=$(daos cont query $p $c)
        if (($?)); then
		echo "container $c not existing under $p"
	else
		daos cont destroy $p $c -f
                if (($?)); then
			echo "Error: failed to destroy container $c. exiting"
                        exit 1
                fi
	fi
	if [[ -z "$t" ]]; then
		daos cont create $p $c
	else
		daos cont create --type "$t" $p $c
	fi
}

if [[ "$#" < 4 ]]; then
	echo "need no less than 4 parameters."
	echo "$usage"
	exit 1
fi

workload=$3
if [[ "$workload" == "dfsioe" ]]; then
        script_folder="$HIBENCH_DIR/bin/workloads/micro/dfsioe/spark"
elif [[ "$workload" == "repartition" ]]; then
        script_folder="$HIBENCH_DIR/bin/workloads/micro/repartition/spark"
else
	echo "invalid workload $workload. exiting"
	exit 1
fi

check_file "$SPARKJOB_CONFIG_DIR/loop.sh"
check_file "$JAR_DIR/sparkbench-assembly-8.0-SNAPSHOT-dist.jar"
check_dir "$HIBENCH_DIR"

$SPARKJOB_CONFIG_DIR/loop.sh "pdsh -w ?? mkdir -p /var/tmp/spark/hibench_jars/"
$SPARKJOB_CONFIG_DIR/loop.sh "scp $JAR_DIR/sparkbench-assembly-8.0-SNAPSHOT-dist.jar ?:/var/tmp/spark/hibench_jars/"


cd $script_folder


if [[ "$4" == "true" || "$4" == "1" ]]; then
	pool=$2
	echo "Trying to re-create containers, hadoop_fs and spark_shuffle to restore space"
	recreate_container "$pool" "hadoop_fs" "POSIX"
	recreate_container "$pool" "spark_shuffle" ""
	if [[ "$#" > 4 ]];then
		echo "WARNING: sleep $5 s for waiting space reclaiming"
		sleep "$5"
	else
		echo "WARNING: sleep 30 s for waiting space reclaiming"
		sleep 30
	fi
else
	# give spark components more time to be ready
	sleep 10
fi

if [[ "$workload" == "dfsioe" ]]; then
	check_file "$PWD/run_write.sh"
	check_file "$PWD/run_read.sh"
	# pool already updated in submission script
	./run_write.sh
	./run_read.sh
else
	check_file "$PWD/run.sh"
	./run.sh
fi
