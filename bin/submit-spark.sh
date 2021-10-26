#! /bin/bash
set -u

# Set the directory containing our scripts if unset.
[[ -z ${SPARKJOB_SCRIPTS_DIR+X} ]] && \
	declare SPARKJOB_SCRIPTS_DIR="$(cd $(dirname "$0")&&pwd)"

declare -r configdir="$(pwd)"
declare -r version='Spark DAOS Job  v1.0.3'
declare -r usage="$version"'

Usage:
	submit-spark.sh [options] JOBFILE [arguments ...]

JOBFILE can be:
	script.py			pyspark scripts
	run-example examplename		run Spark examples, like "SparkPi"
	--class classname example.jar	run Spark job "classname" wrapped in example.jar
	shell-script.sh			when option "-s" is specified

Required options:  
	-t WALLTIME		Max run time in minutes
	-n NODES		Job node count
	-q QUEUE		Queue name

Optional options:
	-A PROJECT		Allocation name
	-o OUTPUTDIR		Directory for job output files (default: current dir)
	-s			Enable script mode
	-m			Master uses a separate node
	-I			Start an interactive ssh session
	-w WAITTIME		Time to wait for prompt in minutes (default: 30)
	-b			Prefer Spark built-in mllib to default OAP mllib 
	-h			Print this help messages
Example:
	submit-spark.sh -t 60 -n 2 -q arcticus dense_kmeans_example.py daos://pool0/cont1/kmeans/input/libsvm 10
	submit-spark.sh -t 30 -n 1 -q arcticus -o output-dir run-example SparkPi
	submit-spark.sh -I -t 30 -n 2 -q arcticus --class com.intel.jlse.ml.KMeansExample example/jlse-ml-1.0-SNAPSHOT.jar daos://pool0/cont1/kmeans/input/csv csv 10
	submit-spark.sh -t 30 -n 1 -q arcticus -s example/test_script.sh job.log
'

while getopts :hmsIbA:t:n:q:w:p:o: OPT; do
	case $OPT in
	A)	declare -r	allocation="$OPTARG";;
	t)	declare -r	time="$OPTARG";;
	s)	declare -ir	scriptMode=1;;
	n)	declare -r	nodes="$OPTARG";;
	q)	declare -r	queue="$OPTARG";;
	o)	declare -r	outputdir="$OPTARG";;
	m)	declare -ir	separate_master=1;;
	w)	declare -ir	waittime=$((OPTARG*60));;
	b)	declare -ir	oapml=0;;
	I)	declare -ir	interactive=1;;
	h)	echo "$usage"; exit 0;;
	?)	
		if [[ $OPTARG == - ]];then
                        # Assume we see a double dash `--option` that should be passed to `spark-submit`.
                        break
                else
			echo "submit-spark.sh: illegal option: -$OPTARG"
			echo "$usage"
			exit 1
		fi
		;;
	esac
done

[[ -z ${waittime+X} ]] && declare -ir waittime=$((30*60))
[[ -z ${scriptMode+X} ]] && declare -ir scriptMode=0
[[ -z ${outputdir+X} ]] && declare -r outputdir=.
[[ -z ${separate_master+X} ]] && declare -ir separate_master=0
[[ -z ${oapml+X} ]] && declare -ir oapml=1 

if [[ -z ${time+X} || -z ${nodes+X} || -z ${queue+X} ]];then
	echo "$usage"
	exit 1
fi

shift $((OPTIND-1))

declare -a scripts=()

if (($#>0));then
	[[ -z ${interactive+X} ]] && declare -ir interactive=0
	scripts=( "$@" )
	echo "# Submitting job: ${scripts[@]}"
else
	[[ -z ${interactive+X} ]] && declare -ir interactive=1
	echo "Submitting an interactive job and wait for at most $waittime sec."
fi

if [[ ! -d $outputdir ]];then
	if ! mkdir "$outputdir";then
		echo "Cannot create directory: $outputdir"
		exit 1
	fi
fi

declare SPARKJOB_OUTPUT_DIR="$(cd $outputdir&&pwd)"
declare SPARKJOB_CONFIG_DIR=$configdir
declare SPARKJOB_INTERACTIVE=$interactive
declare SPARKJOB_SCRIPTMODE=$scriptMode
declare SPARKJOB_SEPARATE_MASTER=$separate_master
declare SPARKJOB_OAPML=$oapml

declare -i SPARKJOB_JOBID=0
mysubmit() {
	# Options to pass to qsub
	local -a opt=(
		--attrs 'enable_ssh=1'
		-n $nodes -t $time -q $queue
		--env "SPARKJOB_SCRIPTS_DIR=$SPARKJOB_SCRIPTS_DIR"
		--env "SPARKJOB_CONFIG_DIR=$SPARKJOB_CONFIG_DIR"
		--env "SPARKJOB_INTERACTIVE=$SPARKJOB_INTERACTIVE"
		--env "SPARKJOB_SCRIPTMODE=$SPARKJOB_SCRIPTMODE"
		--env "SPARKJOB_OUTPUT_DIR=$SPARKJOB_OUTPUT_DIR"
		--env "SPARKJOB_SEPARATE_MASTER=$SPARKJOB_SEPARATE_MASTER"
		--env "SPARKJOB_OAPML=$SPARKJOB_OAPML"
		-O "$SPARKJOB_OUTPUT_DIR/\$jobid"
		"$SPARKJOB_SCRIPTS_DIR/start-spark.sh"
	)

	if ((${#scripts[@]}>0));then
		opt+=("${scripts[@]}")
	fi
	[[ ! -z ${allocation+X} ]] && opt=(-A $allocation "${opt[@]}")

	SPARKJOB_JOBID=$(qsub "${opt[@]}")
	export SPARKJOB_JOBID
	if ((SPARKJOB_JOBID > 0));then
		echo "# Submitted"
		echo "SPARKJOB_JOBID=$SPARKJOB_JOBID"
	else
		echo "# Submitting failed."
		exit 1
	fi
}

if ((interactive>0)); then
	cleanup(){ ((SPARKJOB_JOBID>0)) && qdel $SPARKJOB_JOBID; }
	trap cleanup 0
	mysubmit
	declare -i mywait=1 count=0
	source "$SPARKJOB_SCRIPTS_DIR/setup-common.sh" 0
	JOB_PATH=$SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID
	echo "Waiting for Spark to launch by checking $JOB_PATH"
	for ((count=0;count<waittime;count+=mywait));do
		[[ ( ! -f $JOB_PATH.error ) && ( ! -f $JOB_PATH.output ) ]] || break
		sleep 1 
	done
	[[ -s $SPARKJOB_WORKING_ENVS ]] || SPARKJOB_WORKING_ENVS=$SPARKJOB_OUTPUT_DIR/.spark-env
	if [[ -s $SPARKJOB_WORKING_ENVS ]];then
		source "$SPARKJOB_WORKING_ENVS"
		echo "# Spark is now running (SPARKJOB_JOBID=$SPARKJOB_JOBID) on:"
		column "$SPARK_CONF_DIR/slaves" | sed 's/^/# /'
		declare -p SPARK_MASTER_URI
		declare -ar sshmaster=(ssh -o ControlMaster=no -t $MASTER_HOST)
		declare -r runbash="exec bash --rcfile <(
			echo SPARKJOB_JOBID=\'$SPARKJOB_JOBID\';
			echo SPARKJOB_SCRIPTS_DIR=\'$SPARKJOB_SCRIPTS_DIR\';
			echo SPARKJOB_INTERACTIVE=\'$SPARKJOB_INTERACTIVE\';
			echo SPARKJOB_SCRIPTMODE=\'$SPARKJOB_SCRIPTMODE\';
			echo SPARKJOB_OUTPUT_DIR=\'$SPARKJOB_OUTPUT_DIR\';
			echo SPARKJOB_SEPARATE_MASTER=\'$SPARKJOB_SEPARATE_MASTER\';
			echo SPARKJOB_OAPML=\'$SPARKJOB_OAPML\';
			echo SPARKJOB_CONFIG_DIR=\'$SPARKJOB_CONFIG_DIR\';
			echo source ~/.bashrc;
			echo source \'$SPARKJOB_SCRIPTS_DIR/setup.sh\';
			echo cd \'$SPARKJOB_OUTPUT_DIR\';
		        echo mkdir -p $SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID/loggedin	) -i"
		echo "# Spawning bash on host: $MASTER_HOST"

		"${sshmaster[@]}" "$runbash"
		rm -rf $SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID/loggedin	
	else
		echo "Spark failed to launch within $((waittime/60)) minutes."
	fi
else
	mysubmit
fi
