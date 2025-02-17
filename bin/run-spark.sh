#! /bin/bash
set -u


# This script has to be run on the actual compute node for master.
# It is designed to be called from start-spark.sh

source "$SPARKJOB_SCRIPTS_DIR/setup.sh"


if [[ ! -s $SPARK_CONF_DIR/nodes ]];then
	echo "Unable to get the nodes file: $SPARK_CONF_DIR/nodes"
	exit 1
fi
[[ -z ${SPARKJOB_SEPARATE_MASTER+X} ]] && declare -ir SPARKJOB_SEPARATE_MASTER=0

if ((SPARKJOB_SEPARATE_MASTER>0));then
	echo "dedicated spark master. removing $(hostname) from spark workers"
	grep -v "$(hostname)" "$SPARK_CONF_DIR/nodes" > "$SPARK_CONF_DIR/slaves"
else
	cp -a "$SPARK_CONF_DIR/nodes" "$SPARK_CONF_DIR/slaves"
fi

ssh(){	# Intercept ssh call to pass more envs.  Requires spark using bash.
	# This is a exported function.  Any global variables used here should be exported.
	#echo "[[ Hijacked ssh: $@ from host $(hostname)]"
	#export -p | grep SPARK
	#echo "]"
	local -a os cs
	while [[ $1 == -* ]];do
		os+=("$1" "$2")
		shift 2
	done
	local -r h="$1";shift
	local -ar cs=("$@")
	#echo "Saving ssh output to $SPARKJOB_WORKING_DIR/ssh.$h.output"
	#echo "Saving ssh error to $SPARKJOB_WORKING_DIR/ssh.$h.error"
	# ControlMaster has issues with compute nodes
	/usr/bin/ssh -o ControlMaster=no \
		"${os[@]}" "$h" "bash -lc \"
		SPARKJOB_HOST='$SPARKJOB_HOST' ; 
		SPARKJOB_SCRIPTS_DIR='$SPARKJOB_SCRIPTS_DIR' ;
	        SPARKJOB_CONFIG_DIR='$SPARKJOB_CONFIG_DIR' ;	
		SPARKJOB_OUTPUT_DIR='$SPARKJOB_OUTPUT_DIR' ; 
		SPARKJOB_WORKING_DIR='$SPARKJOB_WORKING_DIR' ;  
		SPARKJOB_OAPML='$SPARKJOB_OAPML' ;
		SPARKJOB_DAOS='$SPARKJOB_DAOS' ;
		source '$SPARKJOB_SCRIPTS_DIR/setup.sh' ; 
		${cs[@]} \""
	#	>>'$SPARKJOB_WORKING_DIR/ssh.$h.output' 
	#	2>>'$SPARKJOB_WORKING_DIR/ssh.$h.error'\""
	local -ir st=$?
	#echo "[ Hijacked ssh returned with status: $st]"
	((st==0)) || return $st
	if mkdir -p "$SPARKJOB_WORKING_ENVS.lock">/dev/null 2>&1;then	# We use POSIX mkdir for a mutex.
	{
		declare -p | grep SPARK	# Get SPARK related envs.
		echo "declare -x SPARK_MASTER=${cs[${#cs[@]}-1]}"
		echo "declare -x MASTER_HOST=$(hostname)"
	} > "$SPARKJOB_WORKING_ENVS"
	echo "spark env file created: $SPARKJOB_WORKING_ENVS"
	fi	# We don't release the mutex here, because we only need one copy of env.
	return $st
}
export -f ssh


# export SPARK_SSH_FOREGROUND=yes
# start spark master first
$SPARK_HOME/sbin/start-master.sh
echo "checking spark master listens on worker connection"
# find master log file
declare -r master_log_regex="spark.+master.+"
declare MASTER_LOG_FILE=
# max wait 60s
declare -i maxwait_0=30 count_0=0
for ((count_0=0;count_0<maxwait_0;count_0+=1));do
        for f in $SPARKJOB_WORKING_DIR/logs/*
        do
                if [[ $f =~ $master_log_regex ]]; then
                        MASTER_LOG_FILE="$f"
                        break;
                fi
        done
        if [ ! -z "$MASTER_LOG_FILE" ]; then
                break;
        fi
        echo "master log not created yet. sleep 2 seconds ..."
        sleep 2
done

if [ -z "$MASTER_LOG_FILE" ]; then
        echo "spark job not run since spark master log not found after waiting for 1 min, existing"
        exit $?
fi

# check if spark master started successfully
# max wait 60s
maxwait_0=12
count_0=0
spark_master_started=
for ((count_0=0;count_0<maxwait_0;count_0+=1));do
        if [ ! -s "${MASTER_LOG_FILE}" ]; then
                echo "master log is empty, sleeping 5 seconds ..."
                sleep 5
	else
		spark_master_started=$(grep "Successfully started service 'MasterUI'" ${MASTER_LOG_FILE})
		if [ ! -z "${spark_master_started}" ]; then
			break;
		fi
		sleep 5
	fi
done

if [ ! -s "${MASTER_LOG_FILE}" ]; then
        echo "spark job not run since spark master log is empty after waiting for 1 min, under $SPARKJOB_WORKING_DIR/logs/"
        exit $?
fi

if [ -z "${spark_master_started}" ]; then
        echo "spark job not run since spark master failed to start after waiting for 1 min, under $SPARKJOB_WORKING_DIR/logs/"
        exit $?
fi

# start spark workers
$SPARK_HOME/sbin/start-workers.sh
echo "waiting 10 seconds for workerers to startup ..."
sleep 10

source "$SPARKJOB_WORKING_ENVS"
echo $SPARK_MASTER
echo "spark.master $SPARK_MASTER" >> "$SPARK_CONF_DIR/spark-defaults.conf"
# startup shuffle server as necessary
# $SPARKJOB_SCRIPTS_DIR/start-shuffle-server.sh

# Clean up our mutex here see the use in function ssh above.
rmdir "$SPARKJOB_WORKING_ENVS.lock"

echo "Spark daemons started"
if (($#>0));then
	echo "The first arg is ARG_START:$1:ARG_END"
	# We have jobs to submit
	if ((SPARKJOB_SCRIPTMODE>0));then
		script=$(to_abs_path_if_nc $1)
		echo "running script $script"
		$script "${@:2}"
	elif [[ $1 == run-example ]];then
		echo "running spark example ${@:2}"
		"$SPARK_HOME/bin/run-example" --master $SPARK_MASTER $GPU_OPTIONS "${@:2}"
	elif [[ $1 == spark-submit ]];then
		echo "submitting spark job $@"
		"$SPARK_HOME/bin/spark-submit" --master $SPARK_MASTER $GPU_OPTIONS "$@"
        else
               echo "submitting spark job from script $@"
               script_file=$1
               echo $script_file
               "$SPARKJOB_CONFIG_DIR/$script_file" "${@:2}"
	fi
else
	echo "nothing to submit to spark. Run any script by yourself in interactive mode"
fi

# copy executor logs to job dir
# find application ID
declare -r regex=".+Registered.+(app\-[0-9]+\-[0-9]+)"
while read -r line
do
        if [[ $line =~ $regex ]]; then
                declare -r app_id=${BASH_REMATCH[1]}
                break;
        fi
done < "$MASTER_LOG_FILE"

copy_log=1
if [[ ! -z ${app_id+x} ]]; then
        echo "got application ID: $app_id"
else
        echo "no application id found"
	copy_log=0
fi

if [[ $copy_log == 1 ]];then
	declare -r EXE_LOG_DIR="$SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID/$app_id"
	[[ ! -d $EXE_LOG_DIR ]] && mkdir $EXE_LOG_DIR

	unset -f ssh

	# copy log file
	while read -r host
	do
		echo "$host ..."
		ssh -n $host "if [[ -d $SPARK_WORKER_DIR/$app_id ]]; then 
				find $SPARK_WORKER_DIR/$app_id -name '*.jar' -delete
				cp -r $SPARK_WORKER_DIR/$app_id/* $EXE_LOG_DIR
		  	fi"
		echo "$host $?"
	done < "$SPARK_CONF_DIR/slaves"
fi
# wait for interactive
if ((SPARKJOB_INTERACTIVE>0));then
	echo "waiting 5 minutes for interactive login..."
	declare -i maxwait=300 count=0
	for ((count=0;count<maxwait;count+=1));do
		[[ -d $SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID/loggedin ]] && break
                sleep 1
	done
	# wait user exiting
	count=1
	while ((count>0));do
		[[ ! -d $SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID/loggedin ]] && break
		sleep 5
		echo "waiting user exiting"
	done

fi
