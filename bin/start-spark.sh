#! /bin/bash
set -u

[[ -z ${SPARKJOB_JOBID+X} ]] &&
	declare SPARKJOB_JOBID=$COBALT_JOBID    # Change it for other job system
export SPARKJOB_JOBID
echo "SPARKJOB_JOBID is $SPARKJOB_JOBID"
# Set the directory containing our scripts if unset.
# SPARKJOB_SCRIPTS_DIR is passed to the job via qsub.
[[ -z ${SPARKJOB_SCRIPTS_DIR+X} ]] &&
	declare SPARKJOB_SCRIPTS_DIR="$(cd $(dirname "$0")&&pwd)"
export SPARKJOB_SCRIPTS_DIR
[[ -z ${SPARKJOB_CONFIG_DIR+X} ]] &&
        declare SPARKJOB_CONFIG_DIR="$(pwd)"
export SPARKJOB_CONFIG_DIR
[[ -z ${SPARKJOB_OUTPUT_DIR+X} ]] &&
	declare SPARKJOB_OUTPUT_DIR="$(pwd)"
export SPARKJOB_OUTPUT_DIR

source "$SPARKJOB_SCRIPTS_DIR/setup.sh"
echo "sourced $SPARKJOB_SCRIPTS_DIR/setup.sh"

[[ -d $SPARK_WORKER_DIR ]] || mkdir -p "$SPARK_WORKER_DIR"
[[ -d $SPARK_CONF_DIR ]] || mkdir -p "$SPARK_CONF_DIR"
[[ -d $SPARK_LOG_DIR ]] || mkdir -p "$SPARK_LOG_DIR"

# copy jars to node's tmp folder and use "local://" file to submit
declare -a arguments=()
for p in "$@"
do
        if [[ "$p" =~ .*\.jar ]]; then
                if [[ ! -f "$p" ]]; then
                        echo "Error: jar not exists, $p"
                        exit 1
                fi
		while read -r line;
		do
			scp $p $line:/tmp
		done < "$COBALT_NODEFILE"
                arguments+=("local:///tmp/$(basename $p)")
        else
                arguments+=("$p")
        fi
done

cp "$COBALT_NODEFILE" "$SPARK_CONF_DIR/nodes"
"$SPARKJOB_SCRIPTS_DIR/run-spark.sh" "${arguments[@]}"
