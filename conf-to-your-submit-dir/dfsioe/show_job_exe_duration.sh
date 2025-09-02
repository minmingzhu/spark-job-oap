#!/usr/bin/env bash

jobid=$1

parse_duration() {
	lines=$1
	wl=$2
	tp=$3
	regex=".*finished[[:space:]]in[[:space:]]([0-9]+.*[0-9]*)[[:space:]]s"

        declare -a durs
        while IFS= read -r line; do
                #echo "$line"
                if [[ "$line" =~ $regex ]]; then
                        durs+=("${BASH_REMATCH[1]}")
                        #echo "${BASH_REMATCH[1]}"
                fi
        done <<< "$lines"

        if [[ -z "${durs[@]}" ]]; then
                echo "no job durations found"
                return 1
        fi

	if [[ "$wl" == "dfsioe" ]]; then
        	echo "dfsioe $tp warmup duration   : ${durs[0]} s"
        	echo "dfsioe $tp 2nd round duration: ${durs[1]} s"
	else
		echo "repartition shuffle $tp duration: ${durs[0]} s"
	fi
	return 0
}

logfile=${jobid}
if [[ ! "$jobid" =~ .+\.ER ]]; then
	logfile=${logfile}.ER
fi


rst=$(grep "ResultStage .* (foreach at ScalaDFSIOWriteOnly.scala:75) finished in .\+ s" "$logfile")

# dfsioe
if [[ ! -z "$rst" ]]; then
	parse_duration "$rst" "dfsioe" "write"
	if (( $? )); then
		echo "failed to find dfsioe write duration from log file, $logfile"
	fi
	echo "============================================"
	rst=$(grep "ResultStage .* (foreach at ScalaDFSIOReadOnly.scala:61) finished in .\+ s" "$logfile")
	parse_duration "$rst" "dfsioe" "read"
	if (( $? )); then
                echo "failed to find dfsioe read duration from log file, $logfile"
        fi
	exit 0
else
	# repartiton
	rst=$(grep "ShuffleMapStage 1 (mapPartitionsWithIndex at ScalaInMemRepartition.scala:77) finished in .\+ s" "$logfile")
	parse_duration "$rst" "repartition" "write"
	if (( $? )); then
                echo "failed to find repartition shuffle write duration from log file, $logfile"
        fi
	echo "============================================"
	rst=$(grep "ResultStage 2 (foreach at ScalaInMemRepartition.scala:104) finished in .\+ s" "$logfile")
	parse_duration "$rst" "repartition" "read"
	if (( $? )); then
                echo "failed to find repartition shuffle read duration from log file, $logfile"
        fi
fi
