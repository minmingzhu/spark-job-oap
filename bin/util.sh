declare -r JOB_DIR="$SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID/"
copy_spark_logs() {
	echo "2"
	ssh_exe=$1
	appl_id=$2
	echo $appl_id
	echo "copying spark executor logs ..."
	log_dest="$JOB_DIR/"
	[[ ! -d $log_dest ]] && mkdir -p $log_dest

	# copy log file
	for node in `cat $SPARK_CONF_DIR/slaves`
	do
		$ssh_exe $node "if [[ -d $SPARK_WORKER_DIR/$appl_id ]]; then
			cp -r $SPARK_WORKER_DIR/* $log_dest
			fi" 2>&1 | sed "s/^/$node: /" &
	done
}

copy_rss_logs() {
	ssh_exe=$1
	app_id=$2
	if [[ $SPARKJOB_SHUFFLE_SERVERS == 0 ]];then
		echo "no rss log"
	else
		echo "copying rss logs ..."
		log_dest="$JOB_DIR/$app_id/rss"
		[[ ! -d $log_dest ]] && mkdir -p $log_dest
		# copy log file
		for host in `cat $RSS_CONF_DIR/nodes`
        	do
			ssh_exe $host "if [[ -d $RSS_LOG_DIR ]]; then
				cp -r $RSS_LOG_DIR/* $log_dest
				fi" 2>&1 | sed "s/^/$host: /" &
	        done
	fi	
}

copy_logs() {
	# find master log file
	declare -r master_log_regex="spark.+master.+"
	declare MASTER_LOG_FILE=
	for f in $SPARKJOB_WORKING_DIR/logs/*
	do
		if [[ $f =~ $master_log_regex ]]; then
			MASTER_LOG_FILE="$f"
			break;
		fi
	done

	if [[ ! -s ${MASTER_LOG_FILE} ]]; then
		echo "spark job not run since no master log found under $SPARKJOB_WORKING_DIR/logs/"
		exit $?
	fi
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
		echo "no application id found, skip log copying"
		copy_log=0
	fi
	# copy spark executor log, and rss log if any
	if [[ $copy_log == 1 ]];then
		echo "1"
		ssh_exe=$(which ssh)
		echo $ssh_exe
		copy_spark_logs $ssh_exe $app_id
	fi
}
