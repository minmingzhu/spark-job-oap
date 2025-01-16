#declare -r EXE_LOG_DIR="$SPARKJOB_OUTPUT_DIR/$SPARKJOB_JOBID/$app_id"
#$SPARK_WORKER_DIR/$app_id
#$SPARK_CONF_DIR/slaves

EXE_LOG_DIR="$1"
WORKER_LOG_DIR="$2"
SLAVES_PATH="$3"



[[ ! -d $EXE_LOG_DIR ]] && mkdir $EXE_LOG_DIR


# copy log file
while read -r host
do
	echo "$host ..."
	ssh -n $host "if [[ -d $WORKER_LOG_DIR ]]; then
	                find $WORKER_LOG_DIR -name '*.jar' -delete
		      	cp -r $WORKER_LOG_DIR/* $EXE_LOG_DIR
		      fi"
	echo "$host $?"
done < "$SLAVES_PATH"
