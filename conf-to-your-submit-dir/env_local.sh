# This file must be compatible with bash(1) on the system.
# It should be alright to source this file multiple times.

# Because you can change SPARK_CONF_DIR in this file, the main
# scripts only create the directory after sourcing this file.  Since
# we need the directory to create the spark-defaults.conf file, we
# create the directory here.
[[ -d $SPARK_CONF_DIR ]] || mkdir -p "$SPARK_CONF_DIR"

SPARKJOB_FILES=

if ((SPARKJOB_OAPML>0)); then
	SPARKJOB_FILES=${SPARKJOB_ADD_JARS//:/,}	
	# GPU options. variables defined in env_<node>.sh
    	OPTIONS="--conf spark.oap.mllib.device=GPU"
        export GPU_OPTIONS=$OPTIONS
        echo "GPU Options: $GPU_OPTIONS"
fi

# create event log dir
event_log_dir=$SPARKJOB_CONFIG_DIR/event_logs
[[ -d $event_log_dir ]] || mkdir -p "$event_log_dir"

# The created spark-defaults.conf file will only affect spark
# submitted under the current directory where this file resides.
# The parameters here may require tuning depending on the machine and workload.
[[ -s $SPARK_CONF_DIR/spark-defaults.conf ]] ||
	cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<EOF
#spark.executor.cores                 5
spark.executor.cores                 8
spark.driver.memory                  128g
#spark.executor.memory                28g
spark.executor.memory		     41g
spark.executor.instances             $EXECUTOR_NUM

spark.worker.resourcesFile           $GPU_RESOURCE_FILE
spark.worker.resource.gpu.amount     $GPU_WORKER_AMOUNT
spark.executor.resource.gpu.amount   1
spark.task.resource.gpu.amount       0.001

spark.driver.extraJavaOptions        -XX:+UseG1GC -Djava.net.preferIPv4Stack=true
spark.executor.extraJavaOptions      -XX:+UseG1GC -Djava.net.preferIPv4Stack=true

spark.executor.extraClassPath	     $SPARKJOB_ADD_JARS
spark.driver.extraClassPath          $SPARKJOB_ADD_JARS

spark.scheduler.maxRegisteredResourcesWaitingTime 600s
spark.scheduler.minRegisteredResourcesRatio 1.0
spark.cores.max  $(($EXECUTOR_NUM * 8))
spark.sql.files.maxPartitionBytes 1g

# spark.shuffle.manager=org.apache.spark.shuffle.daos.DaosShuffleManager
# spark.shuffle.daos.pool.uuid		Intel3
# spark.shuffle.daos.container.uuid	spark_shuffle

spark.eventLog.enabled               true
spark.eventLog.dir                   file://$event_log_dir


spark.shuffle.daos.write.buffer.single 512k
spark.shuffle.daos.write.wait.ms  10000
spark.shuffle.daos.read.wait.ms  12000

spark.network.timeout 120s
spark.worker.timeout 120

spark.scheduler.listenerbus.eventqueue.capacity 1000000
spark.rpc.io.backLog	64000
spark.rpc.io.receiveBuffer 1048576
spark.rpc.io.sendBuffer 1048576

spark.task.maxFailures  4
spark.stage.maxConsecutiveAttempts      1

spark.shuffle.daos.read.wait.ms		600000

EOF

[[ -s $SPARK_CONF_DIR/spark-env.sh ]] ||
        cat > "$SPARK_CONF_DIR/spark-env.sh" <<EOF
export SPARKJOB_CONFIG_DIR=$SPARKJOB_CONFIG_DIR
EOF


# TODO: python 2 or 3

# On cooley, interactive spark jobs setup ipython notebook by
# defaults.  You can change it here, along with setting up your
# other python environment.
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

#export CRT_PHY_ADDR_STR="ofi+sockets"
