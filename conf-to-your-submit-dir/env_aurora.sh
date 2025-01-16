[[ -z ${SPARKJOB_DAOS+X} ]] && declare -ir SPARKJOB_DAOS=1
if [ "$SPARKJOB_DAOS" -gt 0 ];then
        echo "loading DAOS module"
        module use /soft/modulefiles
        module load daos/base
        #export DAOS_AGENT_CONF=/soft/storage/daos/yaml-file/daos01_agent.yml

        #export LD_LIBRARY_PATH=/soft/storage/daos/sles1/daos-2tb5/install/lib64:/soft/storage/daos/sles1/daos-2tb5/install/prereq/release/protobufc/lib:/soft/storage/daos/sles1/daos-2tb5/install/prereq/release/ofi/lib:/soft/storage/daos/spark/lib-deps:$LD_LIBRARY_PATH
else
        echo "no DAOS module"
fi
# set Spark Worker resources
export SPARK_WORKER_CORES=96
export SPARK_WORKER_MEMORY=500G

export SPARK_DAEMON_MEMORY=2G
export SPARK_DAEMON_JAVA_OPTS="-Djava.net.preferIPv4Stack=true"

# set GPU options
export GPU_RESOURCE_FILE=$SPARKJOB_CONFIG_DIR/gpuResourceFile_aurora.json
export GPU_WORKER_AMOUNT=12

# bind to hsn7
spark_ip=$(ip -f inet addr show hsn7 | sed -En -e 's/.*inet ([0-9.]+).*/\1/p')
if [[ $spark_ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	#export SPARK_LOCAL_IP=$spark_ip
	export SPARK_DRIVER_HOST=$spark_ip
else
	echo "not able to get ip from hsn7"
fi
# bind to hsn1
#spark_ip=$(ip -f inet addr show hsn1 | sed -En -e 's/.*inet ([0-9.]+).*/\1/p')
#if [[ $spark_ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
#	export SPARK_DRIVER_HOST=$spark_ip
#else
#	echo "not able to get ip from hsn1"
#fi
