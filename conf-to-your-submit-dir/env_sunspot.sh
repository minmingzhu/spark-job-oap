[[ -z ${SPARKJOB_DAOS+X} ]] && declare -ir SPARKJOB_DAOS=1
if [ "$SPARKJOB_DAOS" -gt 0 ];then
        echo "loading DAOS module"
        #module use /soft/storage/daos/modulefiles
        module load daos
        #export DAOS_AGENT_CONF=/soft/storage/daos/yaml-file/daos01_agent.yml

        #export LD_LIBRARY_PATH=/soft/storage/daos/sles1/daos-2tb5/install/lib64:/soft/storage/daos/sles1/daos-2tb5/install/prereq/release/protobufc/lib:/soft/storage/daos/sles1/daos-2tb5/install/prereq/release/ofi/lib:/soft/storage/daos/spark/lib-deps:$LD_LIBRARY_PATH
else
        echo "no DAOS module"
fi
# set Spark Worker resources
export SPARK_WORKER_CORES=96
export SPARK_WORKER_MEMORY=900G

# set GPU options
export GPU_RESOURCE_FILE=$SPARKJOB_CONFIG_DIR/gpuResourceFile_sunspot.json
export GPU_WORKER_AMOUNT=12

