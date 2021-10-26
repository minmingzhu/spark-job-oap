module use /home/daos/sles/modulefiles
module load daos/1.3

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/kalfizah/daos-210628/install/prereq/release/protobufc/lib:/soft/storage/daos/spark/lib-deps
export CRT_CREDIT_EP_CTX=0

# set Spark Worker resources
export SPARK_WORKER_CORES=36
export SPARK_WORKER_MEMORY=160G

# set GPU options
export GPU_OPTIONS=
