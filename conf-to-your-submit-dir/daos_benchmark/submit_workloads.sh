#!/usr/bin/env bash
# make sure script run under home folder so that some info shared between login node and compute node
SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")
SCRIPT_PATH=$(realpath "$SCRIPT_PATH")
if [[ "$SCRIPT_PATH" != "$HOME"* ]]; then
	echo "Error: script needs to run under home directory or home descendant dirs. Current dir is $SCRIPT_PATH. exiting"
	exit 1
fi

# get apps root dir
[[ -z ${APPS_DIR+X} ]] && declare APPS_DIR=/lus/flare/projects/Aurora_deployment/spark
apps_dir=$APPS_DIR

if [[ "$#" != 2 ]]; then
	echo "Error: need parameters of workload and scale. exiting"
	exit 1
fi
workload=$1
declare -i scale=$2

# set python path
export PATH=$apps_dir/python2/bin:$PATH
python_path=$(which python)
echo "using python $python_path"

# check user groups
grps=$(groups)
if [[ "$grps" == *"Aurora_deployment"* ]]; then
	echo "User is in Aurora_deployment group"
else
	echo "Error: Need Aurora_deployment group, exiting"
	exit 1
fi

WORKLOAD_DFSIOE="dfsioe"
WORKLOAD_REPARTITION="repartition"

declare -A resource_worker_cores
resource_worker_cores["$WORKLOAD_DFSIOE"]=32
resource_worker_cores["$WORKLOAD_REPARTITION"]=96

declare -A resource_exe_mem
resource_exe_mem["$WORKLOAD_DFSIOE"]="83g"
resource_exe_mem["$WORKLOAD_REPARTITION"]="41g"

if [[ "$workload" == "$WORKLOAD_DFSIOE" || "$workload" == "$WORKLOAD_REPARTITION" ]]; then
        echo "running $workload. set worker cores to ${resource_worker_cores[$workload]} and executor memory to ${resource_exe_mem[$workload]}"
else
        echo "Error: unknown workload $workload. should be either $WORKLOAD_DFSIOE or $WORKLOAD_REPARTITION. exiting"
        exit 1
fi

# 1.==========update cores and executor memory
if [[ ! -e "env_aurora.sh.template" ]]; then
        echo "Error: missing env_aurora.sh.template. Please copy it from spark-job tool conf folder. exiting"
        exit 1
fi
if [[ ! -e "env_local.sh.template" ]]; then
        echo "Error: missing env_local.sh.template. Please copy it from spark-job tool conf folder. exiting"
        exit 1
fi
sed "s/<WORKER_CORES>/${resource_worker_cores[$workload]}/" env_aurora.sh.template > env_aurora.sh
cfg=$(grep "export[[:space:]]\+SPARK_WORKER_CORES=${resource_worker_cores[$workload]}" env_aurora.sh | sed 's/^[[:space:]]*//' | grep -v "^#")
if [[ -z "$cfg" ]]; then
       echo "Error: failed to set worker cores to env_aurora.sh. exiting"
       exit 1
fi
echo "Updated spark worker cores in env_aurora.sh"
sed "s/<EXECUTOR_MEMORY>/${resource_exe_mem[$workload]}/" env_local.sh.template > env_local.sh
cfg=$(grep "spark.executor.memory[[:space:]]\+${resource_exe_mem[$workload]}" env_local.sh | sed 's/^[[:space:]]*//' | grep -v "^#")
if [[ -z "$cfg" ]]; then
       echo "Error: failed to set executor memory in env_local.sh. exiting"
       exit 1
fi
echo "Updated spark executor memory in env_local.sh"


# 2.==========check and update pool, as well as copy files with updated pool to target directory
[[ -z ${DAOS_POOL+X} ]] && declare DAOS_POOL=Intel
pool=${DAOS_POOL}
echo "Using DAOS pool $pool"
# ====core-site.xml "<value>daos://Intel/hadoop_fs</value>"
if [[ ! -e "core-site.xml" ]]; then
	echo "Error: missing core-site.xml. Please copy it from spark-job tool conf folder. exiting"
	exit 1
fi
# already same pool?
fs_value=$(grep "<value>[[:space:]]*daos://$pool/hadoop_fs[[:space:]]*</value>" core-site.xml | sed 's/^[[:space:]]*//' | grep -v "^<\!--")
if [[ -z "$fs_value" ]]; then
	# has xml valid other value?
	fs_value=$(grep "<value>[[:space:]]*daos://.*/hadoop_fs[[:space:]]*</value>" core-site.xml | sed 's/^[[:space:]]*//' | grep -v "^<\!--")
	if [[ -z "$fs_value" ]]; then
		echo "Error: core-site.xml malformat in terms of valid daos URI. exiting"
		exit 1
	fi
	# update pool and check if update successful
	sed -i "s/<value>[[:space:]]*daos:\/\/.*\/hadoop_fs[[:space:]]*<\/value>/<value>daos:\/\/$pool\/hadoop_fs<\/value>/" core-site.xml
	fs_value=$(grep "<value>daos://$pool/hadoop_fs</value>" core-site.xml | sed 's/^[[:space:]]*//' | grep -v "^<\!--")
	if [[ -z "$fs_value" ]]; then
		echo "Error: failed to update pool in core-site.xml. exiting"
		exit 1
	fi
	echo "Updated pool $pool in core-site.xml"
fi
# ====env_local.sh "spark.shuffle.daos.pool.uuid            Intel"
sed -i "s/spark.shuffle.daos.pool.uuid[[:space:]]\+.*$/spark.shuffle.daos.pool.uuid  $pool/" env_local.sh
cfg=$(grep "spark.shuffle.daos.pool.uuid  $pool" env_local.sh | sed 's/^[[:space:]]*//' | grep -v "^#")
if [[ -z "$cfg" ]]; then
       echo "Error: failed to set daos pool in env_local.sh. exiting"
       exit 1
fi
echo "Updated pool $pool in env_local.sh"
# ====/lus/flare/projects/Aurora_deployment/jiafuzha/HiBench/conf/hadoop.conf "hibench.hdfs.master       daos://Intel/hadoop_fs/"
hibench_hadoop_conf="$apps_dir/HiBench/conf/hadoop.conf"
if [[ ! -e "$hibench_hadoop_conf" ]]; then
       echo "Error: $hibench_hadoop_conf not found"
       exit 1
fi
sed -i "s/hibench.hdfs.master[[:space:]]\+daos:\/\/.*\/hadoop_fs/hibench.hdfs.master  daos:\/\/$pool\/hadoop_fs/" "$hibench_hadoop_conf"
cfg=$(grep "hibench.hdfs.master  daos://$pool/hadoop_fs" "$hibench_hadoop_conf" | sed 's/^[[:space:]]*//' | grep -v "^#")
if [[ -z "$cfg" ]]; then
       echo "Error: failed to set daos pool in $hibench_hadoop_conf. exiting"
       exit 1
fi
echo "Updated pool $pool in $hibench_hadoop_conf"


# 3.==========re-create containers each time?
[[ -z ${CLEAN_CONTAINER+X} ]] && declare -i CLEAN_CONTAINER=1
clean_container=${CLEAN_CONTAINER}

if ((clean_container>0)); then
	echo "Warning: DAOS containers, hadoop_fs and spark_shuffle, will be re-created for restoring space after compute nodes being allocated."
	echo "Warning: Cancel the job submission via 'qdel' if it's undesired operation."
	echo "Warning: Then, re-run the script with 'CLEAN_CONTAINER=0'."
fi


# 4.=========update scale related config
declare -A scale_dfsioe
scale_dfsioe["2"]=384
scale_dfsioe["4"]=768
scale_dfsioe["8"]=1536
scale_dfsioe["16"]=3072
scale_dfsioe["32"]=6144
scale_dfsioe["64"]=12288
scale_dfsioe["128"]=24576
scale_dfsioe["256"]=49152

declare -A scale_repartition_map_paralellism
declare -A scale_repartition_reduce_paralellism
scale_repartition_map_paralellism["2"]=570
scale_repartition_map_paralellism["4"]=1140
scale_repartition_map_paralellism["8"]=2280
scale_repartition_map_paralellism["16"]=4560
scale_repartition_map_paralellism["32"]=9120
scale_repartition_map_paralellism["64"]=18240
scale_repartition_map_paralellism["128"]=36480
scale_repartition_map_paralellism["256"]=72960
scale_repartition_reduce_paralellism["2"]=72960
scale_repartition_reduce_paralellism["4"]=72960
scale_repartition_reduce_paralellism["8"]=72960
scale_repartition_reduce_paralellism["16"]=72960
scale_repartition_reduce_paralellism["32"]=72960
scale_repartition_reduce_paralellism["64"]=72960
scale_repartition_reduce_paralellism["128"]=72960
scale_repartition_reduce_paralellism["256"]=145920


update_pool() {
	p=$1
	f=$2
	if [[ ! -e "$f" ]]; then
       		echo "Error: $f not found. exiting"
       		exit 1
	fi
	sed -i "s/daos:\/\/.*\/hadoop_fs/daos:\/\/$p\/hadoop_fs/g" "$f"
	cfg=$(grep "daos://$p/hadoop_fs" "$f" | sed 's/^[[:space:]]*//' | grep -v "^#")
	if [[ -z "$cfg" ]]; then
       		echo "Error: failed to set daos pool in $f. exiting"
      		exit 1
	fi
	echo "Updated pool $p in $f"
}

if [[ "$workload" == "$WORKLOAD_DFSIOE" ]]; then
	dfsioe_conf_temp="$SCRIPT_PATH/workloads/dfsioe.conf.template"
	if [[ ! -e "$dfsioe_conf_temp" ]]; then
		echo "Error: missing $dfsioe_conf_temp. please copy it from spark-job conf folder. exiting"
		exit 1
	fi
	tmp_file="$SCRIPT_PATH/workloads/.dfsioe.conf.tmp"
	sed "s/<NUM_OF_FILES>/${scale_dfsioe[$scale]}/g" "$dfsioe_conf_temp" > "$tmp_file"
        cfg1=$(grep "hibench.dfsioe.large.read.number_of_files[[:space:]]\+${scale_dfsioe[$scale]}"  "$tmp_file" | sed 's/^[[:space:]]*//' | grep -v "^#")
	cfg2=$(grep "hibench.dfsioe.large.write.number_of_files[[:space:]]\+${scale_dfsioe[$scale]}"  "$tmp_file" | sed 's/^[[:space:]]*//' | grep -v "^#")
	if [[ -z "$cfg1" || -z "$cfg2" ]]; then
		echo "Error: failed to update <NUM_OF_FILES> in $tmp_file. exiting"
		exit 1
	fi
	target_file="$apps_dir/HiBench/conf/workloads/micro/dfsioe.conf"
	cp "$tmp_file" "$target_file"
        echo "Updated <NUM_OF_FILES> to ${scale_dfsioe[$scale]} in $target_file"
	# update run_read.sh and run_write.sh
	update_pool $pool "$apps_dir/HiBench/bin/workloads/micro/dfsioe/spark/run_write.sh"
	update_pool $pool "$apps_dir/HiBench/bin/workloads/micro/dfsioe/spark/run_read.sh"
else
	hibench_conf_temp="$SCRIPT_PATH/workloads/hibench.conf.template"
	if [[ ! -e "$hibench_conf_temp" ]]; then
                echo "Error: missing $hibench_conf_temp. please copy it from spark-job conf folder. exiting"
                exit 1
        fi
        tmp_file="$SCRIPT_PATH/workloads/.hibench.conf.tmp"
        sed "s/<MAP_PARALLELISM>/${scale_repartition_map_paralellism[$scale]}/" "$hibench_conf_temp" > "$tmp_file"
	sed -i "s/<REDUCE_PARALLELISM>/${scale_repartition_reduce_paralellism[$scale]}/" "$tmp_file"
	cfg1=$(grep "hibench.default.map.parallelism[[:space:]]\+${scale_repartition_map_paralellism[$scale]}"  "$tmp_file" | sed 's/^[[:space:]]*//' | grep -v "^#")
        cfg2=$(grep "hibench.default.shuffle.parallelism[[:space:]]\+${scale_repartition_reduce_paralellism[$scale]}"  "$tmp_file" | sed 's/^[[:space:]]*//' | grep -v "^#")
        if [[ -z "$cfg1" || -z "$cfg2" ]]; then
                echo "Error: failed to update <MAP_PARALLELISM> or <REDUCE_PARALLELISM> in $tmp_file. exiting"
                exit 1
        fi
        target_file="$apps_dir/HiBench/conf/hibench.conf"
        cp "$tmp_file" "$target_file"
        echo "Updated <MAP_PARALLELISM> to ${scale_repartition_map_paralellism[$scale]} and <REDUCE_PARALLELISM> to ${scale_repartition_reduce_paralellism[$scale]} in $target_file"
fi


# 5.=========submit job
nodes=$((scale+1))
submit_script="$apps_dir/spark-job/bin/submit-spark.sh"
if [[ ! -e "$submit_script" ]]; then
	echo "Error: submit script, $submit_script not found. exiting"
	exit 1
fi

workload_script="run_workloads.sh"
if [[ ! -e "$workload_script" ]]; then
	echo "Error: missing $workload_script. please copy it from spark-job conf folder. exiting"
        exit 1
fi

"$submit_script" -y -A Intel-Punchlist -l walltime=30:00 -l select=$nodes -l daos=daos_user  -l filesystems=flare:daos_user_fs -q nre-priority "$workload_script" "$apps_dir" $pool $workload $clean_container


# 6.=========show job time by searching logs
