echo "loading spark module"
module use /home/damon/spark_module/modulefiles/
module load spark/3.2.0

[[ -z ${SPARKJOB_OUTPUT_DIR+X} ]] && declare SPARKJOB_OUTPUT_DIR="$(pwd)"
[[ -z ${SPARKJOB_CONFIG_DIR+X} ]] && declare SPARKJOB_CONFIG_DIR="$(pwd)"

# LOAD DAOS and STARTUP DAOS AGENT
SCRIPT_PATH=$(dirname "$BASH_SOURCE")

[[ -z ${SPARKJOB_SCRIPTS_DIR+X} ]] \
        && declare SPARKJOB_SCRIPTS_DIR="$(cd ${SCRIPT_PATH}&&pwd)"

source $SPARKJOB_SCRIPTS_DIR/setup-common.sh

# MORE SPARK DIR
export SPARK_WORKER_DIR="${SPARKJOB_OUTPUT_DIR}/spark_worker_log"
# export SPARK_WORKER_DIR="/tmp/workers-${USER}"
[[ -z ${SPARKJOB_WORKING_DIR+X} ]] && declare SPARKJOB_WORKING_DIR="$(pwd)"
export SPARK_CONF_DIR="$SPARKJOB_WORKING_DIR/conf"
export SPARK_LOG_DIR="$SPARKJOB_WORKING_DIR/logs"

source $SPARKJOB_CONFIG_DIR/env_$SPARKJOB_HOST.sh
echo "sourced $SPARKJOB_CONFIG_DIR/env_$SPARKJOB_HOST.sh"

