# Set the scripts dir if unset.

SCRIPT_PATH=$(dirname "$BASH_SOURCE")

[[ -z ${SPARKJOB_SCRIPTS_DIR+X} ]] \
        && declare SPARKJOB_SCRIPTS_DIR="$(cd ${SCRIPT_PATH}&&pwd)"

declare SPARKJOB_ADD_JARS=

if ((SPARKJOB_OAPML>0)); then
	echo "Adding oap mllib to additional class path"
	for f in $SCRIPT_PATH/../jars/*
	do
		if [ -z $SPARKJOB_ADD_JARS ]; then
			SPARKJOB_ADD_JARS=$f
		else
			SPARKJOB_ADD_JARS=$SPARKJOB_ADD_JARS:$f
		fi
	done
fi

source "$SPARKJOB_SCRIPTS_DIR/setup-common.sh"

if ((SPARKJOB_OAPML>0)); then
	source "$SPARKJOB_SCRIPTS_DIR/env_gpu.sh"
	echo "sourced $SPARKJOB_SCRIPTS_DIR/env_gpu.sh"
fi

source "$SPARKJOB_SCRIPTS_DIR/env_spark_daos.sh"
echo "sourced $SPARKJOB_SCRIPTS_DIR/env_spark_daos.sh"
[[ -s $SPARKJOB_CONFIG_DIR/env_local.sh ]] &&
	source "$SPARKJOB_CONFIG_DIR/env_local.sh"
[[ -s $SPARKJOB_CONFIG_DIR/env_local.sh ]] &&
	echo "sourced $SPARKJOB_CONFIG_DIR/env_local.sh"
