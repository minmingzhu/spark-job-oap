if [[ "$#" == 1 ]]; then
	if [ -z "$SPARK_CONF_DIR" ]; then
		echo "SPARK_CONF_DIR env is not set. Please use this script after 'submit-spark.sh'"
		exit 1
	fi
	NODE_FILE="$SPARK_CONF_DIR/nodes"
	COMMAND_TEMP=$1
elif [[ "$#" == 2 ]]; then
	NODE_FILE=$1
        COMMAND_TEMP=$2
else
	echo "wrong arguments"
	echo -e "usage:\nloop.sh <COMMAND TEMPLATE> or loop.sh <nodes file> <COMMAND TEMPLATE>"
	echo -e "<COMMAND TEMPLATE> should contain '??' or '?' as placeholder for all concatenated nodes or single node. If it's '??', the command will run once for all nodes. Otherwise, it will run once for each node."
	echo -e "examples:\nloop.sh \"pdsh -w ?? mkdir /tmp/jars \""
	echo -e "loop.sh \"scp xx.jar ?:/tmp/jars/ \""
	exit 1
fi

function join_by { local IFS="$1"; shift; echo "$*"; }

all_nodes=()

while IFS= read -r line
do
	all_nodes+=("$line")
done < "$SPARK_CONF_DIR/nodes"

all_nodes_str=$(join_by , ${all_nodes[@]})

echo $all_nodes_str

if [[ $COMMAND_TEMP =~ "??" ]]; then
       cmd=${COMMAND_TEMP//\?\?/$all_nodes_str}
       echo $cmd
       $cmd
else
	for n in "${all_nodes[@]}"
	do
        	cmd=${COMMAND_TEMP//\?/$n}
		echo $cmd
		$cmd
	done

fi
