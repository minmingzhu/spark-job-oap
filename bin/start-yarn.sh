cur_dir=$(dirname "${BASH_SOURCE[0]}")
source $cur_dir/env_spark_daos.sh

env_cust=$HADOOP_CONF_DIR/env-cust.sh
echo "export JAVA_HOME=$JAVA_HOME" > $env_cust
echo "export DAOS_LIBRARY_PATH=$DAOS_LIBRARY_PATH" >> $env_cust

$HADOOP_HOME/sbin/start-yarn.sh
