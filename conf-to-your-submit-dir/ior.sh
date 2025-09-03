#!/usr/bin/bash
#
export JAVA_HOME=/lus/flare/projects/Aurora_deployment/jiafuzha/java
export LD_PRELOAD=$JAVA_HOME/lib/libjsig.so
export PATH=/home/jiafuzha/apps/python2/bin:/home/jiafuzha/apps/daos/bin:$PATH

module use /soft/modulefiles
module load daos/base

ior -a DFS --dfs.pool Intel --dfs.cont hadoop_fs -w -r -W -R -t 1m -b 200G -D 150 -k -o /testFile1
