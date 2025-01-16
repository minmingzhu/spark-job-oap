#!/usr/bin/bash
maxwait_0=12
count_0=0
spark_master_started=
for ((count_0=0;count_0<maxwait_0;count_0+=1));do
        if [ ! -s "$1" ]; then
                echo "master log is empty, sleeping 5 seconds ..."
                sleep 5
        else
                spark_master_started=$(grep "Successfully started service 'MasterUI'" "$1")
		echo "started: $spark_master_started"
                if [ ! -z "${spark_master_started}" ]; then
			echo "breaking"
                        break;
                fi
		sleep 5
        fi
done

echo "$spark_master_started"

if [ ! -s "$1" ]; then
        echo "spark job not run since spark master log is empty after waiting for 1 min, under $SPARKJOB_WORKING_DIR/logs/"
        exit $?
fi

if [ -z "${spark_master_started}" ]; then
        echo "spark job not run since spark master failed to start after waiting for 1 min, under $SPARKJOB_WORKING_DIR/logs/"
        exit $?
fi
