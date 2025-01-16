#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
An example for computing correlation matrix.
Run with:
  bin/spark-submit examples/src/main/python/ml/correlation_example.py
"""
import sys

sys.path.append('..')
from utils.utils import Timer
import traceback

# $example on$
from pyspark.ml.stat import Correlation
# $example off$
from pyspark.sql import SparkSession

defaultUri = "daos://pool0/cont1/Correlation/input/libsvm"

helpMsg = "\nParameter's order is,\n" + \
          f"1, datset URI (default: {defaultUri} )\n" + \
          f"2, executorNum \n"

print("Running 'Correlation Example - HiBench Dataset:\n")
params = ' '.join([e for e in sys.argv])
print(sys.argv)
print("\n")

if __name__ == "__main__":

    # INIT
    uriStr = defaultUri
    try:
        argLen = len(sys.argv)
        if argLen > 1:
            uriStr = sys.argv[1]
        if argLen > 2:
            executorNum = sys.argv[2]

        metrics_name = "Correlation_" + str(executorNum)
        correlation_timer = Timer(metrics_name)
        correlation_timer.record("Start")
        spark = SparkSession.builder.appName("Correlation Example - HiBench Dataset, " + params).getOrCreate()
        # INIT end
        correlation_timer.record("Init")
        # Preprocessing start
        # $example on$
        df = spark.read.parquet(uriStr).toDF("features")
        r1 = Correlation.corr(df, "features").head()
        print("Pearson correlation matrix:\n" + str(r1[0]))

    # $example off$
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        raise Exception(helpMsg) from e
    finally:
       spark.stop()
       # Postprocessing
       correlation_timer.record("Postprocessing")
       correlation_timer.printTimeTable()
