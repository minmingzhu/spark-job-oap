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
An example for summarizer.
Run with:
  bin/spark-submit examples/src/main/python/ml/summarizer_example.py
"""
import sys

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics

sys.path.append('..')
import traceback

from pyspark.sql import SparkSession

defaultUri = "daos://pool0/cont1/Summarizer/input/libsvm"

helpMsg = "\nParameter's order is,\n" + \
          f"1, datset URI (default: {defaultUri} )\n" + \
          f"2, executorNum \n"

print("Running 'Summarizer Example - HiBench Dataset:\n")
params = ' '.join([e for e in sys.argv])
print(sys.argv)
print("\n")

if __name__ == "__main__":
    uriStr = defaultUri
    try:
        argLen = len(sys.argv)
        if argLen > 1:
            uriStr = sys.argv[1]

        spark = SparkSession.builder.appName("Summarizer Example - HiBench Dataset, " + params).getOrCreate()
        # $example on$
        df = spark.read.parquet(uriStr).toDF("features")
        rdd = df.select("features").rdd.map(lambda row: Vectors.dense(row.features))

        summary = Statistics.colStats(rdd)
        print(summary.mean())  # a dense vector containing the mean value for each column
        print(summary.variance())  # column-wise variance
        print(summary.numNonzeros())  # number of nonzeros in each column
    # $example off$
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        raise Exception(helpMsg) from e
    finally:
        spark.stop()
