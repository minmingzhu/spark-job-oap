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

import sys

sys.path.append('..')
import traceback

from pyspark.ml.feature import PCA
from pyspark.sql import SparkSession


defaultUri = "/home/damon/opt/storage/HiBench/PCA/Input/10"
defaultK = 10

helpMsg = "\nParameter's order is,\n" + \
          f"1, datset URI (default: {defaultUri} )\n" + \
          f"2, k (default: {defaultK} )\n" + \
          f"3, executorNum \n"

print("Running 'PCA Example - HiBench Dataset:\n")
params = ' '.join([e for e in sys.argv])
print(sys.argv)
print("\n")

if __name__ == "__main__":

    uriStr = defaultUri

    try:
        argLen = len(sys.argv)
        if argLen > 1:
            uriStr = sys.argv[1]
        if argLen > 2:
            k = int(sys.argv[2])

        # INIT
        k = defaultK
        spark = SparkSession.builder.appName("PCA Example - HiBench Dataset, " + params).getOrCreate()
        df = spark.read.parquet(uriStr).toDF("features")

        pca = PCA(k=k, inputCol="features", outputCol="pcaFeatures")
        model = pca.fit(df)

        print("Principal Components: ", model.pc, sep='\n')
        print("Explained Variance: ", model.explainedVariance, sep='\n')
    # $example off$
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        raise Exception(helpMsg) from e
    finally:
       spark.stop()
