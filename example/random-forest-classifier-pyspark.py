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
Random Forest Classifier Example.
"""
# $example on$
import sys

sys.path.append('..')
import traceback

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
from pyspark.sql import SparkSession

defaultUri = "daos://pool0/cont1/RandomForest/input/libsvm"
defaultTreesCount = 10

helpMsg = "\nParameter's order is,\n" + \
          f"1, datset URI (default: {defaultUri} )\n" + \
          f"2, trees count (default: {defaultTreesCount} )\n"

print("Running 'RandomForest Classifier Example - HiBench Dataset:\n")
params = ' '.join([e for e in sys.argv])
print(sys.argv)
print("\n")

if __name__ == "__main__":
    uriStr = defaultUri
    treescount = defaultTreesCount
    try:
        argLen = len(sys.argv)
        if argLen > 1:
            uriStr = sys.argv[1]
        if argLen > 2:
            treescount = sys.argv[2]

        spark = SparkSession.builder.appName(
            "RandomForest Classifier Example - HiBench Dataset, " + params).getOrCreate()
        # $example on$
        # Load and parse the data file, converting it to a DataFrame.
        data = spark.read.parquet(uriStr).toDF("label", "features")
        data.printSchema()
        data.show()

        # Index labels, adding metadata to the label column.
        # Fit on whole dataset to include all labels in index.
        labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

        # Automatically identify categorical features, and index them.
        # Set maxCategories so features with > 4 distinct values are treated as continuous.
        featureIndexer = \
            VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

        # Split the data into training and test sets (30% held out for testing)
        (trainingData, testData) = data.randomSplit([0.7, 0.3])

        # Train a RandomForest model.
        rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=treescount)

        # Convert indexed labels back to original labels.
        labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                       labels=labelIndexer.labels)

        # Chain indexers and forest in a Pipeline
        pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

        # Train model.  This also runs the indexers.
        model = pipeline.fit(trainingData)

        # Make predictions.
        # predictions = model.transform(testData)

        # # Select example rows to display.
        # predictions.select("predictedLabel", "label", "features").show(5)

        # # Select (prediction, true label) and compute test error
        # evaluator = MulticlassClassificationEvaluator(
        #     labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
        # accuracy = evaluator.evaluate(predictions)
        # print("Test Error = %g" % (1.0 - accuracy))

        rfModel = model.stages[2]
        print(rfModel.toDebugString)  # summary only
    # $example off$
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        raise Exception(helpMsg) from e
    finally:
        spark.stop()
