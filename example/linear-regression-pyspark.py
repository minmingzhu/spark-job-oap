import sys

sys.path.append('..')
import traceback

from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

defaultUri = "daos://pool0/cont1/RandomForest/input/libsvm"
defaultTreesCount = 10

helpMsg = "\nParameter's order is,\n" + \
          f"1, datset URI (default: {defaultUri} )\n" + \
          f"2, trees count (default: {defaultTreesCount} )\n" + \
          f"3, executorNum \n"

print("Running 'Linear Regression Example - HiBench Dataset:\n")
params = ' '.join([e for e in sys.argv])
print(sys.argv)
print("\n")

if __name__ == "__main__":

    # INIT
    uriStr = defaultUri
    treescount = defaultTreesCount
    try:
        argLen = len(sys.argv)
        if argLen > 1:
            uriStr = sys.argv[1]
        if argLen > 2:
            treescount = sys.argv[2]

        spark = SparkSession.builder.appName("Linear Regression Example - HiBench Dataset, " + params).getOrCreate()
        # $example on$
        # Load and parse the data file, converting it to a DataFrame.
        data = spark.read.parquet(uriStr).toDF("label", "features")
        data.printSchema()
        data.show()

        lr = LinearRegression(solver="normal")

        # Fit the model
        lrModel = lr.fit(data)

        # Print the coefficients and intercept for linear regression
        print("Coefficients: %s" % str(lrModel.coefficients))
        print("Intercept: %s" % str(lrModel.intercept))

        # Summarize the model over the training set and print out some metrics
        trainingSummary = lrModel.summary
        print("numIterations: %d" % trainingSummary.totalIterations)
        print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
        trainingSummary.residuals.show()
        print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
        print("r2: %f" % trainingSummary.r2)

    # $example off$
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        raise Exception(helpMsg) from e
    finally:
        spark.stop()
