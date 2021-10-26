import sys
import traceback
from time import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

columnsToDrop = ["caseid", "dAge", "dAncstry1","dAncstry2","iAvail","iCitizen","iClass","dDepart","iDisabl1",
    "iDisabl2","iEnglish","iFeb55","iFertil","dHispanic","dHour89","dHours","iImmigr","dIncome1","dIncome2","dIncome3",
    "dIncome4","dIncome5","dIncome6","dIncome7","dIncome8","dIndustry","iKorean","iLang1","iLooking","iMarital",
    "iMay75880","iMeans","iMilitary","iMobility","iMobillim","dOccup","iOthrserv","iPerscare","dPOB","dPoverty",
    "dPwgt1","iRagechld","dRearning","iRelat1","iRelat2","iRemplpar","iRiders","iRlabor","iRownchld","dRpincome",
    "iRPOB","iRrelchld","iRspouse","iRvetserv","iSchool","iSept80","iSex","iSubfam1","iSubfam2","iTmpabsnt","dTravtime",
    "iVietnam","dWeek89","iWork89","iWorklwk","iWWII","iYearsch","iYearwrk","dYrsserv"]
defaultUri = "daos://pool0/cont1/kmeans/input/csv/ukmeans.csv"
defaultFormat = "csv"
helpMsg = "\nneed below parameters:\n" +    \
    f"1. data URI(default: {defaultUri})\n" +   \
    f"2. data format (csv, json, orc, parquet or other Spark supported formats, like libsvm), (default: {defaultFormat})\n" +   \
    "3. number of clusters(default: 2)\n" + \
    "4. maximum number of iterations(default: 20)\n" +  \
    "5. initialization algorithm (random or k-means||) (default: k-means||)\n" +    \
    "6. distance measure (cosine or euclidean) (default: euclidean)\n" +    \
    "Note: The parameters are parsed in order. If later parameter is specified, all its previous " +    \
    "parameters should be specified and in order too.\n"

print("Running 'KMeans Example - USA CENSUS 1990' with args:\n")
print(sys.argv)
print("\n")

def main():
    spark = SparkSession.builder.appName("KMeans Example - USA CENSUS 1990").getOrCreate()
    try:
        argLen = len(sys.argv)
        uriStr = defaultUri
        if argLen > 1 :
            uriStr = sys.argv[1]
        format = defaultFormat
        if argLen <= 2 :
            idx = uriStr.rindex(".")
            if idx >= 0 and idx < len(uriStr) - 1 :
                format =  uriStr[idx+1:]
        else:
            format = sys.argv[2]

        if format == "csv" :
            df = spark.read.option("header", "true").csv(uriStr)
        elif format == "json" :
            df = spark.read.json(uriStr)
        elif format == "orc" :
            df = spark.read.orc(uriStr)
        elif format == "parquet" :
            df = spark.read.parquet(uriStr)
        else:
            df = spark.read.format(format).load(uriStr)

        df = df.select(*(col(c).cast("float").alias(c) for c in df.columns))

        assembler = VectorAssembler(inputCols=["iCitizen","iClass","dIncome1"], outputCol="features", handleInvalid="skip")
        df = assembler.transform(df)
        df = df.drop(*columnsToDrop)

        kmeans = KMeans()
        for i in range(3, len(sys.argv)):
            if i == 3 :
                kmeans.setK(int(sys.argv[i]))
            elif i == 4 :
                kmeans.setMaxIter(int(sys.argv[i]))
            elif i == 5 :
                kmeans.setInitMode(sys.argv[i])
            elif i == 6 :
                kmeans.setDistanceMeasure(sys.argv[i])
            else:
                raise Exception("unexpected parameter: " + sys.argv[i])

        startMill = int(time() * 1000)
        model = kmeans.fit(df)
        print("Training time (ms) = %d\n" % (int(time() * 1000) - startMill))
        print("Training cost = %f\n" % model.summary.trainingCost)
        predictions = model.transform(df)

        evaluator = ClusteringEvaluator()
        if kmeans.getDistanceMeasure() != "cosine" :
            evaluator.setDistanceMeasure("squaredEuclidean")
        else:
            evaluator.setDistanceMeasure("cosine")

        silhouette = evaluator.evaluate(predictions)
        print(f"Silhouette with {kmeans.getDistanceMeasure()} = {silhouette}\n")
        print("Cluster Centers: \n")
        for ct in model.clusterCenters():
            print(ct)
            print("\n")

    except Exception as e:
        print(str(e))
        traceback.print_exc()
        raise Exception(helpMsg) from e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
