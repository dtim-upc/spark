from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datasketch import MinHashLSHEnsemble, MinHash
from statistics import mean
from pyspark.sql import Row
import time, sys, argparse

class LSH_Benchmark:

    def __init__(self, thresholds):
        self.timeIndexingT = 0
        self.timeReadingT = 0
        self.timesQueryingT = {}
        self.thresholds = thresholds

    def readDataset(self,file, sep, multi, nullVal,ignoreTrailing):
        return spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", sep) \
            .option("multiline", multi) \
            .option("quote","\"") \
            .option("escape", "\"") \
            .option("nullValue", nullVal) \
            .option("ignoreTrailingWhiteSpace", ignoreTrailing) \
            .csv(file)


    def getDatasets(self, pathInfoDatasets, pathDatasets, benchmarkCategory):
        candidatesDatasets = {}
        startReading = time.time()
        dsInfo = self.readDataset(pathInfoDatasets, ",", "false","","true")

        print("Benchmark testbed {} has {} datasets".format(benchmarkCategory,dsInfo.count()))

        for row in dsInfo.select("filename", "delimiter", "multiline", "nullVal","file_size","ignoreTrailing").distinct().collect():
            print("reading: {}".format(row['filename']))
            candidatesDatasets[row['filename']] = self.readDataset(pathDatasets +"/"+ row['filename'], row['delimiter'],
                                                                   row['multiline'],row['nullVal'],row['ignoreTrailing'])
        candidatesAtt = {}
        for filename, df in candidatesDatasets.items():
            print("filename: {}".format(filename))
            attributes = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
            for a in attributes:

                attributeTmp = set([row[a] for row in df.select(F.lower(F.col(a)).alias(a)).na.drop().distinct().collect()])
                if len(attributeTmp) > 0 :
                    candidatesAtt[filename + "#$#" + a] = attributeTmp
                else:
                    print("attribute {} empty from {} ".format(a,filename))

        sortedAttNames = sorted(candidatesAtt, key=lambda k: len(candidatesAtt[k]), reverse=False)
        endReading = time.time()
        totalTimeReading = abs(endReading - startReading)
        self.timeReadingT = totalTimeReading/60
        print("--- TIME READING: %s minutes ---" % totalTimeReading)
        return candidatesAtt, sortedAttNames

    def writeTimes(self, testbed, pathOut):
        pathW = pathOut + "/time_testbed{}.txt".format(testbed)
        print("Writting times in file: {}".format(pathW))
        with open(pathW, "w") as text_file:


            print("-----------------------------------------------------------", file=text_file)
            print("Total time pre runtime: {} minutes".format(self.timeReadingT + self.timeIndexingT), file=text_file)
            print("Total time runtime: {} minutes".format(sum(self.timesQueryingT.values())), file=text_file)
            print("-----------------------------------------------------------\n", file=text_file)
            print("Other times", file=text_file)
            print("-----------------------------------------------------------\n", file=text_file)
            print("Time reading files: {} minutes".format(self.timeReadingT), file=text_file)
            print("Time indexing: {} minutes".format(self.timeIndexingT), file=text_file)
            print("Average time runtime: {} minutes".format(mean(self.timesQueryingT.values())), file=text_file)

            print("Number of datasets querying: {}".format(len(self.timesQueryingT.values())), file=text_file)
            print("\n\ntime querying: {}".format(self.timesQueryingT), file=text_file)



    def hash(self, candidatesAtt, sortedAttNames):

        print("Creating MinHash...")
        startHashing = time.time()
        minhashes = dict()
        for key, value in candidatesAtt.items():
            # Create MinHash objects
            m = MinHash()
            for v in value:
                m.update(v.encode('utf8'))
            minhashes[key] = (key, m, len(value))

        lshEnsembles = dict()
        for threshold in self.thresholds:
            print("Building LSH Ensemble index for {}".format(threshold))
            lsh = MinHashLSHEnsemble(threshold=threshold)
            lsh.index([minhashes[name] for name in sortedAttNames])
            lshEnsembles[threshold] = lsh


        endHashing = time.time()
        totalTimeHashing = abs(endHashing - startHashing)
        self.timeIndexingT = totalTimeHashing/60
        print("--- TIME HASHING: %s minutes ---" % totalTimeHashing)

        return minhashes, lshEnsembles


    def queryLSH(self, minhashes, lshIndexes, testbed, pathOut):

        resultRow = []
        startQuery = time.time()
        for threshold in self.thresholds:
            print("Querying using threshold {}".format(threshold))
            for key, value in minhashes.items():
                qAttname, minHashQ, lenQ = value

                startQ = time.time()
                result = list(lshIndexes[threshold].query(minHashQ, lenQ))
                endQ = time.time()
                timeQ = startQ - endQ

                query = key.split("#$#")
                self.timesQueryingT[query[0]] = abs(timeQ)/60

                for element in result:
                    candidate = element.split("#$#")

                    if query[0] != candidate[0]:
                        rowTmp = Row(queryDataset=query[0], queryAttribute=query[1], candidateDataset=candidate[0],
                                     candidateAttribute=candidate[1], threshold=threshold)

                        resultRow.append(rowTmp)

        endQuery = time.time()
        timeQuery = endQuery - startQuery
        print("--- TIME QUERYING: %s minutes ---" % timeQuery)
        dataframeResults = spark.createDataFrame(resultRow) \
            .withColumnRenamed("queryDataset","query dataset") \
            .withColumnRenamed("queryAttribute","query attribute") \
            .withColumnRenamed("candidateDataset","candidate dataset") \
            .withColumnRenamed("candidateAttribute","candidate attribute")

        pathW = pathOut + "/LSHEnsembleResults_testbed{}".format(testbed)
        print("Writting discovery in file: {}".format(pathW))

        # keep the highest threshold for an attribute pair
        dataframeResults.groupBy("query dataset", "query attribute", "candidate dataset", "candidate attribute").agg(F.max("threshold").alias("threshold")) \
            .repartition(1).write.mode("overwrite").option("header", "true").csv(pathW)


    def startBenchmark(self, pathInfo, pathDatasets, testbed, pathOut):
        candidatesAtt, sortedAttNames = self.getDatasets(pathInfo, pathDatasets, testbed)
        minhashes, lshIndexes = self.hash(candidatesAtt,  sortedAttNames)
        self.queryLSH(minhashes, lshIndexes, testbed, pathOut)
        self.writeTimes(testbed, pathOut)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run LSH Ensemble comparison using testbeds obtained "
                    "from https://github.com/dtim-upc/spark/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/")

    parser.add_argument("--datasetInfo", type=str, required=True,
                        help="Path to the CSV file with the datasets names and the configuration to read them")

    parser.add_argument("--datasetsDir", type=str, required=True,
                        help="Path to the Datasets folder")

    parser.add_argument("--testbed", type=str, required=False, default="",
                        help="testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated")

    parser.add_argument("--output", type=str, required=True,
                        help="Path to write results")

    args = parser.parse_args(sys.argv[1:])

    conf = SparkConf().set('spark.driver.maxResultSize', '8G').set('spark.driver.memory', '8G')
    spark = SparkSession.builder.config(conf=conf).master("local").getOrCreate()

    thresholds = [0.1,0.25,0.5,0.75]
    testbed = 1
    b = LSH_Benchmark(thresholds)
    b.startBenchmark(args.datasetInfo, args.datasetsDir, args.testbed, args.output)







