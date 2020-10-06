from pandas import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from statistics import mean
from pyspark.sql.types import StringType
from sklearn.exceptions import ConvergenceWarning
import argparse, sys, flexmatcher, warnings, time

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=ConvergenceWarning)

class FlexMatcher_benchmark:

    def __init__(self):
        self.timeReadingT = {}
        self.timeTraining = {}
        self.timePredicting = {}
        self.datasets = {}
        self.schemas = {}
        self.attributesNumber = 0

    def readDataset(self, file, sep, multi, nullVal, trailing):
        return spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", sep) \
            .option("multiline", multi) \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("nullValue", nullVal) \
            .option("maxCharsPerColumn", 1000001) \
            .option("ignoreTrailingWhiteSpace", trailing) \
            .csv(file)

    def getDatasets(self, pathInfo, pathDatasets, tesbed):


        dsInfo = self.readDataset(pathInfo, ",", "false", "", "true")

        print("Testbed {} has {} datasets".format(tesbed, dsInfo.count()))

        for row in dsInfo.select("filename", "delimiter", "multiline", "nullVal", "file_size",
                                 "ignoreTrailing").distinct().collect():

            # hail-2015.csv does not work with FlexMatcher profile.
            if row['filename'] != "hail-2015.csv":
                print("reading: {}".format(row['filename']))

                startReading = time.time()
                ds = self.readDataset(pathDatasets + "/"+row['filename'], row['delimiter'],
                                                                       row['multiline'], row['nullVal'],row['ignoreTrailing'])

                endReading = time.time()
                totalTimeReading = abs(endReading - startReading)
                self.timeReadingT[row['filename']] = totalTimeReading/60

                # variable to keep string attributes names
                attStr = []
                for f in ds.schema.fields:
                    if isinstance(f.dataType, StringType):
                        attStr.append(f.name)
                    else:
                      ds = ds.drop(f.name)

                self.datasets[row['filename']] = ds.toPandas()
                self.schemas[row['filename']] = attStr



    def start(self,pathInfo,pathDatasets,testbed, pathOutput):
        self.getDatasets(pathInfo,pathDatasets,testbed)

        columnsDF = ["query dataset", "query attribute", "candidate dataset", "candidate attribute"]
        data = []
        self.attributesNumber = 0

        datasetsN = self.datasets.keys()
        for queryDataset in datasetsN:

            print("Using querydataset {}".format(queryDataset))
            mapping = {}

            self.attributesNumber += len(self.schemas[queryDataset])
            for q in self.schemas[queryDataset]:
                mapping[q] = q

            if len(self.schemas[queryDataset]) == 1:
                # If query dataset have only one column,
                # we create other column to be the negative column for the training

                mapping["negativeColumnBench"] = "negativeColumnBench"
                self.datasets[queryDataset].insert(0,"negativeColumnBench","lorem ipsum")

            # We create the model for the query dataset
            startTraining = time.time()

            fm = flexmatcher.FlexMatcher([self.datasets[queryDataset]],[mapping])
            fm.train()

            endTraining = time.time()
            timeTraining = endTraining - startTraining
            self.timeTraining[queryDataset] = (abs(timeTraining))/60

            auxTimePredicting  = 0
            for filename, candidateDataset in self.datasets.items():

                if filename != queryDataset:

                    startPredicting = time.time()
                    predicted_mapping = fm.make_prediction(candidateDataset, True)
                    endPredicting = time.time()
                    auxTimePredicting = auxTimePredicting + abs(startPredicting - endPredicting)


                    for key, pred in predicted_mapping.items():
                        # pred, score = value
                        if pred != "negativeColumnBench" and key != "negativeColumnBench":
                            data.append([queryDataset, pred, filename, key])

            self.timePredicting[queryDataset] = auxTimePredicting /60

        df = pd.DataFrame(data, columns=columnsDF)

        pathW = pathOutput + "/flextMatcherResults_testbed{}.csv".format(testbed)
        print("Writting discovery in file: {}".format(pathW))
        df.to_csv(pathW, sep=',', encoding='utf-8')
        self.writeTimes(testbed, pathOutput)

    def writeTimes(self, testbed, pathOutput):
        pathW = pathOutput + "/time_testbed{}.txt".format(testbed)
        print("Writting times in file: {}".format(pathW))
        with open(pathW, "w") as text_file:
            timeRe = sum(self.timeReadingT.values())
            timeTrain = sum(self.timeTraining.values())
            print("-----------------------------------------------------------", file=text_file)
            print("Total time pre runtime: {} minutes".format(timeRe + timeTrain), file=text_file)
            print("Total time runtime: {} minutes".format(sum(self.timePredicting.values())), file=text_file)
            print("Number of attributes querying: {}".format(self.attributesNumber), file=text_file)
            print("-----------------------------------------------------------\n", file=text_file)
            print("Other times", file=text_file)
            print("-----------------------------------------------------------\n", file=text_file)
            print("Total time reading files: {} minutes".format(timeRe), file=text_file)
            print("Average time reading files: {} minutes".format(mean(self.timeReadingT.values())), file=text_file)
            print("Average time training: {} minutes".format(mean(self.timeTraining.values())), file=text_file)
            print("Total time training: {} minutes".format(timeTrain), file=text_file)
            print("Average time runtime: {} minutes".format(mean(self.timePredicting.values())), file=text_file)
            print("\n\ntimes reading {}".format(self.timeReadingT), file=text_file)
            print("\n\ntimes training {}".format(self.timeTraining), file=text_file)
            print("\n\ntimes runtime {}".format(self.timePredicting), file=text_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Run FlexMatcher comparison using testbeds obtained "
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

    flex = FlexMatcher_benchmark()
    flex.start(args.datasetInfo, args.datasetsDir, args.testbed, args.output)