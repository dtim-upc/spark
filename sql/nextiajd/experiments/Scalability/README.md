# Scalability

Since we target large-scale scenarios, we evaluate the scalability of our NextiaJD in such settings. We experiment with different
file sizes both in terms of rows and columns. For this goal, we create 4 different classes that allows to recreate this experiment and creates the files requirements for this experiment based on a base file.


## Prerequisites

* NextiaJD. To see how to install NextiaJD [check this page](https://github.com/dtim-upc/NextiaJD#installation). 
* The spark-submit script. You can find this scrip in your Spark installation under bin folder e.g $SPARK_HOME/bin
* Download the [JAR](https://mydisk.cs.upc.edu/s/itRkyqC5yPzd7r3/download) and the source code can be found [here](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/Scalability/src/main/scala). This JAR contains the following classes:
    * **GeneratorM** it replicates M columns from a base file.
    * **GeneratorN** it replicates N rows from a base file.
    * **ToParquet** it converts a csv file to a parquet file.
    * **Profiling** it will measure the profiling time from a list of datasets

## GeneratorM

This object generates a new file with a suffix Gen{M} where M is the number of columns replicated. The following parameters are required:


| Parameter             | Required | Description                                              |
|-----------------------|----------|----------------------------------------------------------|
| -p, --path-dataset    | True     | Path to the dataset                                      |
| -o, --output          | True     | Path to write the results                                |
| -m, --m               | True     | Number of M columns to replicate                         |
| -n, --nullval         | False    | Dataset null value. Default value is ""                  |
| --multiline           | False    | Indicate if dataset is multiline. Default value is false |
| -i, --ignore-trailing | False    | Ignores dataset trailing. Default value is true          |
| -d, --delimiter       | False    | Dataset delimiter. Default value is ","                  |
| -h, --help            | False    | Show help message                                        |


## GeneratorN

This object generates a new file with a suffix Gen{N} where N is the number of rows replicated. The following parameters are required:  

| Parameter             | Required | Description                                              |
|-----------------------|----------|----------------------------------------------------------|
| -p, --path-dataset    | True     | Path to the dataset                                      |
| -o, --output          | True     | Path to write the results                                |
| -n, --n               | True     | Number of N rows to replicate                            |
| --nullval             | False    | Dataset null value. Default value is ""                  |
| -m, --multiline       | False    | Indicate if dataset is multiline. Default value is false |
| -i, --ignore-trailing | False    | Ignores dataset trailing. Default value is true          |
| -d, --delimiter       | False    | Dataset delimiter. Default value is ","                  |
| -h, --help            | False    | Show help message                                        |

## ToParquet

This object converts csv files to parquet files. It will generate a folder for each dataset read and a file parquetFiles.csv with all the parquet files generated. The following parameters are required:

| Parameter           | Required | Description                                                                                            |
|---------------------|----------|--------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | True     | path to datasets info file. This file contains the datasets names and their configuration to read them |
| -o, --output        | True     | Path to write the results                                                                              |
| -p, --path-datasets | True     | path to the folder where all datasets are                                                              |
| -h, --help          | False    | Show help message  

## Profiling

This object will compute the attribute profile for each dataset to measure the execution time. The following parameters are required.

| Parameter           | Required | Description                                                                                            |
|---------------------|----------|--------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | True     | path to datasets info file. This file contains the datasets names and their configuration to read them |
| -o, --output        | True     | Path to write the results                                                                              |
| -p, --path-datasets | True     | path to the folder where all datasets are                                                              |
| -t, --type-profile  |          | The type of files for profiling. It can be csv or parquet. Default is csv                              |
| -h, --help          | False    | Show help message                                                                                      |
