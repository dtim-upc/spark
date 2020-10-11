# Scalability

Since we target large-scale scenarios, we evaluate the scalability
of our NextiaJD in such settings. We experiment with different
file sizes both in terms of rows and columns. For this goal, we create 4 different objects that allows to create the files requirements for this experiment based on a base file. Here you can find the following:

* GeneratorM. It replicates M columns from a base file.
* GeneratorN. It replicates N rows from a base file.
* ToParquet. It converts a csv file to a parquet file.
* Profiling. This object will measure the profiling time from a list of datasets
                                                                                    
All these objects are packaged in a jar file to work with spark-submit. You can find the jar [here]() and the source code [here]()

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
