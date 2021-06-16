# Ground truth

Here, you can find the code to generate the ground truth.

## How to run

To compute the ground truth, the class [GroundTruth](https://github.com/dtim-upc/NextiaJD2/blob/nextiajd_v3.0.1/sql/nextiajd/groundTruth/src/main/scala/GroundTruth.scala) needs to be executed with the following parameters.

| Parameter           | Required | Description                                                                                                                                                                                               |
|---------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | true     | path to datasets info file. This file contains the datasets names and their configuration to read them. It must contain the following attributes: "filename","delimiter","multiline" and "ignoreTrailing" |
| -o, --output        | true     | path to generate the ground truth csv file                                                                                                                                                                |
| -p, --path-datasets | true     | path to the datasets                                                                                                                                                                                      |
| -s, --sets          | true     | path to save the distinct values sets for each attribute                                                                                                                                                  |
| -h, --help          | false    | Show help message                                                                                                                                                                                         |
