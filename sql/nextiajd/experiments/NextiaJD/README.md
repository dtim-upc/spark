# NextiaJD experiments


Here you can find the code for:

*  Predictive accuracy: generates the discovery and time execution for the testbed
*  Confusion matrix generator: generates the confusion matrices  and their metrics for the state-of-the-art
*  Discovery of semantic non-sytactic relationships

## Installation

The code have the structure of an SBT project. It was tested on [Intellij IDEA](https://www.jetbrains.com/es-es/idea/). It is necessary to place NextiaJD jars in the lib folder.

## Running Predictive accuracy


To run this experiment, you need to execute `NextiaJD_evaluation.scala` with the following arguments:


| Parameter           | Required | Description                                                                                                 |
|---------------------|----------|-------------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | True     | path to datasets info file. This file containsthe datasets names and their configuration to read them       |
| -g, --groud-truth   | True     | path to the ground truth csv file                                                                           |
| -o, --output        | True     | path to write the discovery and time results                                                                |
| -p, --path-datasets | True     | path to the folder containing all datasets                                                                  |
| -q, --query-type    | False    | The query search. There are two types:querybydataset and querybyattribute. Default value is querybydataset. |
| -t, --testbed       | False    | testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated. Default is ""      |
| -h, --help          | False    | Show help message                                                                                           |

The results of the code are:

*   NextiaJD_testbed.csv: file containing the discovery results and the ground truth
*   Nextia_evaluation_testbed.csv: file containing the confusion matrix and its metrics
*   time_testbed.txt: file containing the times from the execution: pre runtime and runtime

## Confusion matrix generator

This code allows you to compare the results of NextiaJD, LSH Ensemble and FlexMatcher. The following parameters are needed:


| Parameter         | Required | Description                                             |
|-------------------|----------|---------------------------------------------------------|
| -n, --nextiajd    | True     | path to the discovery result file from nextiajd         |
| --nextiajd-s      | False    | path to the discvoery for testbed S                     |
| --nextiajd-m      | False    | path to the discvoery for testbed M                     |
| -l, --lsh         | True     | path to the discovery result file from lsh              |
| --lsh-s           | False    | path to the discvoery for testbed S                     |
| --lsh-m           | False    | path to the discvoery for testbed M                     |
| -f, --flexmatcher | True     | path to the discovery result file from flexmatcher      |
| --flexmatcher-s   | False    | path to the discvoery for testbed S                     |
| --flexmatcher-m   | False    | path to the discvoery for testbed M                     |
| -o, --output      | True     | path to write the results metrics e.g. confusion matrix |
| --help            | False    | Prints the parameter summary                            |

The result of the code is the file Comparison_state_of_the_art.txt` with the binary confusion matrices and their metrics. If discoveries for testbeds S and M are provided. The code will merge the discovery results for each solution.


## Discovery of semantic non-sytactic relationships

For this experiment, it is necessary to download the following [models]() since we deactivating the chain classifier strategy for this kind of pairs. The code needs the following parameters:

| Parameter          | Required | Description                                 |
|--------------------|----------|---------------------------------------------|
| -m, --models-dir   | True     | Path to the folder containing the models    |
| -o, --output       | True     | Path to write the results                   |
| -s, --semantics-ns | True     | Path to the semantic non-syntactic csv file |
| -h, --help         | False    | Show help message                           |