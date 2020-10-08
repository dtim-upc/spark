# NextiaJD: predictive accuracy and comparison

# About

Here you can find the code for:

*  Predictive accuracy: generates the discovery and time execution for the testbed
*  Comparison: generates the confusion matrices and their metrics for the state-of-the-art

## Installation

The code follows the structure for an SBT project. It is necessary to place NextiaJD jars in the lib folder.

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

# Comparison

This code allows to compare the results of NextiaJD, LSH Ensemble and FlexMatcher. The following parameters are required:


| Parameter         | Required | Description                                             |
|-------------------|----------|---------------------------------------------------------|
| -n, --nextiajd    | True     | path to the discovery result file from nextiajd         |
| -l, --lsh         | True     | path to the discovery result file from lsh              |
| -f, --flexmatcher | True     | path to the discovery result file from flexmatcher      |
| -o, --output      | True     | path to write the results metrics e.g. confusion matrix |
| --help            | False    | Prints the parameter summary                            |

The result of the code is the file Comparison_state_of_the_art.txt with all confusion matrices and their metrics.