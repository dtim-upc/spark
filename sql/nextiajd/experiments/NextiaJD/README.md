# NextiaJD experiments

These experiments are packaged in a JAR file and is designed to work with `spark-submit` and NextiaJD. For all experiments of NextiaJD we are going to use the same JAR and can be downloaded from [here]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download)

This JAR contains three main classes:

*  **NextiaJD_evaluation** will compute the predictive accuracy: generates the discovery and time execution for the testbed
*  **EvaluateDiscovery** Confusion matrix generator: generates the confusion matrices  and their metrics for the state-of-the-art
*  **SemanticNS** will performa discovery for the semantic non-sytactic relationships

## Prerequisites

* NextiaJD. To see how to install NextiaJD [check this page](https://github.com/dtim-upc/NextiaJD#installation). 
* The spark-submit script. You can find this scrip in your Spark installation under bin folder e.g $SPARK_HOME/bin
* A testbed generated for the experiment. See [here](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments) for more information.

## Running Predictive accuracy


To run this experiment, you need to execute the class **NextiaJD_evaluation** from the [JAR]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download). This class have the following arguments:


| Parameter           | Required | Description                                                                                                 |
|---------------------|----------|-------------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | True     | path to datasets info file. This file containsthe datasets names and their configuration to read them       |
| -g, --groud-truth   | True     | path to the ground truth csv file                                                                           |
| -o, --output        | True     | path to write the discovery and time results                                                                |
| -p, --path-datasets | True     | path to the folder containing all datasets                                                                  |
| -q, --query-type    | False    | The query search. There are two types:querybydataset and querybyattribute. Default value is querybydataset. |
| -t, --testbed       | False    | testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated. Default is ""      |
| -h, --help          | False    | Show help message                                                                                           |

An example of how to run this class with Spark-submit is:

```
spark-submit \
--class NextiaJD_evaluation \
--master local[*]  /DTIM/nextiajd-experiments.jar \
-d /DTIM/testbedXS/datasetInformation_testbedXS.csv \
-g /DTIM/testbedXS/groundTruth_testbedXS.csv \
-p /DTIM/datasets -o /DTIM/output
```


### Results

The results of the code are:

*   NextiaJD_testbed.csv: file containing the discovery results and the ground truth
*   Nextia_evaluation_testbed.csv: file containing the confusion matrix and its metrics
*   time_testbed.txt: file containing the times from the execution: pre runtime and runtime

## Discovery metrics

To run this experiment, you need to execute the class **EvaluateDiscovery** from the [JAR](https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download) .This code allows to obtain the metrics (confusion matrix) for the discovery results of NextiaJD, LSH Ensemble and FlexMatcher. The following parameters are needed:


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

You need to run it with spark-submit. An example of how to run this class with Spark-submit is:

```

spark-submit \
--class EvaluateDiscovery \
--master local[*] /DTIM/nextiajd-experiments.jar \
-n /DTIM/nextiaJD_testbedXS.csv \
--nextiajd-m /DTIM/nextiaJD_testbedM.csv \
--nextiajd-s /DTIM/nextiaJD_testbedS.csv \
-l /DTIM/LSH_textbedXS.csv \
--lsh-m /DTIM/lsh_testbedM.csv \
--lsh-s /DTIM/lsh_tesbedS.csv \
-f /DTIM/flextMatcherResults_testbedXS.csv \
--flexmatcher-m /DTIM/flextMatcherResults_testbedXS.csv \
--flexmatcher-s /DTIM/flextMatcherResults_testbedXS.csv \
-o /DTIM/output
```

### Results

The results of the code is:

*   Comparison_state_of_the_art.txt contains the binary confusion matrices and their metrics. If discoveries for testbeds S and M are provided. The code will merge the discovery results for each solution to generate just one matrix for solution.

