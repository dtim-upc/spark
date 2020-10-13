# NextiaJD experiments

Here you can performed the predictive accuracy for NextiaJD and compare the result with the state-of-the-art. 

## Prerequisites

* NextiaJD. To see how to install NextiaJD [check this page](https://github.com/dtim-upc/NextiaJD#installation). 
* The spark-submit script. You can find this script in your Spark installation under the bin folder e.g $SPARK_HOME/bin
* A testbed provided for the experiment. See [this link](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments) for more information.
* [NextiaJD_experiments.jar](https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download). This JAR should be run with `spark-submit` and we will use the following classes:
    *  **NextiaJD_evaluation** will compute the predictive accuracy: generates the discovery and time execution for the testbed
    *  **EvaluateDiscovery** Confusion matrix generator: generates the confusion matrices  and their metrics for the state-of-the-art


## Running Predictive accuracy

To run this experiment, you need to execute the class **NextiaJD_evaluation** from the [JAR]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download). You can see below the parameters needed for this class.


| Parameter           | Required | Description                                                                                                 |
|---------------------|----------|-------------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | True     | path to datasets info file. This file containsthe datasets names and their configuration to read them       |
| -g, --groud-truth   | True     | path to the ground truth csv file                                                                           |
| -o, --output        | True     | path to write the discovery and time results                                                                |
| -p, --path-datasets | True     | path to the folder containing all datasets                                                                  |
| -q, --query-type    | False    | The query search. There are two types:querybydataset and querybyattribute. Default value is querybydataset. |
| -t, --testbed       | False    | testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated. Default is ""      |
| -h, --help          | False    | Show help message                                                                                           |

### Run

* Go to your Spark installation under the bin folder e.g $SPARK_HOME/bin
* Open the terminal
* Run it using the below command. Note that you should replace the parameters by your directories.

```
spark-submit \
--class NextiaJD_evaluation \
--master local[*]  /DTIM/nextiajd-experiments.jar \
-d /DTIM/testbedXS/datasetInformation_testbedXS.csv \
-g /DTIM/testbedXS/groundTruth_testbedXS.csv \
-p /DTIM/datasets -o /DTIM/output
```
* Once the program ends. You can find the following files in the provided output directory.
    *  **NextiaJD_testbed.csv**: this file contains the discovery results and the ground truth
    *  **Nextia_evaluation_testbed.csv**: this file contains the confusion matrix and its metrics
    *  **time_testbed.txt**: this file contains the times from the execution: pre runtime and runtime

## Discovery metrics

To run this experiment, you need to execute the class **EvaluateDiscovery** from the [JAR](https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download) .This code allows to obtain the metrics (confusion matrix) for the discovery results of NextiaJD, LSH Ensemble and FlexMatcher. 
The below parameters are needed. Note that if you provided the parameters for different testbed discoveries, the code will merge the discovery results for each solution to generate just one matrix for solution. As an example, if you provide --nextiajd and --nextiajd-s, the code will merge both testbed to generate only one confusion matrix. 



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


### Run

* Go to your Spark installation under the bin folder e.g $SPARK_HOME/bin
* Open the terminal
* Run it using the spark-submit command, below you can see an example. Note that you should replace the parameters by your directories and only use the parameters neeeded.

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
* Once the program ends. You can find the following file in the provided output directory.
    *  **comparison_state_of_the_art.txt**: contains the binary confusion matrices and their metrics for each solution provided

