# Discovery metrics

This is a helper code that will aid on the collection of metrics (confusion matrices) for the results obtained after executing NextiaJD, LSH Ensemble and FlexMatcher. 

To run the program experiment, you need to execute the class **EvaluateDiscovery** from the [JAR](https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download).

If you provide the parameters for different testbed discoveries, the code will merge the discovery results for each solution to generate a single confusion matrix combining the results from all the rest. As an example, if you provide --nextiajd and --nextiajd-s, the code will merge both testbed to generate only one confusion matrix. 

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


## Run

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
    *  **comparison\_state\_of\_the\_art.txt**: contains the binary confusion matrices and their metrics for each solution provided

