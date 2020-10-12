# Discovery of Semantic Non-Syntactic relationships



These experiments are packaged in a JAR file and is designed to work with `spark-submit` and NextiaJD. For all experiments of NextiaJD we are going to use the same JAR and can be downloaded from

This JAR contains three main classes:

*  **NextiaJD_evaluation** will compute the predictive accuracy: generates the discovery and time execution for the testbed
*  **EvaluateDiscovery** Confusion matrix generator: generates the confusion matrices  and their metrics for the state-of-the-art
*  **SemanticNS** will performa discovery for the semantic non-sytactic relationships

## Prerequisites

* NextiaJD. To see how to install NextiaJD [check this page](https://github.com/dtim-upc/NextiaJD#installation).
* The spark-submit script. You can find this scrip in your Spark installation under bin folder e.g $SPARK_HOME/bin
* Download the following [zip file](https://mydisk.cs.upc.edu/s/3fa7RQHoycE95F7/download) and uncompress it. This zip contains the Random Forest models built without chain classifiers.
* Download the [file](https://mydisk.cs.upc.edu/s/eN6XqEJWYAkSP38/download). This file contains the ground truth and distances produced by the semantic non-syntactic pairs.
* Download the [JAR]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download). This JAR contains the class `SemanticNS` which compute the predictions for the Semantic Non-Syntactic pairs using the models provided.

## Run

The class `SemanticNS` requires the following parameters:

| Parameter          | Required | Description                                 |
|--------------------|----------|---------------------------------------------|
| -m, --models-dir   | True     | Path to the folder containing the models    |
| -o, --output       | True     | Path to write the results                   |
| -s, --semantics-ns | True     | Path to the semantic non-syntactic csv file |
| -h, --help         | False    | Show help message                           |


To run this experiment, you need to execute the class **SemanticNS** from the [JAR]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download) with spark-submit. An example of how to run this class with Spark-submit is:

```
spark-submit \
--class "SemanticNS" \
--master local[*] /Users/javierflores/Documents/Research/Projects/FJA/prueba2/target/scala-2.12/nextiajd-experiments.jar \
-o /Users/javierflores/Documents/Research/Projects/FJA/prueba/lib \
-m /Users/javierflores/Documents/Research/Projects/FJA/prueba/lib/models \
-s /Users/javierflores/Downloads/semanticns_publish/semantic_ns.csv

```

### Results

The results of the code is:

*   NextiaJD_semanticNS.txt contains the binary confusion matrix and its metrics from the discovery result.
