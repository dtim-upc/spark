# Discovery of Semantic Non-Syntactic relationships

For this experiment, we manually curated a test dataset of 532 semantic non-syntactic attribute pairs and compute the distances required by the models. You can find the datasets [here](https://mydisk.cs.upc.edu/s/aNbnSiSfg5xan6W/download). 

## Prerequisites

* NextiaJD. To see how to install NextiaJD [check this page](https://github.com/dtim-upc/NextiaJD#installation).
* The spark-submit script. You can find this scrip in your Spark installation under bin folder e.g $SPARK_HOME/bin
* Download the following [zip file](https://mydisk.cs.upc.edu/s/3fa7RQHoycE95F7/download) and uncompress it. This zip contains the Random Forest models built without chain classifiers.
* Download the [file](https://mydisk.cs.upc.edu/s/eN6XqEJWYAkSP38/download). This file contains the ground truth and distances produced by the semantic non-syntactic pairs. This ground truth was obtaining by applying the following three transformations: (1) string cleaning (e.g., remove accents, dashes, etc.), (2) dictionary lookup(e.g., country codes to country names, author names to full names etc.), and (3) unwind collections. 
* Download the [JAR]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download). This JAR contains the class `SemanticNS` which compute the predictions for the Semantic Non-Syntactic pairs using the models provided.

## About

To run this experiment, you need to execute the class **SemanticNS** from the [JAR]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download) with spark-submit. 

### Parameters

The class `SemanticNS` requires the following parameters:

| Parameter          | Required | Description                                 |
|--------------------|----------|---------------------------------------------|
| -m, --models-dir   | True     | Path to the folder containing the models    |
| -o, --output       | True     | Path to write the results                   |
| -s, --semantics-ns | True     | Path to the semantic non-syntactic csv file |
| -h, --help         | False    | Show help message                           |

### Run


* Go to your Spark installation under the bin folder e.g $SPARK_HOME/bin
* Open the terminal
* Run it using spark-submit as the below command. Note that you should replace the parameters by your own directories.

```
spark-submit \
--class SemanticNS \
--master local[*] /Users/javierflores/Documents/Research/Projects/FJA/prueba2/target/scala-2.12/nextiajd-experiments.jar \
-o /Users/javierflores/Documents/Research/Projects/FJA/prueba/lib \
-m /Users/javierflores/Documents/Research/Projects/FJA/prueba/lib/models \
-s /Users/javierflores/Downloads/semanticns_publish/semantic_ns.csv
```
* Once the program ends. You can find the following file in the provided output directory.
    *  **NextiaJD_semanticNS.txt**: contains the binary confusion matrix and its metrics from the discovery result.

