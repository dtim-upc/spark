# Reproducibility of experiments

Here, you can find the code and instructions to reproduce the experiments to evaluate NextiaJD. On the one hand, we quantify the ability of the model to discover high quality joins through several experiments, and on the other hand we compare its performance to state of the art competitors.

## Setting
For evaluation purposes, we collected 139 real datasets. We further divided such datasets into 4 testbeds (extra-small, small, medium and large) according to their file size. The  characteristics and the download link for each testbed is in the following table:


| Testbed | File size       | datasets | String attributes |
|---------|-----------------|----------|-------------------|
| [XS](https://mydisk.cs.upc.edu/s/eCmfrNEBSKkcWcn/download)      | up to 1mb       | 28       | 159               |
| [S](https://mydisk.cs.upc.edu/s/dX3FajwWZn7rrrd/download)       | up to 100mb     | 46       | 590               |
| [M](https://mydisk.cs.upc.edu/s/niPyR4WTtxydprj/download)       | up to 1gb       | 46       | 600               |
| [L](https://mydisk.cs.upc.edu/s/4qoi76ziT2wJaCR/download)       | bigger than 1gb | 29       | 331               |

Each testbed zip file contains the below files:

* **datasetInformation_testbedX.csv** contains a list with the datasets names, the configuration to read them properly and the dataset source.
* **groundTruth_testbedX.csv** contains the ground truth with the containment obtained and the quality assigned for each pair of attributes.
* **dataset folder** contains all datasets used for the testbed

## Experiment 1: Predictive accuracy 

The goal of the first experiment is to evaluate the prediction quality on generating a ranking of candidate equi join predicates. 

The code and instructions for this experiment can be found [here](https://github.com/dtim-upc/NextiaJD2/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/NextiaJD#nextiajd-experiments).

## Experiment 2: Comparison with the state-of-the-art

We compare our approach with the following state-of-the-art data discovery solutions, whose source code is openly available: [LSH Ensemble](https://github.com/ekzhu/datasketch) and [FlexMatcher](https://github.com/biggorilla-gh/flexmatcher). 
The code and instruction for each solution can be found in the following links:

* [NextiaJD](https://github.com/dtim-upc/NextiaJD2/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/NextiaJD#nextiajd-experiments) -- Note that it is not necessary to run again this step if you previously executed Experiment 1.
* [FlexMatcher](https://github.com/dtim-upc/NextiaJD2/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/FlexMatcher#flexmatcher-discovery)
* [LSH Ensemble](https://github.com/dtim-upc/NextiaJD2/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/LSH%20Ensemble#lsh-ensemble-discovery)

Each solution will generate a discovery file with the results proposed. Afterwards, in order to combine the results, we provide you with class `EvaluateDiscovery`. The instructions on how to use it are described in the following [website](https://github.com/dtim-upc/NextiaJD2/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/DiscoveryMetrics).

## Experiment 3: Scalability

The most intensive task for our approach in terms of computational resources is the generation of attribute profiles from datasets. Hence, we performed a stress test of this component. We created a jar to prepare the files for this experiment. The code and more information can be found [here](https://github.com/dtim-upc/NextiaJD2/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/Scalability).

## Experiment 4: Discovery of semantic non-syntactic relationships

This experiment evaluates if NextiaJD can find semantic attributes represented in different formats. The code for this experiment can be found [here](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/SemanticNS#discovery-of-semantic-non-syntactic-relationships).
