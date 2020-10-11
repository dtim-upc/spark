# Evaluation

Here you can find the experiments done for evaluate NextiaJD. On the one hand, we quantify the ability of the model to discover high quality joins through several experiments, and on the other hand we compare its performance to state of the art competitors.

## Setting
For evaluation purposes, we collected 139 real datasets. We further divided such datasets into 4 testbeds (extra-small, small, medium and large) according to their file size. The  characteristics of each testbed is in the following table:


| Testbed | File size       | datasets | String attributes |
|---------|-----------------|----------|-------------------|
| XS      | up to 1mb       | 28       | 159               |
| S       | up to 100mb     | 46       | 590               |
| M       | up to 1gb       | 46       | 600               |
| L       | bigger than 1gb | 29       | 331               |

The testbeds are public and can be downloaded from [here](https://mydisk.cs.upc.edu/s/mXMnNo4ARAPxLg3?path=%2Finput_datasets). Each testbed zip file contains the following files:

* **datasetInformation_testbedX.csv** contains a list with the datasets names, the configuration to read them properly and the dataset source.
* **groundTruth_testbedX.csv** contains the ground truth with the containment obtained and the quality assigned for each pair of attributes.
* **dataset folder** contains all datasets for the testbed
## Predictive accuracy 

The goal of the first experiment is to evaluate the prediction quality on generating a ranking of candidate equi join predicates. The code for this experiment can be found [here](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/NextiaJD)

## Data Discovery for the state-of-the-art

We compare our approach with the following state-of-the-art data discovery solutions, whose source code is openly available: [LSH Ensemble](https://github.com/ekzhu/datasketch) and [FlexMatcher](https://github.com/biggorilla-gh/flexmatcher). 
The code for each comparison can be found in the following links:

* [NextiaJD](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/NextiaJD)
* [FlexMatcher](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/FlexMatcher)
* [LSH Ensemble](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/LSH%20Ensemble)

Each project will generated a file with the discovery result. To evaluate them, we can use the class `EvaluateDiscovery.scala` in this [project](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/NextiaJD)


## Scalability

The most intensive task for our approach in terms of computational resources is the generation of attribute profiles from datasets. Hence, we performed a stress test of this component. We created a jar to prepare the files for this experiment. The code and more information can be found [here](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/Scalability)



## Discovery of semantic non-syntactic relationships

This experiment evaluates if NextiaJD can find semantic attributes represented in different formats. The code for this experiment can be foun [here](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/NextiaJD)



