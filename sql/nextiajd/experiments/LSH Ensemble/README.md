# LSH Ensemble discovery

## Requirements

* Python 3.6

## Installation

* Clone the repository
* Open the terminal
* Go to the project root directory
* Install the dependencies by running the following command:
```
pip install -r requirements.txt
```

## Parameters

The below parameters are required:

| Parameter     | Required | Description                                                                             |
|---------------|----------|-----------------------------------------------------------------------------------------|
| --datasetInfo | True     | Path to the CSV file with the datasets names and the configuration to read them         |
| --datasetsDir | True     | Path to the Datasets folder                                                             |
| --output      | True     | Path to write discovery results and time execution                                      |
| --testbed     | False    | testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated |


## Run

* Open the terminal
* Go to the directory for the file [LSHEnsemble_comparison.py](https://github.com/dtim-upc/NextiaJD/blob/nextiajd_v3.0.1/sql/nextiajd/experiments/LSH%20Ensemble/LSHEnsemble_comparison.py)
* Run the `LSHEnsemble_comparison.py` to execute the discovery by using the below command. Note you should replace the parameters values with your own.

```
python LSHEnsemble_comparison.py \
--datasetInfo /DTIM/datasetInformation_testbedXS.csv \
--datasetsDir /DTIM/datasets \
--output /DTIM/output
```
