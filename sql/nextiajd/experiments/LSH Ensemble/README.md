# LSH Ensemble discovery

## Installation

The only prerequisite is Python > 3 (tested with 3.3)

1. Clone the repository
2. Install the dependencies by running the following command:
```
pip install -r requirements.txt
```


## Running 

1. Run `LSHEnsemble_comparison.py` to execute the discovery

Required arguments 

| Parameter     | Required | Description                                                                             |
|---------------|----------|-----------------------------------------------------------------------------------------|
| --datasetInfo | True     | Path to the CSV file with the datasets names and the configuration to read them         |
| --datasetsDir | True     | Path to the Datasets folder                                                             |
| --output      | True     | Path to write discovery results and time execution                                      |
| --testbed     | False    | testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated |