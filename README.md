

<h1 align="center">
  <a href="https://www.essi.upc.edu/dtim/nextiajd/"><img src="https://github.com/dtim-upc/spark/blob/nextiajd_v3.0.1/sql/nextiajd/img/logo.png?raw=true" alt="NextiaJD" width="300">
  </a>
</h1>

<h4 align="center">A Scalable Data Discovery solution using profilies based on <a href="https://spark.apache.org/" target="_blank">Apache Spark</a>.</h4>

<p align="center">
  <a href="#about">About</a> •
  <a href="#key-features">Key Features</a> •
  <a href="#how-it-works">How it works</a> •
  <a href="#usage">Usage</a> •
  <a href="#installation">Installation</a> •
  <a href="#demo">Demo</a> •
  <a href="#experiments-reproducibility">Reproducibility</a>
</p>

## About
**NextiaJD**, from <a href="https://nahuatl.uoregon.edu/content/nextia" target="_blank">*nextia*</a> in the <a href="https://en.wikipedia.org/wiki/Nahuatl" target="_blank">Nahuatl</a> language (the old Aztec language), is a scalable data discovery system. **NextiaJD** computes profiles, which are succint representations of the underlying characteristics of datasets and their attributes, to efficiently discover joinable attributes on datasets. We aim to automatically discover pairs of attributes in a massive collection of heterogeneous datasets (i.e., data lakes) that can be crossed.

Here, we provide you detailed information on how to run and evaluate NextiaJD. To learn more about the project, visit our [website](https://www.essi.upc.edu/dtim/nextiajd/).

## Key features   
* Attribute profiling built-in Spark  
* A fully distributed end-to-end framework for joinable attributes discovery.  
* Easy data discovery for everyone  

## How it works

We encourage you to read our paper to better understand what NextiaJD is and how can fit your scenarios. 

The simple way to describe it: 

<div align="center">
 <img src="https://github.com/dtim-upc/spark/raw/nextiajd_v3.0.1/sql/nextiajd/img/example.gif?raw=true" alt="NextiaJD" width="300">
</div>

You have one dataset and a collection of independent datasets. Then, you will like to find other datasets with attributes that performed a high quality join.
 
NextiaJD reduces the effort to do a manual exploration by predicting which attributes are candidates for a join based on some qualities defined. 

We have as an example two scenarios:

* In a data lake when a new dataset is ingested,  a profile should be computed. Then, whenever a data analysts has a dataset, NextiaJD can find other datasets in the data lake that can be joined.
* In a normal repository,  when having a few datasets and we want to know how they can be crossed against one dataset.

## Requirements

* Spark 3.0.1
* Scala 2.12.
* Java 8 or 11

## Installation
  
There are two options to install NextiaJD in your computer: <a href="#by-building-nextiajd-jars">building the jars from this repository using Maven</a> or <a href="#by-downloading-the-compiled-jars">downloading the NextiaJD compiled jars</a> *(Recommended)*
  
### Build from sources 

To install NextiaJD you need to follow the steps below:

* Clone this project
```  
$ git clone https://github.com/dtim-upc/NextiaJD
```  
* Go to the project root directory in a terminal
* Run the command below. It will build the spark catalyst, spark sql and spark nextiajd jars through Maven. Note that this will take some time.
    * Alternatively, you can build the whole Spark project as specified [here](https://spark.apache.org/docs/latest/building-spark.html)
```  
$ ./build/mvn clean package -pl :spark-catalyst_2.12,:spark-sql_2.12,:spark-nextiajd_2.12 -DskipTests 
```
* If the build succeeds, you can find the compiled jars under the following directories:
    * /sql/nextiajd/target/spark-nextiajd_2.12-3.0.1.jar
    * /sql/core/target/spark-sql_2.12-3.0.1.jar
    * /sql/catalyst/target/spark-catalyst_2.12-3.0.1.jar
* Then go to your Spark directory under the **jars** folder e.g. **$SPARK_HOME/jars**
* Place the downloaded JARs inside the **jars** folder (replace any if necessary)
* You are now ready to use NextiaJD        

### Download the compiled JARs

To install NextiaJD you need to follow the steps below:

* First, you need to download the compiled jars using these links: 
    * [Spark-NextiaJD](https://mydisk.cs.upc.edu/s/7wKRxp3DJTgQ7yb/download)
    * [SparkSQL](https://mydisk.cs.upc.edu/s/B36NjoYC6LTP5GQ/download)
    * [Catalyst](https://mydisk.cs.upc.edu/s/j6KfLkgqxtprDod/download)
* Go to your Spark directory under the **jars** folder e.g. **$SPARK_HOME/jars**
* Place the downloaded JARs inside the **jars** folder (replace any if necessary)
* You are now ready to use NextiaJD    

## Usage    
         
### Attribute profiling  
  
To start a profiling we can use the method `attributeProfile()`from a DataFrame object. By default, once a profile is computed, it will be saved in the dataset directory. This allows to reuse the profile for future discoveries without having to compute it again. While you can use any dataset format, we recommend to use parquet files to compute profiles faster.
  
```  
val dataset = spark.read.csv(...)  
# computes attribute profile
dataset.attributeProfile() 
# returns a dataframe with the profile information
dataset.getAttributeProfile()   
```  
  
### Join Discovery  
  
Our Join Discovery is focused on the quality result of a join statement. Thus, we defined a totally-ordered set of quality classes:

* **High**: attributes pair with a containment similarity of 0.75 and a maximum cardinality proportion of 4.    
* **Good**: attributes pair with a containment similarity of 0.5 and a maximum cardinality proportion of 8.     
* **Moderate**: attributes pair with a containment similarity of 0.25 and a maximum cardinality proportion of 12.     
* **Poor**: attributes pair with a containment similarity of 0.1    
* **None**: otherwise   

You can start a discovery by using the function `discovery()` from `org.apache.spark.sql.NextiaJD`. As an example the following code will start a discovery to find any attribute from our dataset that can be used for a join with some dataset from the repository.
  
```  
import org.apache.spark.sql.NextiaJD.discovery
val dataset = spark.read.csv(...) 
val repository = # list of datasets  

import org.apache.spark.sql.NextiaJD.discovery

discovery(dataset, repository)
```    

By default, we just show candidates attributes that performs a High and Good quality joins. If you want to explore Moderate and Poor results, the discovery function have the boolean parameters `showModerate` and `showPoor`. Once enable, the discovery only show results for the specified quality.  

## Demo (Zeppelin Notebook) 

Check out the [demo project](http://35.222.211.178:8080/#/notebook/2FQSWVBQ8) for a quick example of how NextiaJD works. Bear in mind that, in order to access them you must first login with the following credentials (user: user1, password: nextiajd).

Note that we also have a step by step notebook which can also be found [here](http://35.222.211.178:8080/#/notebook/2FQSWVBQ8)

More information and a video can be found [here](https://www.essi.upc.edu/~snadal/nextiajd.html#demonstration)
 
## Reproducibility of Experiments

We performed differents experiments to evaluate the predictive performance and efficiency of NextiaJD. In the spirit of open research and experimental reproducibility, we provide detailed information on how to reproduce them. More information about it can be found [here](https://github.com/dtim-upc/NextiaJD2/tree/nextiajd_v3.0.1/sql/nextiajd/experiments#experiments-reproducibility).
