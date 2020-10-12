

<h1 align="center">
  <a href="https://www.essi.upc.edu/dtim/nextiajd/"><img src="https://github.com/dtim-upc/spark/blob/nextiajd_v3.0.1/sql/nextiajd/img/logo.png?raw=true" alt="NextiaJD" width="300">
  </a>
</h1>

<h4 align="center">A Scalable Data Discovery solution using profilies based on <a href="https://spark.apache.org/" target="_blank">Apache Spark</a>.</h4>


<p align="center">
  <a href="#how-to-use">About</a> •
  <a href="#key-features">Key Features</a> •
  <a href="#how-it-works">How it works</a> •
  <a href="#usage">Usage</a> •
   <a href="#installation">Installation</a> •
  <a href="#demo">Demo</a> •
  <a href="#benchmarks">Benchmarks</a>
</p>

## About
**NextiaJD** is a Scalable Data Discovery solution using profiles. We aim to  discover automatically attributes pairs in a massive collection of heterogeneous datasets (i.e., data lakes) that can be crossed.     
  
To learn more about it, visit our [web page](https://www.essi.upc.edu/dtim/nextiajd/)  

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

## Installation
  
To run NextiaJD in your computer, there are two options: by downloading the compiled jars *(Recommended)* or by building the jars from this project. Both options are explained in this section.
  
### Pre-requisites

* Spark 3.0.1
* Scala 2.12.
* Java 8 or 11
### By Downloading compiled jars

You can download the final compiled jars using these links: 

    * [Spark-NextiaJD](https://mydisk.cs.upc.edu/s/7wKRxp3DJTgQ7yb/download)
    * [SparkSQL](https://mydisk.cs.upc.edu/s/B36NjoYC6LTP5GQ/download)
    * [Catalyst](https://mydisk.cs.upc.edu/s/j6KfLkgqxtprDod/download)

Once you have the jars, you need to move them to your Spark installation directory under the folder jars. Usually this directory is in $SPARK_HOME/jars. Note that it is necessary to replace spark-sql and spark-catalyst with our compile jars.

### By Building NextiaJD jars 

Build the source code from this repository. The following commands will build the spark catalyst, spark sql and spark nextiajd jars:
   * Clone this project  
```  
$ git clone https://github.com/  
```  
* Build through Maven:  
```  
./build/mvn clean package -pl :spark-catalyst_2.12,:spark-sql_2.12,:spark-nextiajd_2.12 -DskipTests 
```

Alternatively, we can build the whole Spark project as specified [here](https://spark.apache.org/docs/latest/building-spark.html). If the build succeeds, you can find the compiled jars under the folders:

* /sql/nextiajd/target/spark-nextiajd_2.12-3.0.1.jar
* /sql/core/target/spark-sql_2.12-3.0.1.jar
* /sql/catalyst/target/spark-catalyst_2.12-3.0.1.jar

Once you have the jars, you need to move them to your Spark installation directory under the folder jars. Usually this directory is in $SPARK_HOME/jars. Note that it is necessary to replace spark-sql and spark-catalyst with our compile jars.



      

## Usage    
         
### Attribute profiling  
  
To start a profiling we can use the method `attributeProfile()`from a DataFrame object. By default, once a profile is computed it is saved in the dataset directory. This allows to reuse the profile for future discoveries without having to compute it again.
  
```  
val dataset = spark.read.csv(...)  
dataset.attributeProfile() dataset.getAttributeProfile() # returns a dataframe with the profile information  
```  
  
### Join Discovery  
  
Our Join Discovery is focused on the quality result of a join statement. Thus, we defined a totally-ordered set of quality classes:

* High: attributes pair with a containment similarity of 0.75 and a maximum cardinality proportion of 4.    
* Good: attributes pair with a containment similarity of 0.5 and a maximum cardinality proportion of 8.     
* Moderate: attributes pair with a containment similarity of 0.25 and a maximum cardinality proportion of 12.     
* Poor: attributes pair with a containment similarity of 0.1    
* None: otherwise   

You can start a discovery by using the function `discovery()` from NextiaJD. As an example the following code will start a discovery to find any attribute from our dataset that can be used for a join with some dataset from the repository.
  
```  
val dataset = spark.read.csv(...) 
val repository = # list of datasets  

import org.apache.spark.sql.NextiaJD.discovery

discovery(dataset, repository)
```    

By default, we just show candidates attributes that performs a High and Good quality joins. If you want to explore Moderate and Poor results, the discovery function have the parameters `showModerate:Boolean` and `showPoor:Boolean` that allows to see the specified quality in the results. 
  

  

##  Demo  

Check out the [demo project](http://34.89.14.170:8000/notebooks/NextiaJD_demo.ipynb) for a quick example of how NextiaJD works. The password for the notebook is `password`
 
## Experiments Reproducibility

We performed differents experiments to evaluate nextiaJD. More information about it can be found [here](https://github.com/dtim-upc/NextiaJD/tree/nextiajd_v3.0.1/sql/nextiajd/experiments)
