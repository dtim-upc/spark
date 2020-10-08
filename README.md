

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

The simple way to describe it: you have one dataset and a collection of independent datasets. Then, you will like to find other datasets with attributes that can be joined. NextiaJD reduces the effort to do a manual exploration by predicting which attributes are candidates for a join based on some qualities defined.

As an example, it can be used in two scenarios:

* In a data lake when a new dataset is ingested,  a profile should be computed. Then, whenever a data analysts has a dataset, NextiaJD can find other datasets in the data lake that can be joined.
* In a normal repository,  when having a few datasets and we want to know how they can be crossed against one dataset.


## Usage    
    
NextiaJD only supports Scala and Java programming. It is required those versions will be compatible with the version of Spark.    
  
### Attribute profiling  
  
To start a profiling we can use the method `attributeProfile()`from a DataFrame object. By default, once a profile is computed it is saved in the dataset directory. This allows to reuse the profile for future discoveries.  
  
```  
val dataset = spark.read.csv(...)  
dataset.attributeProfile() dataset.getAttributeProfile() # returns a dataframe with the profile information  
```  
  
### Join Discovery  
  
A Join Discovery will return the attributes pairs ordered by their relevance as indicator of the quality of the resulting join. To get more information visit our [web page]()  
  
  
NextiaJD defines a totally-ordered set of quality classes:    
    
* High: attributes pair with a containment similarity of 0.5 and a maximum cardinality proportion of 4.    
* Good: attributes pair with a containment similarity of 0.5 and a maximum cardinality proportion of 4.     
* Moderate: attributes pair with a containment similarity of 0.5 and a maximum cardinality proportion of 4.     
* Poor: attributes pair with a containment similarity of 0.1    
* None: otherwise    
    
In case you want to start a dataset profile you can use the method. This option is recommended once a dataset is ingested into the repository    
uilding and maintaining    
 <!---
 ### [Guide video]()  
 -->   

  
## Installation
  
To install NextiaJD, we need to include some libraries to our Spark installation. There are two ways to get the compiled jars.  
  
* Directly download to the final compiled jars:
    * [Spark-NextiaJD](https://mydisk.cs.upc.edu/s/7wKRxp3DJTgQ7yb/download)
    * [SparkSQL](https://mydisk.cs.upc.edu/s/B36NjoYC6LTP5GQ/download)
    * [Catalyst](https://mydisk.cs.upc.edu/s/j6KfLkgqxtprDod/download)
* Build the source code from this repository as show below:  
   * Clone this project  
```  
$ git clone https://github.com/  
```  
* Build through Maven:  
```  
./build/mvn clean package -pl :spark-catalyst_2.12,:spark-sql_2.12,:spark-nextiajd_2.12 -DskipTests 
```
  This will build the spark catalyst, spark sql and spark nextiajd. Alternatively, we can build the whole Spark project as specified [here](https://spark.apache.org/docs/latest/building-spark.html). If the build succeeds, you can find the jars compile under the target folder for each module. Those jars should be copied and replaced to the jars folder from Spark.  
      
##  Demo  

Check out the [demo project](http://34.89.14.170:8000/notebooks/NextiaJD_demo.ipynb) for a quick example of how NextiaJD works. The password for the notebook is `password`
 
## Evaluation

We performed differents experiments to evaluate nextiaJD. More information about it can be found [here](https://github.com/dtim-upc/spark/tree/nextiajd_v3.0.1/sql/nextiajd/experiments)
