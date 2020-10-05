

<h1 align="center">
  <a href="https://www.essi.upc.edu/dtim/"><img src="https://github.com/dtim-upc/spark/blob/nextiajd_v3.0.1/sql/nextiajd/img/logo.png?raw=true" alt="NextiaJD" height="200">
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
  <a href="#benchmarks">Benchmarks</a> •
</p>

## About
**NextiaJD** is a Scalable Data Discovery solution using profiles. We aim to  discover automatically attributes pairs in a massive collection of heterogeneous datasets (i.e., data lakes) that can be crossed.     
  
To learn more about it, visit our [web page]()  

## Key features   
* Attribute profiling built-in Spark  
* A fully distributed end-to-end framework for joinable attributes discovery.  
* Easy data discovery for everyone  

## How it works

We encourage you to read our paper to better understand what NextiaJD is and how can fit your scenarios. 

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
    
 ### [Guide video]()  
  
## Installation
  
To install NextiaJD, we need to include some libraries to our Spark installation. There are two ways to get the compiled jars.  
  
* Directly download the final compiled jars in this [repository](https://mydisk.cs.upc.edu/s/mXMnNo4ARAPxLg3?path=%2Frelease)  
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

Check out the [demo project](https://www.essi.upc.edu/dtim/) for a quick example of how NextiaJD works. 
 
## Benchmarks   
NextiaJD were compared with LSH Ensemble and FlexMatcher. The code for generating the benchmark can be found [at]() . Each approach were tested with real datasets. We create the following testbeds:   
  
* Testbed XS : datasets smaller than 1mb  
* Testbed S : datasets smaller than 100mb   
* Tesbed M : datasets smaller than 1gb   
* Testbed L : datasets bigger than 1gb