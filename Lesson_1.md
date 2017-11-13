# Big Data

- Huge data that classical applications cannot handle
- Analysis,storage, capture, search, sharing, transfer, visualization,querying, updating...
- Can be described by the following features: Volume, Variety, Velocity, Variability (inconsistency of dataset), Veracity (quality of captured data)


# Architecture history

- 2000 Seisintdeveloped C++ based distributed filesharing framework for data storage and query
- 2004 LexisNexis acquired Seisint and in 2008 acquired ChoicePoint. The two platforms were merged in HPCC 
- 2004, Google published paper on MapReduce that uses a similar architecture. With it, queries are distributed across parallel nodes and processed in parallel (map). Results are gathered and delivered (reduce). An implementation of MapReduce was developed by Hadoop, an Apache open-source project.


# Apache Spark

- Fast
- Easy to use
- General engine (let us combine multiple types of computations)

It is a cluster computing platform designed to be fast and general purpose.
It extends MapReduce to support more types of computations, including interactive queries and stream processing. It can run computations in memory and it is also more efficient than MapReduce for complex applications running on disks (10-20x faster).
It offers APIs in Python, Java, Scala. It can run in Hadoop clusters and access any Hadoop data source.


# The Spark stack

- Allows querying data via SQL and HQL (hive query language).
- Enables processing of live streams of data. 
- It includes several types of machine learning algorithms.
- Library to manipulate graphs and perform graph-parallel computations.
- Spark can run over a variety of cluster managers including Hadoop Yarn, Apache Mesos and a simple cluster manager included in Spark.
- Basic functionality for task management, memory management, API for RDDs. 
- 
  Resilient Distributed Data: collection of items distributed across many compute nodes that can be manipulated in parallel.


# Introduction to Spark

- Spark is written in Scala and runs on the Java Virtual Machine (JVM). It requires Java 6.
- From your Spark directory go to `/usr/local/` spark and then type `bin/pyspark` for Python `bin/spark-shell` for Scala.


# Practices

- SSH servers
- Downloading Spark
- Running it locally
- Use of standalone application
- Examples of RDD and parallel operations on them


# Core Spark Concepts

- Every Spark application consists of a driver program that launches various parallel operations on a cluster
- Driver programs access Spark through a SparkContext object representing a connection to the computing cluster (in the shell sc is the SparkContext automatically created)
- Try to type sc on the shell to see what happens
- Once you have a SparkContext you can use it to build RDDs (we previously used `sc.textFile()` to create an RDD representing the lines oftextina file. On these lines we can perform various operations)
- To run these operations ,driver programs manage a number of nodes called executors.
- E.g.we we call count different machines might count lines in different ranges. With shell it runs the job locally but **we can connect the same shell to a cluster and analyze data in parallel**


# Example

 

```python
lines = sc.textFile(“README.md”)
pythonLines = lines.filter(lambda line: "Python" in line)
pythonLines.first()

```

# Standalone Applications

- Spark can be linked into standalone programs. You need to initialize your own SparkContext and then API is the same
- In Python you write the application and run using `bin/spark-submit` which includes the dependencies for us in Python
- Then you need to import the Spark packages in your program and createa SparkContext

```python
from pysparkimport SparkConf, SparkContext
conf= SparkConf().setMaster("local").setAppName("My App")
sc= SparkContext(conf= conf)
```
You need to pass 2 parameters (cluster URL and application name) `sys.exit()` or `stop()` method on your SparkContext exits the program.

# Exercise

- Set up your own Java and Scala environment to launch standalone programs
- Next time we’ll run Hello worlds standalone programs written by you.

# Appunti miei
- Spark is a cluster computing platform

- Spark extends the popular MapReduce model to efficiently support more types of computations, including interactive queries and stream processing.

- Spark offers for speed is the ability to run computations in memory, but the system is also more efficient than MapReduce for complex applications running on disk

- Spark is designed to be highly accessible, offering simple APIs in Python, Java, Scala, and SQL, and rich built-in libraries. It also integrates closely with other Big Dat tools. In particular, Spark can run in Hadoop clusters and access any Hadoop data source, including Cassandra.

#Spark Core 
contains the basic functionality of Spark, including components for task scheduling, memory management, fault recovery, interacting with storage systems,
and more. Spark Core is also home to the API that defines resilient distributed data‐sets (RDDs), which are Spark’s main programming abstraction. RDDs represent a collection of items distributed across many compute nodes that can be manipulated in parallel. Spark Core provides many APIs for building and manipulating these collections.

#Storage
Spark can create distributed datasets from any file stored in the Hadoop distributed filesystem (HDFS) or other storage systems supported by the Hadoop APIs (including your local filesystem, Amazon S3, Cassandra, Hive, HBase, etc.). It’s important to
remember that Spark does not require Hadoop;
