Since its release, Apache Spark has seen rapid adoption byenterprises across a wide range of industries. Internet powerhouses such as Netflix, Yahoo, and eBay have deployed Spark at massive scale, collectively processing multiple petabytes of data on clusters of over 8,000 nodes.


**The team that created Apache Spark founded Databricksin 2013.**

Apache Spark is 100% open source, hosted at the vendor independent Apache Software Foundation. At Databricks, we are fully committed to maintaining this open development model.

Together with the Spark community, Databricks continues to contribut eheavily to the Apache Spark project,through both development and community evangelism

# mapPartitions

```python
mapPartitions(f, preservesPartitioning=False)

# Return a new RDD by applying a function to each partition of this RDD.

rdd= sc.parallelize([1, 2, 3, 4], 2) 

deff(iterator): yield sum(iterator) 

rdd.mapPartitions(f).collect() [3, 7]

#Loadingand SavingYour Data

```

#Loading and Saving your Data

How to load and save when data don’t fit on a single machine? Spark can access data through the InputFormat and OutputFormat interfaced used by Hadoop MapReduce(later lectures).

You want to use higher-level APIs built on top of these interfaces.

- File formats and filesystems Spark can access text, JSON, SequenceFiles, protocol buffers
- Structured data sources through Spark SQL API for structured data sources (JSON and Apache Hive)
- Databases and key/value stores Built-in and third-party libraries for connecting to Cassandra, Hbase, Elasticsearch, JDBC databases


# File Formats

Format ranges from unstructured, text, to semistructured, JSON, to structured, SequenceFiles. Method `textFile()` handles automatically the conversion based on file extension. Besides the output mechanism supported in Spark we can se both Hadoop’s new and old APIs for keyed data.

# Text Files


Each line of the text file becomes an element in the RDD. We can also load multiple whole text files at the same time into a pair RDD. 

To handle multipart inputs we can use textFileand pass it a directory. If our files are small we can use `SparkContext.wholeTextFiles()` method and get back a pair RDD where the key is the name of the input file. 

`SaveAsTextFile()` takes a path and outputs the contents of the RDD to that file. 

Path is treated as a directory and Spark will output multiple files underneath that directory. 

This allows Spark to write the output from multiple nodes. `results.saveAsTextFile(outputFile)`


# JSON

JSON is a popular semistructureddata format. To load JSON data first  load the data as a text file and then map the values with a JSON parser. Writing JSON is easier. Example if jsonis on same line

```python
import json
input = sc.textFile(“infomultiple.json”)

# Voglio mappare lambda x con json.loads(x)
data = input.map(lambda x: json.loads(x))

# Otherwise first read the whole text in a variable and then parse it
import json
input = sc.wholeTextFiles(“info.json”)
data = input.map(lambda x: json.loads(x[1]))

# For output just use saveAsTextFile() method
data.map(lambda x:json.dumps(x)).saveAsTextFile(”Output”)
```

# Comma and Tab Separated Values


Records are stored one per line. Loading CSV/TSV is similar to loading JSON data. In Python we will use the csv library

```python
import csv
import StringIO
defloadRecord(line):
input = StringIO.StringIO(line)
reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
return reader.next()
input = sc.textFile(inputFile).map(loadRecord)
```

# Loading CSV in full

If there are newlines embedded in fields, we need to load each file in full

```python
import csv
import StringIO
defloadRecords(fileNameContents):
input = StringIO.StringIO(fileNameContents[1])
reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
return reader
fullFileData= 
sc.wholeTextFiles(inputFile).flatMap(loadRecords)
```

# Saving CSV

```python
deftoCSVLine(data):

return ‘,’.join(str(d) for d in data)

lines = sc.parallelize([(0.0,0.08), (0.0,0.114)])

data = lines.map(toCSVLine)

data.saveAsTextFile(“outputCSV”)
```

# SequenceFiles

They are popular Hadoop format composed of flat files with key/value pairs.SequenceFileshave sync markers that allow Spark to seek to a point in the file and then resynchronize with the record  boundaries. 

This allows Spark to efficiently read  SequenceFilesin parallel from multiple nodes. 

```python
data = sc.sequenceFile(inFile, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.IntWritable") 
```

# Object Files

They are simple wrapper around SequenceFiles that allows to save our RDDs containing just values.

They require almost no work to save almost arbitrary objects.

Not available in Python but Python RDDs and SparkContextsupport methods called `saveAsPickleFile() andpickleFile()`


# Hadoop

Open source software framework for distributed storage and distributed processing of very large data sets on computer clusters.

The core consists of a storage part, known as Hadoop Distributed File System (HDFS), and a processing part called MapReduce. 

Hadoop splits files in large blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers packaged code for nodes to process in parallel based on the data that needs to be processed.This approach takes advantage of data locality.

# Hadoop

The base Apache Hadoop framework consists of the following modules:

- Hadoop Common –libraries and utilities needed by other Hadoop modules;
- Hadoop Distributed FileSystem(HDFS) –a distributed file system that stores data on commodity machines, providing very high aggregate bandwidth across the cluster;
- Hadoop YARN –resource management platform responsible for managing computing resources in clusters and using them for scheduling of users’ applications;
- Hadoop MapReduce–an implementation of the MapReduce programming model for large scale data processing.

# Hadoop Input and Output Formats

We can interact with any Hadoop-supported formats. Spark supports both the old and the new 

##Hadoop file APIs

We can read a file using the new Hadoop API. 

`newAPIHadoopFiletakes` a path, and three classes (format class representing our input format, class for our key, class for value). We can also pass a conf object to specify additional Hadoop configuration properties.

`hadoopFile()` exists for working with Hadoop input formats implemented in the older API.

Saving with Hadoop Output Formats `saveAsHadoopFile(fileName, Text.class, IntWritable.class, sequenceFileOutputFormat.class)` `hadoopDataset/saveAsHadoopDataSet` and `newAPIHadoopDataset/saveAsNewAPIHadoopDataset` to access Hadoop-supported storage formats that are not filesystems.


# File Compression


Spark’s native input formats (textFileand sequenceFile) can automatically handle some types of compression. When reading compressed data, there are some compression codecs that can be used to automatically guess the compression type. 

These compression options apply only to the Hadoop formats that support compression, namely those that are written out to a filesystem.

Each worker needs to be able to find the start of a new record  when we read data from multiple different machines. 

Some compression formats make this impossible, which requires a single node to read all of the data and this can easily lead to a bottleneck. 

Formats that can be easily read from multiple machines are called split-table.

# File Compression

While Spark’s `textFile()` method can handle compressed input, it automatically disables splittableeven if the input is compressed such that it could be read in a splittableway. 

If you find yourself needing to read in a large single-file compressed input, consider skipping Spark’s wrapper and instead use either `newAPIHadoopFileor` hadoopFileand specify the correct compression codec. 

# Examples –Word Count

```python
text_file= sc.textFile(“file") 

counts = text_file.flatMap(lambda line: line.split(" ")) \.map(lambda word: (word, 1)) \.reduceByKey(lambda a, b: a + b) 

counts.saveAsTextFile(”output”)
```

# Examples –PI Estimation

```python

defsample(p): x, y = random.random(), random.random() 

return 1 if x*x + y*y < 1 else 0 

count = sc.parallelize(xrange(0,NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b) 

print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES) 

```

# Examples –Standalone

- WordCount
- Sort
- PageRank
- Kmeans

(Use Spark 2.0)
https://github.com/apache/spark/tree/master/examples/src/main/python
Under common folder in our server

# File Systems

Spark supports a large number of file systems for reading and writing to.

Spark supports loading files from local filesystembut it requires that the files are available at the same path on all nodes in your cluster.

Some network filesystems(NFS, AFS, MapR’sNFS) are exposed to the user as a regular filesystem. 

If your data is already in one of these systems, then you can use it as an input by just 
specifying a file://path. Spark will handle it as long as the file system is mounted at the same path on each node.

# Local/Regular FS

```python
valrdd= sc.textFile("file:///home/holden/happypandas.gz") 
```

If your file is not on all the nodes in the cluster you can load locally on the driver without going through Spark and then call parallelizeto distribute the content to the workers. 

This approach is slow so it is better to load the file in a shared filesystemlike HDFS, NFS, S

# Amazon S

S3 is especially fast when your compute nodes are located inside of Amazon EC2, but can easily have much worse performance if you have to go over the public Internet.

You should first set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables to your S3 credentials. 

You can create these credentials from the Amazon Web Services console. 

As with all the other filesystems, Spark supports wildcard paths for S3, such as `s3n://bucket/my-files/*.txt.`

# HDFS

- Hadoop Distributed File System (HDFS)is a popular distributed filesystemwith which Spark works well. HDFS is  designed to work on commodity hardware and be resilient to node failure while providing high data throughput. Spark and HDFS can be collocated on the same machines, and Spark can take advantage of this data locality to avoid network overhead.
- Using Spark with HDFS is as simple as specifying hdfs://master:port/pathfor your input and output. 

# Structured Data with Spark SQL

- With structured data we mean data with a schema, consistent of a set of fields across data records
- We give Spark SQL a SQL query to run on the data source (selecting some fields or a function of the 
  fields), and we get back an RDD of Row objects, one per record. 

# Apache Hive

- Hive can store tables in a variety of formats, from plain text to column-oriented formats, inside HDFS or other storage systems.
- Spark SQL can load any table supported by Hive
- To c onnec t S pa rk S QL to ex isting H iv e insta lla tion, we need to prov ide a Hive configuration (copying hive-site.xmlfile to Spark’s ./conf/ directory)
- After that, you create a HiveContextobject, entry point to Spark SQL, and can write Hive Query Language (HQL) queries against your tables to get data back as RDDs of rows.

```python
from pyspark.sqlimport HiveContext

hiveCtx= HiveContext(sc)

rows = hiveCtx.sql("SELECT name, age FROM users") 

firstRow= rows.first()

print firstRow.name
```

# JSON

- Spark SQL can infer the schema of JSON data with a consistent schema across records.
- To l o ad J S O N d ata c reate a HiveContext(no need of hive-site.xml   file)
- Then use the HiveContext.jsonFilemethod to get an RDD of Row objects for the whole file.
- You can also register this RDD as a tabl e and select specific fieds from it

# JSON example

```javascript
{"user": {"name": "Holden", "location": "San Francisco"}, "text": "Nice day out today"} {"user": {"name": "Matei", "location": "Berkeley"}, "text": "Even nicer here :)"} 
```

```python
tweets = hiveCtx.jsonFile("tweets.json") 

tweets.registerTempTable("tweets") 

results = hiveCtx.sql("SELECT user.name, text FROM tweets") 
```

Run using pyspark

# JDBC

- Spark can access several databases using either their Hadoop connectors or custom Spark connectors.

- It can load data from any relational database that supports Java DataBaseConnectivity (JDBC)

- To access this data we construct an `org.apache.spark.rdd.JdbcRDDand` provide it with our SparkContextand the other parameters.

# Cassandra

- The Cassandra connector is not currently part of Spark, so you will need to add some further dependencies. Cassandra doesn’t use Spark SQL, but returns RDDs of CassandraRow objects, which have some of the same methods as Spark SQL’s Rowobject.

- Cassandra connector is currently available in Java and Scala

# HBase

- Spark can access HBasethrough its Hadoop input format implemented in the `org.apache.hadoop.hbase.mapreduce.TableInputFormat` class. This input format returns key/value pairs where the key is of type `org.apache.hadoop.hbase.io.ImmutableBytesWritable` and the value is of type `org.apache.hadoop.hbase.client.Result`. The Result class includes various methods for getting values.
- To use Spark with HBase, you can call `SparkContext.newAPIHadoopRDDwith` the correct input format

# Elasticsearch

- Spark can both read and write data from Elasticsearchusing Elastichsearch-Hadoop
- Elasticsearchis a new open source, Lucene-based search system
- Elasticsearchignores the path information we provide and depends on setting up configuration on our SparkContext.


  ​