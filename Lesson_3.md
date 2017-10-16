# Spark documentation API Docs

[http://spark.apache.org/docs/2.0.1/](http://spark.apache.org/docs/2.0.1/)
Tab API Docs


# Working with Key/Value pairs

- RDDs of key/value pairs are common data type required for many operations in Spark

- Partitioning lets the users control the layout of pair RDDs across nodes. Choosing the right partitioning for a distributed dataset is similar to choosing the right data structure for a local one

- Spark provides special operations on RDDs containing key/value pairs (e.g. `reduceByKey()` and `join()`)


# Creating a pair RDD

```python
lines = sc.parallelize(["ciao bello","aaa bbb"])
pairs = lines.map(lambda  x: (x.split(“ “)[0],x))
```
Pair RDDs contain tuples, so we need to pass functions that operate on tuples rather than on individual elements

```python
result = pairs.filter(lambda keyValue : len(keyValue[1]) < 10)
```

# Creating a pair RDD

Sometimes working with pairs can be bad if we want to access only the value part of our pair RDD. Spark provides the `mapValues(func)` function which is the same as `map{case (x,y): (x,func(y))}`


# Aggregations
- When datasets are expressed as key/value pairs, it is common to aggregate statistics across elements with the same key.
- We have already seen `fold()`, `aggregate()`, `reduce()` actions on basic RDDs. 
- Spark has similar operations that combine values that have the same key. These operations return RDDs and are transformations instead of actions
- `reduceByKey()` is similar to `reduce()`. It runs parallel reduce operations, one for each key in the dataset, where each operation combines values that have the same key. It returns a new RDD consisting of each key and the reduced value for that key
- `foldByKey()` is similar to `fold()`. 

```python
rdd = sc.parallelize([("panda",0),  ("pink",3), (“pirate”,3),  (“panda”,1),  (“pink”,4)])

rdd.mapValues(lambda x : (x , 1)).reduceByKey(lambda  x, y : ( x [0] + y [0], x[1] + y[1])) 
```

# Word count problem

```python
rdd = sc.textFile(“words.txt”)

#[try to execute with map instead of flatMap to see what happens]
words = rdd.flatMap(lambda  x: x.split(" ")) 

result = words.map(lambda  x: (x, 1)).reduceByKey(lambda  x, y: x + y)
```

# combineByKey

`combineByKey()` is the most general of the per-key aggregation functions. Most of the others are implemented using it. Like aggregate(), `combineByKey()` allows the user to return values not the same type of the input data.


# Example of combineByKey

```python
nums = sc.parallelize([(‘coffee’,1),(‘coffee’,2),(‘panda’,3),(‘coffee’,)])
sumCount = nums.combineByKey((lambda  x: (x,1)), (lambda  x, y: (x[0] + y, x[1] + 1)), (lambda  x, y: (x[0] + y[0], x[1] + y[1])))

r = sumCount.map(lambda  (key, xy): (key, xy[0]/xy[1])).collectAsMap()

#Provare a togliere collectAsMap()

print(r)
```

# Tuning the level of parallelism

For now we have talked about how all the transformations are distributed but we have not really looked at how Spark decides how to split the work. Each RDD has a fixed number of partitions that determine the degree of parallelism to use when executing operations on the RDD. When performing aggregations or grouping operations we can ask Spark to use a specific number of partitions. Spark will always try to infer a sensible default value based on the size of your cluster but we can tune the level of parallelism for better performance. Most of the operators discussed so far accept a second 
parameter.


# Tuning the level of parallelism

```python
data = [("a", 3), ("b", 4), ("a", 1)]

# Default parallelism
sc.parallelize(data).reduceByKey(lambda  x, y: x + y)  
#Custom parallelism
sc.parallelize(data).reduceByKey(lambda  x, y: x + y, 10) 
```

# Tuning the level of parallelism

`repartition()` function shuffles the data across the network to create a new set of partitions.
Repartitioning is expensive.
`coalesce()` is an optimized version of`repartition()` that allows avoiding data movement as long as you decrease the number dof RDD partitions.


# Grouping data

`groupByKey()` groups our data using key in our RDD. On an RDD consisting of keys of type K and 
values of type V, we get back an RDD of type `[K, Iterable[V]]`.
`rdd.reduceByKey(func)` produces the same result (but more efficient) as `rdd.groupByKey().mapValues(value=>value.reduce(func))`


# Grouping data

- We can group data sharing the same key from multiple RDDs using `cogroup()` 

-`cogroup()` over two RDDs sharing the same key type, K, with the respective value types V and W 
gives us back RDD[(K,(Iterable[V], Iterable[W]))]

- `cogroup()` gives us the power to group data from multiple RDDs.


# Example cogroup

```python
data1  = [("a", 3), ("b",  4), ("a",  1)]
data1  = sc.parallelize(data1)
data2 = [("a", 3), ("b",  4), ("a",  1)]
data2 = sc.parallelize(data2)
a = data1.cogroup(data2)
    for el in a.collect():
        for el1 in el[1][0]:
            print  el
        for el1 in el[1][1]:
            print  el
```

# Joins

Joining data is one of the most common operations on a pair RDD.

`join()`: this is the inner join. Only keys present in both pair RDDs are output.

`leftOuterJoin(other)` and `rightOuterJoin(other)` both join pair RDDs together by key, where one of the pair RDDs can be missing the key.


# Example Joins

```python
data1  = [("a", 3), ("b",  4), ("a",  1)]
data1  = sc.parallelize(data1)
data2  = [("a", 3), (“c",  4 )]
data2  = sc.parallelize(data2)
a = data1.join(data2)
a.collect()
a = data1.leftOuterJoin(data2)
a.collect()
a = data1.rightOuterJoin(data1)
a.collect()
```

# Sorting data

We can sort an RDD with key/value pairs provided that there is an ordering defined on the key. 
`rdd.sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x : str(x))`
This sorts integer as they were strings. Try to sort the following rdd
`>>>rdd=sc.parallelize([(1,"ciao"),(2,"bello"),(10,"ccc "),(100,"caa")])`

# Actions on pair RDDs

All the traditional actions available on the base RDD are also available here. Some additional actions are available to take advantage of the key/value nature of the data.

Actions  on pair RDDs  `({(1,2), (3,4), (3,6)})`


# Data Partitioning

Control datasets’ partitioning across nodes. Spark programs can choose to control their RDDs’ 
partitioning to reduce communication.
Partitioning is useful only when a dataset is reused multiple times in key-oriented operations such as joins Spark lets the program ensure that a set of keys will appear together on some node.
For example, you might choose to hash- partition an RDD 
into 100  partitions so that keys that have the same hash value modulo  100  appear on the same node. 

# Data Partitioning Example

Consider an application that keeps a large table of user information in memory. say, an RDD of `(UserID, UserInfo)` pairs, UserInfo contains a list of topics the user is subscribed to. The application periodically combines this table with a smaller file representing events that happened in the past five minutes, say, a table of `(UserID, LinkInfo)` pairs for users who have clicked a link on a website in those five minutes. 
For example, we may wish to count how many users visited a link that was not to one of their subscribed topics. We can perform this combination with Spark’s `join()` operation, which can be used to group the `User Info` and `LinkInfo` pairs for each `UserID` by key. 


# Data Partitioning Example

It will be inefficient because the join() operation does not know anything about how the keys are partitioned in the datasets. 

By default, this operation will hash all the keys of both datasets, sending elements with the same key hash across the network to the same machine, and then join together the elements with the same key on that machine. 

Because we expect the userData table to be much larger than the small log of events seen every five minutes, this wastes a lot of work: the userData table is hashed and shuffled across the network on every call, even though it doesn’t change. 


# Data Partitioning Example

To fix this just use `partitionBy()` on userData to hash-partition it at the start of the program.

Because we called `partitionBy()` when building userData, Spark will now know that it is hash-partitioned, and calls to `join()` on it will take advantage of this information. In particular, when we call `userData.join(events)`,  Spark will shuffle only the events RDD, sending events with each particular `UserID` to the machine that contains the corresponding hash partition of `userData`. 

The result is that a lot less data is communicated over the network, and the program runs significantly faster. 

# Data Partitioning Example

Note that `partitionBy()` is a transformation, so it  always returns a new RDD and does not change the original RDD. 

The 100 passed to `partitionBy()` represents the number of partitions, which will control how many parallel tasks perform further operations on the RDD (e.g., joins); in general, make this at least as large as the number of cores in your cluster. 


# Determining an RDD’s Partitioner

In Java and Scala we can determine how an RDD is partitioned using its partitioner property (`partitioner()` method in Java). This is a way to test in the Spark shell how different Spark 
operations affect partitioning, and to check that the operations you want to do will yield the right result.

```scala
scala> val pairs = sc.parallelize(List((1,  1), (2, 2), (3, 3)))
scala> pairs.partitioner
scala> import org.apache.spark. HashPartitioner
scala> val partitioned  = pairs.partitionBy(new 
HashPartitioner(2))
scala> partitioned.partitioner
```

## Operations that benefit from partitioning

`cogroup()`, `groupWith()`,  `join()`, `leftOuterJoin()`, `rightOuter Join()`, `groupByKey()`, `reduceByKey()`, `combineByKey()`,  and  `lookup()`. 

For operations that act on a single RDD, such as `reduceByKey()`, running on a pre-partitioned RDD will cause all the values for each key to be computed locallyon a single machine, requiring only the final, locally reduced value to be sent from each worker node back to the master. 

For binary operations, such as `cogroup()` and `join()`, pre-partitioning will cause at least one of the RDDs (the one with the known partitioner) to not be shuffled.

If both RDDs have the same partitioner, and if they are cached on the same machines, or if one of them has not yet been computed, then no shuffling across the network occur.


# Operations that affect partitioning

Because the elements  with the same key have been hashed to the same machine, Spark knows that the result is hash-partitioned, and operations like `reduceByKey()` on the join result are going to be significantly faster. 

Flipside is if you call `map()` on a hash-partitioned RDD of key/value pairs, the function passed to `map()` can in theory change the key of each element, so the result will not have a partitioner. 

Here are all the operations that result in a partitioner being set on the output RDD: `cogroup()`, `groupWith()`, `join()`, `leftOuterJoin()`, `rightOuterJoin()`, `groupByKey()`, `reduceByKey()`, `combineByKey()`, `partitionBy()`, `sort()`, `mapValues()` (if the parent RDD has a partitioner), `flatMapValues()` (if parent has a partitioner), `andfilter()` (if parent has a partitioner). All other operations will pro- duce a result with no partitioner. 

For binary operations, whichpartitioner is set on the output depends on the parent RDDs’ partitioners. By default, it is a hash partitioner, with the number of partitions set to the level of parallelism of the operation. However, if one of the parents has a partitioner set, it will be that partitioner; and if both parents have a partitionerset, it  will be the partitioner of the first parent. 


# Full Example of word count

In this version of WordCount, the goal is to learn the distribution of letters in the most popular words in a corpus. The application:

- Creates a SparkConf and SparkContext. A Spark application corresponds to an instance of the SparkContext class. When running a shell, the SparkContextis created for you.
- Gets a word frequency threshold.
- Reads an input set of text documents.
- Counts the number of times each word appears.
- Filters out all words that appear fewer times than the threshold.
- For the remaining words, counts the number of times each letter occurs.
- In MapReduce, this requires two MapReduce applications, as well as persisting the intermediate data to HDFS between them. In Spark, this application requires about 90 percent fewer lines of code than one developed using the MapReduce API.

```python
import sys from pyspark
import SparkContext, SparkConf

if __name__ == "__main__": 
   # create Spark context with Spark configuration 
   conf= SparkConf().setAppName("Spark  Count") 
   sc= SparkContext(conf=conf) 

   # get threshold a
   threshold = int(sys.argv[2]) 
   
   # read in text file and split each document into words 
   tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" ")) 
   
   # count the occurrences of each word 
   wordCounts = tokenized.map(lambda word: (word,  1)).reduceByKey(lambda v1,v2:v1 +v2) 

   # filter out  words with  fewer than threshold occurrences 
   filtered = wordCounts.filter(lambda pair:pair[1] >= threshold) 

   # count characters 
   charCounts= filtered.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2) 
   list = charCounts.collect() print repr(list)[1:-1]
```