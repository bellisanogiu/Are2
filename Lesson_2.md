
# Programming with RDDs

- An RDD in Spark is simply an immutable distributed collection of objects
- Each RDD is split into multiple partitions, which may be computed on different nodes of the cluster
- In Spark all work is done either creating new RDDs, transforming existing RDDs, or calling operations on RDDs to compute a result.
- Spark, under the hood, distributes the data contained in RDD across the cluster and parallelizes operations we perform on them.


# RDD Basics

- Each RDD is split in multiple partitions which may be computed on different nodes of the cluster.

- Users create RDDs in two ways:
 - By loading an external dataset sc.textFile(“etc.”)
 - By distributing a collection of objects in their driver program (list or set)
 We have already seen loading a text file as an RDD of strings using SparkContext.textFile(): `lines = sc.textFile("README.md")`

- Once created RDDs has 2 types of operations:
 - Transformations  (construct  a new RDD from a previous one). For example, one common transformation is filtering data that matches a predicate. In our text file example, we can use this to create a new RDD holding just the strings that contain the word Python:
 ```
 pythonLines = lines.filter(lambda line: "Python" in line)
 ```
 - Actions (compute a result based on a RDD), and either return it to the driver program or save it to an external storage system (e.g., HDFS). One example of an action we called earlier is first(), which returns the first element in an RDD and is demonstrated in:

```
 pythonLines.first()
 ```


# Transformations

One common transformation is to filter data that matches a predicate

```python
lines = sc.textFile("README.md") # Create an RDD called lines
pythonLines= lines.filter(lambda line: "Python" in line) 
```

Note that filter does not change lines. It returns a pointer to a new RDD

In Examples the variable called lines is an RDD, created here from a text file on our local machine. We can run various parallel operations on the RDD, such as counting the number of elements in the dataset (here, lines of text in the file) or printing the first one.

```python
inputRDD = sc.textFile("log.txt")
errorsRDD = inputRDD.filter(lambda x: "error" in x) 
warningsRDD = inputRDD.filter(lambda x: "warning" in x) 
badLinesRDD = errorsRDD.union(warningsRDD) 
```

# Transformations

Spark keeps tracks of the set of dependencies between different RDDs, called the lineage graph. It uses this info to compute each RDD on demand and recover lost data if part of a persistent RDD is lost


# Actions

Compute a result based on an RDD, and either return it to the driver program or save it to external storage. E.g. first() method called.


```python
pythonLines.first()
print "Input had " + badLinesRDD.count() + " concerning lines" 
print "Here are 10 examples:"
for line in badLinesRDD.take(10): 
print line
```
RDDs have a collect method to retrieve  the entire RDD. To use collect the entire dataset must fit in memory on a single machine. It cant be used on large dataset. When they are too big, they cannot be collected but are written on a distributed storage system such as HDFS. RDD can be saved with methods such as saveAsTextFile() or saveAsSequenceFile()


# Lazy Evaluation

Spark computes RDDs in a lazy fashion, that is the first time they are used in an action. This approach makes sense when working with Big Data. In the example `lines = sc.textFile(...)`, Spark does not load and store all the lines. With the first action (filter) it prunes a lot of information. Calling the method `first()`, Spark scans the file until it finds the first matching line; it does not read the whole file.


# Persist

RDDs are recomputed each time there is an action on them. If we want to reuse RDD in multiple actions, we can use `RDD.persist()`. After computing the first time, Spark will store the RDD contents in memory (partitioned across the cluster) and reuse them in future actions. `unpersist()` is to remove them from cache Persisting  RDDs on disk instead of memory is also possible. We will use `persist()` to load a subset of our data into memory and query it repeatedly. E.g.

```python
pythonLines.persist()
pythonLines.count()
pythonLines.first()
```

# Class exercise

Load tagged-train.tsv in two RDDs. One persists and see time of `count()`. The other does not persist and see time of `count()`. What happens?


# Creating RDDs

- Loading an external dataset
  `lines = sc.textFile(“README.md”)`

- Parallelizing a collection in your driver program
  `lines = sc.parallelize(["pandas", "i like pandas"])`

The simplest way to create RDDs is to take an existing collection in your program and pass it to SparkContext’s parallelize() method, as shown in previous examples. This approach is very useful when you are learning Spark, since you can quickly create your own RDDs in the shell and perform operations on them. Keep in mind, however, that outside of prototyping and testing, this is not widely used since
it requires that you have your entire dataset in memory on one machine.
Example:
```
lines = sc.parallelize(["pandas", "i like pandas"])
```

# Passing Functions to Spark

In Python

```
python
word = rdd.filter(lambda s: "error" in s) 
def containsError(s): 
return "error" in s 
word = rdd.filter(containsError) 
```

# RDDs operations. 
Common Transformations and Actions. As we’ve discussed, RDDs support two types of operations: transformations and actions. 

## Transformations are operations on RDDs that return a new RDD, such as:
- `map()` and `filter()` are the two most common.
- `map()` takes in a function and applies it to each element in the RDD with the result of the function being the new value of each element in the resulting RDD. Its return type does not have to be the same as its input type.
- `filter()` takes in a function and returns a RDD that only passes the filter

- Transformations are operations on RDDs that return a new RDD.

- Many transformations are element-wise; that is, they work on one element at a time; but this is not true for all transformations.
```
#filter transformation
inputRDD = sc.textFile("log.txt")
errorsRDD = inputRDD.filter(lambda x: "error" in x)
```
Note that the filter() operation does not mutate the existing inputRDD. Instead, itcreturns a pointer to an entirely new RDD. inputRDD can still be reused later in the program—for instance, to search for other words. In fact, let’s use inputRDD again to search for lines with the word warning in them. Then, we’ll use another transformation, union(), to print out the number of lines that contained either error or warning.
```
#union() transformation in Python
errorsRDD = inputRDD.filter(lambda x: "error" in x)
warningsRDD = inputRDD.filter(lambda x: "warning" in x)
badLinesRDD = errorsRDD.union(warningsRDD)
```

- **As an example**, suppose that we have a logfile, log.txt, with a number of messages, and we want to select only the error messages. We can use the filter() transformation seen before.

# Example map() e flatMap()

```
python
nums= sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect() 
for numin squared: 
print "%i" % (num)

lines = sc.parallelize(["hello world", "hi"]) 
words = lines.flatMap(lambda line: line.split(" ")) 
words.first() # returns "hello" 
```

**Run the second example with map and flatMap and understand the differences**

# Pseudo set operations

RDDs supports union, intersection  even when RDDs are not properly set. Union will contain duplicates if any. Intersection  removes  duplicates. Performances  of intersections  are worst.


# Pseudo set operations

We can compute  a Cartesian product between  two RDDs. It is very expensive on large RDDs.

## Actions
`reduce()` is the most common, which takes a function that operates on two elements of the type in RDD and returns a new element of the same type.
`sum = rdd.reduce(lambda x, y: x+y)`
`fold()` is similar to reduce() but takes a zero value. They both require that the return type of the result must be the same type of the elements of the RDD we are working on.


# Problem

Compute  average of a list of numbers  [1,2,3,4]
`nums = sc.parallelize([1, 2, 3, 4])`
We need first to use `map()` to transform every element into a list (element and number 1) and use `reduce()` on pairs.


# Solution

```python
nums = sc.parallelize([1, 2, 3, 4,5])
b=nums.map(lambda x: (x,1))
b.reduce(lambda x,y: (x[0]+y[0],x[1]+1))
```
The `aggregate()` function  is another solution
```python
sumCount = nums.aggregate((0, 0), (lambda acc, value: (acc[0] + value, acc[1] + 1)), (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))) ;
return sumCount[0] / float(sumCount[1]) ;
```

