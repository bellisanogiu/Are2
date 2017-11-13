
# Programming with RDDs

- RDD is simply a distributed collection of elements.
- In Spark all work is done either creating new RDDs, transforming existing RDDs, or calling operations on RDDs to compute a result.
- Spark, under the hood, distributes the data contained in RDD across the cluster and parallelizes operations we perform on them.


# RDD Basics

- Each RDD is split in multiple partitions which may be computed on different nodes of the cluster.

- Users create RDDs in two ways:
 - By loading an external dataset sc.textFile(“etc.”)
 - By distributing a collection of objects in their driver program

- Once created RDDs has 2 types of operations:
 - Transformations  (construct  a new RDD from a previous one)
 - Actions (compute a result based on a RDD)


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


# Passing Functions to Spark

In Python

```
python
word = rdd.filter(lambda s: "error" in s) 
def containsError(s): 
return "error" in s 
word = rdd.filter(containsError) 
```

## Common Transformations and Actions

- `map()` and `filter()` are the two most common.
- `map()` takes in a function and applies it to each element in the RDD with the result of the function being the new value of each element in the resulting RDD. Its return type does not have to be the same as its input type.
- `filter()` takes in a function and returns a RDD that only passes the filter


# Example map() e flatMap()

```python
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

# Actions
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

