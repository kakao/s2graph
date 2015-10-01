# Getting Started

S2Graph consists of a number of modules.

1. **S2Core** is the core library for common classes to store and retrieve data as edges and vertices.
2. **Root Project** is the Play-Framework-based REST API.
3. **Spark** contains spark-related common classes.
4. **Loader** has spark applications for data migration purposes. Load data to the HBase back-end as graph representations (using the S2Core library) by either consuming events from Kafka or copying straight from HDFS.
5. **Asynchbase** is a fork of https://github.com/OpenTSDB/asynchbase. We added a few functionalities to GetRequest which are yet to be merged to the original project. Here are some of the tweeks listed:
	- rpcTimeout
	- setFilter
	- column pagination
	- retryAttempCount
	- timestamp filtering


----------

There are some prerequisites for running S2Graph:

1. An [SBT](http://www.scala-sbt.org/) installation
  - `> brew install sbt` if you are on a Mac. (Otherwise, checkout the [SBT document](http://www.scala-sbt.org/0.13/tutorial/Manual-Installation.html).)
2. An [Apache HBase](http://hbase.apache.org/) installation
	- `> brew install HBase` if you are on a Mac. (Otherwise, checkout the [HBase document](http://hbase.apache.org/book.html#quickstart).)
  - Run `> start-hbase.sh`.
	- Please note that we currently support latest stable version of **Apache HBase 1.0.1 with Apache Hadoop version 2.7.0**. If you are using CDH, checkout **feature/cdh5.3.0**. @@@We are working on providing a profile on HBase/Hadoop version soon.
3. S2Graph currently supports MySQL for metadata storage.
  - `> brew install mysql` if you are on a Mac. (Otherwise, checkout the [MySQL document](https://dev.mysql.com/doc/refman/5.7/en/general-installation-issues.html).)
  - Run `> mysql.server start`.
  - Connect to MySQL server; `> mysql -uroot`.
  - Create database 'graph_dev'; `mysql> create database graph_dev;`.
  - Configure access rights; `mysql> grant all privileges on graph_dev.* to 'graph'​@localhost identified by 'graph'`.

----------

With the prerequisites are setup correctly, checkout the project:
```
> git clone https://github.com/kakao/s2graph.git
> cd s2graph
```
Create necessary MySQL tables by running the provided SQL file:
```
> mysql -ugraph -p < ./s2core/migrate/mysql/schema.sql
```
Now, go ahead and build the project:
```
> sbt compile
```
You are ready to run S2Graph!

```
> sbt run
```

We also provide a simple script under script/test.sh so that you can see if everything is setup correctly.

```
> sh script/test.sh
```

Finally, join the [mailing list](https://groups.google.com/forum/#!forum/s2graph)!

