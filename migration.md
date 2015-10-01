# Migration


## 7. Bulk Loading ##

In many cases, the first step to start using S2Graph in production is to migrate a large dataset into S2Graph. S2Graph provides a bulk loading script for importing the initial dataset.

To use bulk load, you need a running [Spark](https://spark.apache.org/) cluster and **TSV file** that follows the S2Graph bulk load format.

Note that if you don't need additional properties on vertices(i.e., you only need vertex id), you only need to publish the edges and not the vertices. Publishing edges will create vertices with empty properties by default.

#### Edge Format

|Timestamp | Operation | Log Type |  From | To | Label | Props |
|:------- | --- |:----: | --- | -----| --- | --- |
|1416236400000|insert|edge|56493|26071316|talk_friend_long_term_agg_by_account_id|{"timestamp":1416236400000,"score":0}|

#### Vertex Format

|Timestamp | Operation | Log Type |  ID | Service Name | Column Name | Props |
|:------- | --- |:----: | --- | -----| --- | --- |
|1416236400000|insert|vertex|56493|kakaotalk|account_id|`{"is_active":true, "country_iso": "kr"}`|

### Build ###
In order to build the loader, run following command.

`> sbt "project loader" "clean" "assembly"`

This will give you  **s2graph-loader-assembly-X.X.X-SNAPSHOT.jar** under loader/target/scala-2.xx/

### Source Data Storage Options

For bulk loading, source data can be either in HDFS or a Kafka queue.

#### 1. For source data in HDFS ###

 - Run subscriber.GraphSubscriber to upload the TSV file into S2Graph.
 - From the Spark UI, you can check if the number of stored edges are correct.

#### 2. For source data is in Kafka ####

This requires the data to be in the bulk loading format and streamed into a Kafka cluster.

 - Run subscriber.GraphSubscriberStreaming to stream-load into S2Graph from a kafka topic.
 - From the Spark UI, you can check if the number of stored edges are correct.

#### 3. online migration ####
The following explains how to run an online migration from RDBMS to S2Graph. assumes that client send same events that goes to primary storage(RDBMS) and S2Graph.

 - Set label as isAsync "true". This will redirect all operations to a Kafka queue.
 - Dump your RDBMS data to a TSV for bulk loading.
 - Load TSV file with subscriber.GraphSubscriber.
 - Set label as isAsync "false". This will stop queuing events into Kafka and make changes into S2Graph directly.
 - Run subscriber.GraphSubscriberStreaming to queued events. Because the data in S2Graph is idempotent, it is safe to replay queued message while bulk load is still in process.

