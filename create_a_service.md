# Create a Service

## 1. Creating a Service - `POST /graphs/createService`  ##
Service is the top level abstraction in S2Graph which could be considered as a database in MySQL.

### 1.1 Service Fields
In order to create a Service, the following fields should be specified in the request.

|Field Name |  Definition | Data Type |  Example | Note |
|:------- | --- |:----: | --- | :-----|
| **serviceName** | User defined namespace. | String | "talk_friendship"| Required. |
| cluster | Zookeeper quorum address for your cluster.| String | "abc.com:2181, abd.com:2181" | Optional. <br>By default, S2Graph looks for "hbase.zookeeper.quorum" in your application.conf. If "hbase.zookeeper.quorum" is undefined, this value is set as "localhost". |
| hTableName | HBase table name.|String| "test"| Optional. <br> Default is {serviceName}-{phase}. <br> Phase is usually one of dev, alpha, real, or sandbox. |
| hTableTTL | Global time to live setting for data. | Integer | 86000 | Optional. Default is NULL which means that the data lives forever.
| preSplitSize | Factor for the HBase table pre-split size. **Number of pre-splits = preSplitSize x number of region servers**.| Integer|1|Optional. <br> If you set preSplitSize to 2 and have N region servers in your HBase, S2Graph will pre-split your table into [2 x N] parts. Default is 1.|

### 1.2 Basic Service Operations
You can create a service using the following API:

```
curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{"serviceName": "s2graph", "cluster": "address for zookeeper", "hTableName": "hbase table name", "hTableTTL": 86000, "preSplitSize": 2}
'
```
> It is recommended that you stick to the default values if you're unsure of what some of the parameters do. Feel free to ask through  

You can also look up all labels that belongs to a service.

```
curl -XGET localhost:9000/graphs/getLabels/:serviceName
```


