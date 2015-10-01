# Querying

## 6. Querying
Once you have your graph data uploaded to S2Graph, you can traverse your graph using our query APIs.


### 6.1. Query Fields ###

A typical query contains the source vertex as a starting point, a list of labels to traverse, and optional filters or weights for unwanted results and sorting respectively. Query requests are structured as follows:

| Field Name |  Definition | Data Type | Example |  Remarks |
|:------- | --- |:----: | --- | :-----|
| srcVertices | Starting point(s) of the traverse. | JSON (array of vertex representations that includes "serviceName", "columnName", and "id" fields.) | `[{"serviceName": "kakao", "columnName": "account_id", "id":1}]` | Required. |
|**steps**| A list of labels to traverse. | JSON (array of a step representation) |  ```[[{"label": "graph_test", "direction": "out", "limit": 100, "scoring":{"time": 0, "weight": 1}}]] ``` |Explained below.|
|removeCycle| When traversing to next step, disregard vertices that are already visited.| Boolean | "true"/ "false" | "Already visited" vertices are decided by both label and vertex. If a two-step query on a same label "friends" contains any source vertices, removeCycle option will decide whether or not these vertices will be included in the response. Default is "true". |
|select| Edge data fields to include in the query result. | JSON (array of strings) | ["label", "to", "from"] | |
|groupBy | S2Graph will group results by this field. | JSON (array of strings) | ["to"] | |
|filterOut | Give a nested query in this field, and it will run concurrently. The result will be a act as a blacklist and filtered out from the result of the outer query.| JSON (query format) | ||


**step**: Each step tells S2Graph which labels to traverse in the graph and how to traverse them. Labels in the very first step should be directly connected to the source vertices, labels in the second step should be directly connected to the result vertices of the first step traversal, and so on. A step is specified with a list of **query params** which we will cover in detail below.

**step param**:

| Field Name |  Definition | Data Type |  Example | Remarks |
|:------- | --- |:----: | --- | :-----|
| weights | Weight factors for edge scores in a step. These weights will be multiplied to scores of the corresponding edges, thereby boosting certain labels when sorting the end result. | JSON (dictionary) | {"graph_test": 0.3, "graph_test2": 0.2} | Optional. |
| nextStepThreshold | Threshold of the steps' edge scores. Only the edges with scores above nextStepThreshold will continue to the next step | Double |  | Optional.|
| nextStepLimit | Number of resulting edges from the current step that will continue to the next step. | Double | | Optional.|
| sample | You can randomly sample N edges from the corresponding step result. | Integer |5 | Optional.|

**query param**:

| Field Name |  Definition | Data Type | Example |  Remarks |
|:------- | --- |:----: | --- | :-----|
| label | Name of label to traverse.| String | "graph_test" | Required. Must be an existing label. |
| direction | In/ out direction to traverse. | String | "out" | Optional. Default is "out". |
| limit | Number of edges to fetch. | Int | 10 | Optional. Default is 10. |
| offset | Starting position in the index. Used for paginating query results. | Int | 50 | Optional. Default is 0. |
| interval | Filters query results by range on indexed properties. | JSON (dictionary) | `{"from": {"time": 0, "weight": 1}, "to": {"time": 1, "weight": 15}}` | Optional. |
| duration | Specify a time window and filter results by timestamp. | JSON (dictionary) |  `{"from": 1407616431000, "to": 1417616431000}` |Optional. |
| scoring | Scoring factors for property values in query results. These weights will be multiplied to corresponding property values, so that the query results can be sorted by the weighted sum. | JSON (dictionary) |  `{"time": 1, "weight": 2}` |Optional. |
| where | Filtering condition. Think of it as the "WHERE" clause of a SQL query. <br> Currently, a selection of logical operators (**and/ or**) and relational operators ("equal(=)/ sets(in)/ range(between x and y)") are supported. <br> **Do not use any quotation marks for string type** | String | "((_from = 123 and _to = abcd) or gender = M) and is_hidden = false and weight between 1 and 10 or time in (1, 2, 3)". <br> Note that it only supports **long, string, and boolean types.**|Optional. |
|outputField|The usual "to" field of the result edges will be replace with a property field specified by this option.| String | "outputField": "service_user_id". This will output edge's "to" field into "service_user_id" property. | Optional. |
|exclude| For a label with exclude "true", resulting vertices of a traverse will act as a blacklist and be excluded from other labels' traverse results in the same step. | Boolean |  "true"| Optional. Default is "false". |
|include | A Label with include "true" will ignore any exclude options in the step and the traverse results of the label guaranteed be included in the step result. | Boolean | "true" | Optional. Default is "false" |
| duplicate | Whether or not to allow duplicate edges; duplicate edges meaning edges with identical (from, to, label, direction) combination. | String <br> One of "first", "sum", "countSum", or "raw". | "first" | Optional. "**first**" means that only first occurrence of edge survives. <br> "**sum**" means that you will sum up all scores of same edges but only one edge survive. <br>"**countSum**" means to counts the occurrences of same edges but only one edge survives. <br>"**raw**" means that you will allow duplicates edges. Default is "**first**". |
| rpcTimeout | Timeout for the request. | Integer | 100 | Optional. In milliseconds. Default is 100 and maximum is 1000. |
| maxAttempt | Max number of HBase read attempts. | Integer |1 | Optional. Default is 1, and maximum is 5. |
| _to | Specify desired target vertex. Step results will only show edges to given vertex. | String | some vertex id|  Optional. |
| threshold | Score threshold for query results. Edges with lesser scores will be dropped. | Double | 1.0 | Optional. Default is 0.0. |
| transform | "_to" values on resulting edges can be transformed to another value from their properties. | JSON (array of array) | optional, default [ ["_to"]] |


### 6.2. Query API ###

#### 6.2.1. Edge Queries ####
S2Graph provides a query DSL which has been reported to have a pretty steep learning curve.
One tip is to try to understand each features by projecting it to that of a RDBMS such MySQL. This doesn't work all the time, but there are many similarities between S2Graph and a conventional RDBMS.
For example, S2Graphs "getEdges" is used to fetch data and traverse multiple steps. This is very similar to the "SELECT" query in MySQL.
Another tip is to not be shy to ask! Send us an E-mail or open an issue with the problems that you're having with S2Graph.

##### 1. POST /graphs/getEdges #####
Select edges with query.

##### 2. POST /graphs/checkEdges #####
Return edges that connect a given vertex pair.

#### 6.2.2 getEdges Examples ####

##### 1. Duplicate Policy #####
Here is a very basic query to fetch all edges that start from source vertex "101".
```
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{

    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10,
	                "duplicate": "raw"
                }
            ]
        }
    ]
}
'
```

**Notice the "duplicate" field**. If a target label's consistency level is **weak** and multiple edges exist with the same (from, to, label, direction) id, then the query is expect to have a policy for handling edge duplicates. S2Graph provides four duplicate policies on edges.
>1. raw: Allow duplicates and return all edges.
>2. **first**: Return only the first edge if multiple edges exist. This is default.
>3. countSum: Return only one edge, and return how many duplicates exist.
>4. sum: Return only one edge, and return sum of the scores.


With duplicate "raw", there are actually three edges with the same (from, to, label, direction) id.
```
{
    "size": 3,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -29,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 6,
            "timestamp": 6,
            "score": 1,
            "props": {
                "_timestamp": 6,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -29,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 5,
            "timestamp": 5,
            "score": 1,
            "props": {
                "_timestamp": 5,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -29,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 1,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```

Duplicate "countSum" returns only one edge with the score sum of 3.
```
{
    "size": 1,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -135,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 3,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```

##### 2. Select Option Example #####
In case you want to control the fields shown in the result edges, use the "select" option.
```
{
    "select": ["from", "to", "label"],
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10,
	                "duplicate": "raw"
                }
            ]
        }
    ]
}
```

S2Graph will return only those fields in the result.

```
{
    "size": 3,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak"
        },
        {
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak"
        },
        {
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak"
        }
    ],
    "impressionId": 1972178414
}
```

Default value of the "select" option is an empty array which means that all edge fields are returned.

##### 3. groupBy Option Example #####

Result edges can be grouped by a given field.
```
{
  	"select": ["from", "to", "label", "direction", "timestamp", "score", "time", "weight", "is_hidden", "is_blocked"],
    "groupBy": ["from", "to", "label"],
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10,
	                "duplicate": "raw"
                }
            ]
        }
    ]

}
```

You can see the result edges are grouped by their "from", "to", and "label" fields.
```
{
    "size": 1,
    "results": [
        {
            "groupBy": {
                "from": 101,
                "to": "10",
                "label": "s2graph_label_test_weak"
            },
            "agg": [
                {
                    "from": 101,
                    "to": "10",
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "timestamp": 6,
                    "score": 1,
                    "props": {
                        "time": -30,
                        "weight": 0,
                        "is_hidden": false,
                        "is_blocked": false
                    }
                },
                {
                    "from": 101,
                    "to": "10",
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "timestamp": 5,
                    "score": 1,
                    "props": {
                        "time": -10,
                        "weight": 0,
                        "is_hidden": false,
                        "is_blocked": false
                    }
                },
                {
                    "from": 101,
                    "to": "10",
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "timestamp": 4,
                    "score": 1,
                    "props": {
                        "time": 0,
                        "weight": 0,
                        "is_hidden": false,
                        "is_blocked": false
                    }
                }
            ]
        }
    ],
    "impressionId": 1972178414
}
```
##### 4. filterOut option example #####
You can also run two queries concurrently, and filter the result of one query with the result of the other.

```
{
    "filterOut": {
        "srcVertices": [
            {
                "serviceName": "s2graph",
                "columnName": "user_id_test",
                "id": 100
            }
        ],
        "steps": [
            {
                "step": [
                    {
                        "label": "s2graph_label_test_weak",
                        "direction": "out",
                        "offset": 0,
                        "limit": 10,
                        "duplicate": "raw"
                    }
                ]
            }
        ]
    },
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10,
                    "duplicate": "raw"
                }
            ]
        }
    ]
}
```
S2Graph will run two concurrent queries, one in the main step, and another in the filter out clause.
Here is more practical example.

```
{
  "filterOut": {
    "srcVertices": [
      {
        "columnName": "uuid",
        "id": "Alec",
        "serviceName": "daumnews"
      }
    ],
    "steps": [
      {
        "step": [
          {
            "direction": "out",
            "label": "daumnews_user_view_news",
            "limit": 100,
            "offset": 0
          }
        ]
      }
    ]
  },
  "srcVertices": [
    {
      "columnName": "uuid",
      "id": "Alec",
      "serviceName": "daumnews"
    }
  ],
  "steps": [
    {
      "nextStepLimit": 10,
      "step": [
        {
          "direction": "out",
          "duplicate": "scoreSum",
          "label": "daumnews_user_view_news",
          "limit": 100,
          "offset": 0,
          "timeDecay": {
            "decayRate": 0.1,
            "initial": 1,
            "timeUnit": 86000000
          }
        }
      ]
    },
    {
      "nextStepLimit": 10,
      "step": [
        {
          "label": "daumnews_news_belongto_category",
          "limit": 1
        }
      ]
    },
    {
      "step": [
        {
          "direction": "in",
          "label": "daumnews_news_belongto_category",
          "limit": 10
        }
      ]
    }
  ]
}
```

The main query from the above will traverse a graph of users and news articles as follows:
1. Fetch the list of news articles that user Alec read.
2. Get the categories of the result edges of step one.
3. Fetch other articles that were published in same category.

Meanwhile, Alec does not want to get articles that he already read. This can be taken care of with the following query in the filterOut option:

Articles that Alec has already read.
```
{
    "size": 5,
    "degrees": [
        {
            "from": "Alec",
            "label": "daumnews_user_view_news",
            "direction": "out",
            "_degree": 6
        }
    ],
    "results": [
        {
            "cacheRemain": -19,
            "from": "Alec",
            "to": 20150803143507760,
            "label": "daumnews_user_view_news",
            "direction": "out",
            "_timestamp": 1438591888454,
            "timestamp": 1438591888454,
            "score": 0.9342237306639056,
            "props": {
                "_timestamp": 1438591888454
            }
        },
        {
            "cacheRemain": -19,
            "from": "Alec",
            "to": 20150803150406010,
            "label": "daumnews_user_view_news",
            "direction": "out",
            "_timestamp": 1438591143640,
            "timestamp": 1438591143640,
            "score": 0.9333716513280771,
            "props": {
                "_timestamp": 1438591143640
            }
        },
        {
            "cacheRemain": -19,
            "from": "Alec",
            "to": 20150803144908340,
            "label": "daumnews_user_view_news",
            "direction": "out",
            "_timestamp": 1438581933262,
            "timestamp": 1438581933262,
            "score": 0.922898833570944,
            "props": {
                "_timestamp": 1438581933262
            }
        },
        {
            "cacheRemain": -19,
            "from": "Alec",
            "to": 20150803124627492,
            "label": "daumnews_user_view_news",
            "direction": "out",
            "_timestamp": 1438581485765,
            "timestamp": 1438581485765,
            "score": 0.9223930035297659,
            "props": {
                "_timestamp": 1438581485765
            }
        },
        {
            "cacheRemain": -19,
            "from": "Alec",
            "to": 20150803113311090,
            "label": "daumnews_user_view_news",
            "direction": "out",
            "_timestamp": 1438580536376,
            "timestamp": 1438580536376,
            "score": 0.9213207756669546,
            "props": {
                "_timestamp": 1438580536376
            }
        }
    ],
    "impressionId": 354266627
}
```

Without "filterOut"
```
{
    "size": 2,
    "degrees": [
        {
            "from": 1028,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_degree": 2
        }
    ],
    "results": [
        {
            "cacheRemain": -33,
            "from": 1028,
            "to": 20150803105805092,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438590169146,
            "timestamp": 1438590169146,
            "score": 0.9342777143725886,
            "props": {
                "updateTime": 20150803172249144,
                "_timestamp": 1438590169146
            }
        },
        {
            "cacheRemain": -33,
            "from": 1028,
            "to": 20150803143507760,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438581548486,
            "timestamp": 1438581548486,
            "score": 0.9342777143725886,
            "props": {
                "updateTime": 20150803145908490,
                "_timestamp": 1438581548486
            }
        }
    ],
    "impressionId": -14034523
}
```


with "filterOut"
```
{
    "size": 1,
    "degrees": [],
    "results": [
        {
            "cacheRemain": 85957406,
            "from": 1028,
            "to": 20150803105805092,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438590169146,
            "timestamp": 1438590169146,
            "score": 0.9343106784173475,
            "props": {
                "updateTime": 20150803172249144,
                "_timestamp": 1438590169146
            }
        }
    ],
    "impressionId": -14034523
}
```

Note that article **20150803143507760** has been filtered out.


##### 5. nextStepLimit Example #####
S2Graph provides step-level aggregation so that users can take the top K items from the aggregated results.

##### 6. nextStepThreshold Example #####

##### 7. sample Example #####
```
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      {"sample":2,"step": [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 10, "scoring": {"time": 1, "weight": 1}}]}
    ]
}
```

##### 8. transform Example #####
With typical two-step query, S2Graph will start the second step from the "_to" (vertex id) values of the first steps' result.
With the "transform" option, you can actually use any single field from the result edges' properties of step one.

Add a "transform" option to the query from example 1.

```
{
    "select": [],
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10,
                    "duplicate": "raw",
                    "transform": [
                        ["_to"],
                        ["time.$", "time"]
                    ]
                }
            ]
        }
    ]
}
```

Note that we have six resulting edges.
We have two transform rules, the first one simply fetches edges with their target vertex IDs (such as "to": "10"), and the second rule will fetch the same edges but with the "time" values replacing vertex IDs (such as "to": "to": "time.-30").
```
{
    "size": 6,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        },
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 6,
            "timestamp": 6,
            "score": 1,
            "props": {
                "_timestamp": 6,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "time.-30",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 6,
            "timestamp": 6,
            "score": 1,
            "props": {
                "_timestamp": 6,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 5,
            "timestamp": 5,
            "score": 1,
            "props": {
                "_timestamp": 5,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "time.-10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 5,
            "timestamp": 5,
            "score": 1,
            "props": {
                "_timestamp": 5,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 1,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "time.0",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 1,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```

##### 9. Two-Step Traversal Example #####

The following query will fetch a user's (id 1) friends of friends by chaining multiple steps:

```javascript
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
	  {
		  "step": [
			{"label": "friends", "direction": "out", "limit": 100}
		  ]
	  },
	  {
		  "step": [
			{"label": "friends", "direction": "out", "limit": 10}
		  ]
	  }
    ]
}
'
```
##### 10. Three-Step Traversal Example #####

Add more steps for wider traversals. Be gentle on the limit options since the number of visited edges will increase exponentially and become very heavy on the system.


##### 11. More examples #####
Example 1. From label "graph_test", select the first 100 edges that start from vertex "account_id = 1", with default sorting.

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 100
      }]
    ]
}
'
```

Example 2. Now select between the 50th and 100th edges from the same query.

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50}]
    ]
}
'
```

Example 3. Now add a time range filter so that you will only get the edges that were inserted between 1416214118000 and 1416300000000.

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118000, "to": 1416300000000}]
    ]
}
'
```

Example 4. Now add scoring rule to sort the result by indexed properties "time" and "weight", with weights of 1.5 and 10, respectively.


```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118000, "to": 1416214218000}, "scoring": {"time": 1.5, "weight": 10}]
    ]
}
'
```

Example 5. Make a two-step query to fetch friends of friends of a user "account_id = 1". (Limit the first step by 10 friends and the second step by 100.)

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "friends", "direction": "out", "limit": 100}],
      [{"label": "friends", "direction": "out", "limit": 10}]
    ]
}
'
```

Example 6. Make a two-step query to fetch the music playlist of the friends of user "account_id = 1". Limit the first step by 10 friends and the second step by 100 tracks.)

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "talk_friend", "direction": "out", "limit": 100}],
      [{"label": "play_music", "direction": "out", "limit": 10}]
    ]
}
'
```

Example 7. Query the friends of user "account_id = 1" who played the track "track_id = 200".
```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "talk_friend", "direction": "out", "limit": 100}],
      [{"label": "play_music", "direction": "out", "_to": 200}]
    ]
}
'
```

Example 8. See if a list of edges exist in the graph.

```javascript
curl -XPOST localhost:9000/graphs/checkEdges -H 'Content-Type: Application/json' -d '
[
	{"label": "talk_friend", "direction": "out", "from": 1, "to": 100},
	{"label": "talk_friend", "direction": "out", "from": 1, "to": 101}
]
'
```


#### 6.2.3. Vertex Queries ####

##### 1. POST /graphs/getVertices #####
Selecting all vertices from column `account_id` of a service `s2graph`.

```javascript
curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
[
	{"serviceName": "s2graph", "columnName": "account_id", "ids": [1, 2, 3]},
	{"serviceName": "agit", "columnName": "user_id", "ids": [1, 2, 3]}
]
'
```

