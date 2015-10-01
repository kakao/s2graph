# Create a ServiceColumn


## 3. Creating a Service Column (Optional) - `POST /graphs/createServiceColumn`
----------
If your use case requires props assigned to vertices instead of edges, what you need is a Service Column.

>**Remark: If it is only the vertex id that you need and not additional props, there's no need to create a Service Column explicitly. At label creation, by default, S2Graph creates column space with empty properties according to the label schema.**

### 3.1 Service Column Fields

| Field Name |  Definition | Data Type |  Example | Remarks |
|:------- | --- |:----: | --- | :-----|
| serviceName | Which service the Service Column belongs to. | String | "kakaotalk" | Required. |
| columnName | Service Column`s name. | String |  "talk_user_id" | Required. |
| props | Optional properties of Service Column. | JSON (array dictionaries) | Please refer to the examples. | Optional.|

### 3.2 Basic Service Column Operations
Here are some sample requests for Service Column creation as well as vertex insertion and selection.
```
curl -XPOST localhost:9000/graphs/createServiceColumn -H 'Content-Type: Application/json' -d '
{
    "serviceName": "s2graph",
    "columnName": "user_id",
    "columnType": "long",
    "props": [
        {"name": "is_active", "dataType": "boolean", "defaultValue": true},
        {"name": "phone_number", "dataType": "string", "defaultValue": "-"},
        {"name": "nickname", "dataType": "string", "defaultValue": ".."},
        {"name": "activity_score", "dataType": "float", "defaultValue": 0.0},
        {"name": "age", "dataType": "integer", "defaultValue": 0}
    ]
}
'
```

General information on a vertex schema can be retrieved with `/graphs/getServiceColumn/{service name}/{column name}`.

```
curl -XGET localhost:9000/graphs/getServiceColumn/s2graph/user_id
```
This will give all properties on serviceName `s2graph` and columnName `user_id` serviceColumn.

Properties can be added to a Service Column with `/graphs/addServiceColumnProps/{service name}/{column name}`.

```
curl -XPOST localhost:9000/graphs/addServiceColumnProps/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
	{"name": "home_address", "defaultValue": "korea", "dataType": "string"}
]
'
```

Vertices can be inserted to a Service Column using `/graphs/vertices/insert/{service name}/{column name}`.
```
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true}, "timestamp":1417616431},
  {"id":2,"props":{},"timestamp":1417616431}
]
'
```

Finally, query your vertex via `/graphs/getVertices`.
```
curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
[
	{"serviceName": "s2graph", "columnName": "user_id", "ids": [1, 2, 3]}
]
'
```