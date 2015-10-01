# Manage Vertices


## 5. Managing Vertices (Optional)

Vertices are the two end points of an edge, and logically stored in columns of a service. If your use case requires storing metadata corresponding to vertices rather than edges, there are operations available on vertices as well.

### 5.1 Vertex Fields
Unlike edges and their labels, properties of a vertex are not indexed nor require a predefined schema. The following fields are used when operating on vertices.

| Field Name |  Definition | Data Type | Example |  Remarks |
|:------- | --- |:----: | --- | :-----|
| timestamp |  | Long |  1417616431 | Required. Unix Epoch time in **milliseconds**. |
| operation | One of insert, delete, update, increment | String | "i", "insert" | Required only for bulk operations. Alias are also available: i (insert), d (delete), u (update), in (increment). Default is insert.|
| **serviceName** | Corresponding service name | String | "kakaotalk"/"kakaogroup"| Required. |
| **columnName** | Corresponding column name |  String  |"user_id"| Required. |
| id     | Unique identifier of vertex |  Long/ String |101| Required. Use Long if possible.|
| **props** | Additional properties of vertex. | JSON (dictionary) | {"is_active_user": true, "age":10, "gender": "F", "country_iso": "kr"}| Required.  |



### 5.2 Basic Vertex Operations

#### 1. Insert - `POST /graphs/vertices/insert/:serviceName/:columnName` ####

```
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/account_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true, "talk_user_id":10},"timestamp":1417616431000},
  {"id":2,"props":{"is_active":true, "talk_user_id":12},"timestamp":1417616431000},
  {"id":3,"props":{"is_active":false, "talk_user_id":13},"timestamp":1417616431000},
  {"id":4,"props":{"is_active":true, "talk_user_id":14},"timestamp":1417616431000},
  {"id":5,"props":{"is_active":true, "talk_user_id":15},"timestamp":1417616431000}
]
'
```

#### 2. Delete - `POST /graphs/vertices/delete/:serviceName/:columnName` ####

This operation will delete only the vertex data of a specified column and will **not** delete any edges connected to those vertices.

**Important notes**
>**This means that edges returned by a query can contain deleted vertices. Clients are responsible for checking validity of the vertices.**

#### 3. Delete All - `POST /graphs/vertices/deleteAll/:serviceName/:columnName` ####

This operation will delete all vertices and connected edges in a given column.

```
curl -XPOST localhost:9000/graphs/vertices/deleteAll/s2graph/account_id -H 'Content-Type: Application/json' -d '
[{"id": 1, "timestamp": 193829198000}]
'
```

This is a **very expensive** operation. If you're interested in what goes on under the hood, please refer to the following pseudocode:

```
vertices = vertex list to delete
for vertex in vertices
	labels = fetch all labels that this vertex is included.
	for label in labels
		for index in label.indices
			edges = G.read with limit 50K
			for edge in edges
				edge.delete
```

The total complexity is O(L * L.I) reads + O(L * L.I * 50K) writes, worst case. **If the vertex you're trying to delete has more than 50K edges, the deletion will not be consistent.**



#### 4. Update - `POST /graphs/vertices/update/:serviceName/:columnName` ####

Same parameters as the insert operation.

#### 5. Increment ####

Not yet implemented; stay tuned.

