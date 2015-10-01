# The Data Model


There are four important concepts that form the data model used throughout S2Graph; services, columns, labels and properties.

**Services**, the top level abstraction, act like databases in traditional RDBMS in which all data are contained. At Kakao, a service usually represents one of the company's actual services and is named accordingly, e.g. `"KakaoTalk"`, `"KakaoStory"`.

**Columns** are names for vertices and a service can have multiple columns. For example, a service `"KakaoMusic"` can have columns `"user_id"` and `"track_id"`.

**Labels**, represent relations between two columns. Think of it as a name for edges. The two columns can be the same (e.g. for a label representing friendships in an SNS, the two column will both be `"user_id"` of the service). Labels can connect columns from two different services. For example, one can create a label that stores all events where KakaoStory posts are shared to KakaoTalk.

**Properties**, are metadata linked to vertices or edges that can be queried upon later. For vertices representing KakaoTalk users, `date_of_birth` is a possible property, and for edges representing similar KakaoMusic songs their `similarity_distance` can be a property.

Using these abstractions, a unique vertex can be identified with its `(service, column, vertex id)`, and a unique edge can be identified with its `(service, label, source vertex id, target vertex id)`. Additional information on edges and vertices are stored within their properties.

