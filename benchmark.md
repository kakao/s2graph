# Benchmark

### Test Data
1. A synthetic dense matrix(10 million row x 1000 column, total edge 10 billion) data set.
2. Number of HBase region servers: 20
3. Number of S2Graph instances: 1

#### 1. One-Step Query
```
{
    "srcVertices": [
        {
            "serviceName": "test",
            "columnName": "user_id",
            "id": %s
        }
    ],
    "steps": [
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ]
    ]
}
```
| Number of S2Graph Servers |  VUsers | Offset | 1st Step Limit | TPS | Latency |
|:------- | --- |:----: | --- | --- | --- | --- |
| 1 | 20 | 0 | 100 | 3110.3TPS | 6.25ms |
| 1 | 20 | 0 | 200 | 2,595.3TPS | 7.52 ms |
| 1 | 20 | 0 | 400 | 1,449.8TPS | 13.56ms |
| 1 | 20 | 0 | 800 | 789.4TPS | 25.14ms |

<img width="884" alt="screen shot 2015-09-03 at 11 50 59 am" src="https://cloud.githubusercontent.com/assets/1264825/9649651/ce3424f4-5232-11e5-8350-4ac0c5e5a523.png">

#### 2. Two-Step query
```
{
    "srcVertices": [
        {
            "serviceName": "test",
            "columnName": "user_id",
            "id": %s
        }
    ],
    "steps": [
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ],
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ]
    ]
}
```
| Number of S2Graph Servers |  VUsers | 1st Step Limit | 2nd Step Limit | TPS | Latency |
|:------- | --- |:----: | --- | --- | --- | --- |  
| 1 | 20 | 10 | 10 | 3,050.3TPS | 6.43ms  |
| 1 | 20 | 10 | 20 | 2,239.3TPS | 8.8 ms |
| 1 | 20 | 10 | 40 | 1,393.4TPS | 14.19ms |
| 1 | 20 | 10 | 60 | 1,052.2TPS | 18.83ms |
| 1 | 20 | 10 | 80 | 841.2TPS | 23.59ms |
| 1 | 20 | 10 | 100 | 700.3TPS | 28.34ms |
| 1 | 20 | 10 | 200 | 397.8TPS | 50.03ms |
| 1 | 20 | 10 | 400 | 256.9TPS | 77.62ms |
| 1 | 20 | 10 | 800 | 192TPS | 103.97ms |
| 1 | 20 | 20 | 10 | 1,820.8TPS | 10.83ms |
| 1 | 20 | 20 | 20 | 1,252.8TPS | 15.78ms |
| 1 | 20 | 40 | 10 | 1,022.8TPS | 19.36ms |
| 1 | 20 | 60 | 10 | 732.8TPS | 27.11ms |
| 1 | 20 | 80 | 10 | 570.1TPS | 34.87ms |
| 1 | 20 | 100 | 10 | 474.9TPS | 41.87ms |
| 1 | 20 | 100 | 10  | 288.5TPS | 68.34ms |
| 1 | 20 | 100 | 20 |  279.1TPS | 71.4ms |
| 1 | 20 | 100 | 40 | 156.6TPS | 127.43ms |
| 1 | 20 | 100 | 80 | 83.6TPS | 238.48ms |
| 1 | 20 | 200 | 10 | 236.8TPS | 84.16ms |
| 1 | 20 | 400 | 10 | 121.8TPS | 163.87ms |
| 1 | 20 | 800 | 10 | 60.9TPS | 327.23ms |
| 1 | 10 | 800 | 10 | 61.8TPS | 162.19 ms |
| 1 | 5 | 800 | 10 | 54.2TPS | 91.87ms |
| 1 | 20 | 800 | 1 | 181.4TPS | 110ms |

<img width="903" alt="screen shot 2015-09-03 at 11 51 06 am" src="https://cloud.githubusercontent.com/assets/1264825/9649652/ce34abf4-5232-11e5-94d3-51a231d508a4.png">
<img width="892" alt="screen shot 2015-09-03 at 11 51 13 am" src="https://cloud.githubusercontent.com/assets/1264825/9649653/ce35c3a4-5232-11e5-8cdb-4bf220dc2cae.png">

#### 3. Three-Step query
```
{
    "srcVertices": [
        {
            "serviceName": "test",
            "columnName": "user_id",
            "id": %s
        }
    ],
    "steps": [
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ],
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ],
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ]
    ]
}
```
| Number of S2Graph Servers |  VUsers | 1st Step Limit | 2nd Step Limit | 3rd Step Limit | TPS | Latency |
|:------- | --- |:----: | --- | --- | --- | --- | --- |  
| 1 | 20 | 10 | 10 | 10 | 325TPS | 61.14ms |
| 1 | 20 | 10 | 10 | 20 | 189TPS | 105.31ms |
| 1 | 20 | 10 | 10 | 40 | 95TPS | 209.7ms |
| 1 | 20 | 10 | 10 | 80 | 27TPS | 727.56ms |

<img width="883" alt="screen shot 2015-09-03 at 11 51 19 am" src="https://cloud.githubusercontent.com/assets/1264825/9649654/ce372136-5232-11e5-9073-d9fbc37e0e78.png">