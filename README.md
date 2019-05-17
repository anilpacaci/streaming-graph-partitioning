# Streaming Graph Partitioning

Experimental framework for performance analysis of graph partitioning algorithms on graph computations. This framework has been developed as a part of an experimental study focusing on streaming algorithms for graph partitioning. For more details, please refer to our SIGMOD'19 paper:

```
Anil Pacaci and M. Tamer Özsu. 2019. Experimental Analysis of Streaming Algorithms for Graph Partitioning. 
In 2019 International Conference on Management of Data (SIGMOD ’19), 
June 30-July 5, 2019, Amsterdam, Netherlands. 
ACM, New York, NY, USA, 18 pages. https://doi.org/10.1145/3299869.3300076
```

####  Organization

This testbed has two main components with different infrastructure based on the target workload: PowerLyra cluster for offline graph analytics and JanusGraph cluster for online graph queries.

[PowerLyra](https://github.com/realstolz/powerlyra) is a distributed graph computation engine that employs differentiated computation and partitioning on skewed graphs. We added an edge-cut based computation engine and set of partitioning algorithm used in the paper to provide fair comparison between the edge-cut and the vertex-cut partitioning models. Modified version of PowerLyra used in our SIGMOD'paper can be accessed [here](https://github.com/anilpacaci/powerlyra).

[JanusGraph](https://janusgraph.org/) is a distributed graph database optimized for storing and querying graphs across a multi-machine cluster. We modified the explicit partitioning [option](https://docs.janusgraph.org/latest/graph-partitioning.html) of JanusGraph to enable vertex to partition mapping to be provided by a third-party algorithm. We used Cassandra backend with range partitioning [ByteOrderedPartitioner](https://docs.datastax.com/en/archived/cassandra/2.1/cassandra/architecture/architecturePartitionerBOP_c.html). Modified version of the JanusGraph used in or SIGMOD paper can be accessed [here](https://github.com/anilpacaci/janusgraph).

All the components are dockerized and bash scripts to automate initializing, running experiments etc. are provided.

#### Contents

This repository contains:

- scripts:
- containers:


#### Prerequisites

It is recommended to use Linux 16.04 LTS.

Please refer to PowerLyra and JanusGraph documentation for manual setups.

For automated docker setup, cluster of machines with Docker 18.09.5 is required.

Please make sure that Docker can be used as a non-root user in all the machines in the cluster, for details: [Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user) 

In addition, automated scripts assume that passwordless ssh is setup between all machines in the cluster, for more details on how to setup passwordless ssh: [passwordless-ssh](http://www.linuxproblem.org/art_9.html).

#### Datasets

We use the following datasets in our experiments in the SIGMOD'19 paper:

|  Dataset  | Edges | Vertices | Max Degree |     Type     |
|:---------:|:-----:|:--------:|:----------:|:------------:|
|  Twitter  | 1.46B |    41M   |    2.9M    | Heavy Tailed |
| UK2007-05 | 3.73B |   105M   |    975K    |   Power-law  |
|  US-Road  | 58.3M |    23M   |      9     |  Low-degree  |
|  LDBC SNB SF1000 |  3.6M |   447M   |    3682    | Heavy Tailed |


Download Links:
  - Twitter: https://an.kaist.ac.kr/traces/WWW2010.html
  - UK2007-05: http://law.di.unimi.it/webdata/uk-2007-05/
  - USA-Road Network: http://users.diag.uniroma1.it/challenge9/download.shtml
  - LDBC SNB: https://github.com/ldbc/ldbc_snb_datagen

All datasets are in edge-list format by default. Edge-cut streaming graph partitioning algorithms requires adjacency list representation, where each line represents a vertex and all of its outgoing edges:

`vertex_id neighbour_1_id neighbour_2_id ... neighbour_n_id`

We provide Spark scripts to convert from edge list format to adjacency list format. See [csv-converter](https://github.com/anilpacaci/streaming-graph-partitioning/tree/master/csv-converter)

Note: We only use the friendship network (`person_knows_person` relationship) of the LDBC synthetic dataset. File with the friendship information can be find `social_network/person_knows_person_0_0.csv` in the generated dataset.

## Offline Graph Analytics


## Online Graph Queries
