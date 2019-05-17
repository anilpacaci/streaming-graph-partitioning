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

This repository contains necess


#### Prerequisites

It is recommended to use Linux 16.04 LTS.

Please refer to PowerLyra and JanusGraph documentation for manual setups.

For automated docker setup, cluster of machines with Docker 18.09.5 is required.

Please make sure that Docker can be used as a non-root user in all the machines in the cluster, for details: [Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user) 

In addition, automated scripts assume that passwordless ssh is setup between all machines in the cluster, for more details on how to setup passwordless ssh: [passwordless-ssh](http://www.linuxproblem.org/art_9.html).

## Offline Graph Analytics


## Online Graph Queries
