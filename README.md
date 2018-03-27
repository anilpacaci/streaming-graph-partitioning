# graph-partitioning
### How to Build

In order to generate the executable:

```mvn clean assembly:assembly```

### sgp.properties

Supported partitioners are `hash`, `fennel` and `ldg`


### Run partitioner

After configuration of `sgp.properties`, run:

```mvn exec:java -Dexec.mainClass="ca.uwaterloo.cs.sgp.streaming.EdgeCutSGP" -Dexec.args="sgp.properties"```

It will output the total number of edges and edge-cuts in the end