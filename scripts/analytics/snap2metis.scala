//val inputFile = "/home/apacaci/datasets/soc-pokec/soc-pokec-snap"
//val lookupFile = "/home/apacaci/datasets/soc-pokec/soc-pokec-metis-lookup"
//val metisHeader = "/home/apacaci/datasets/soc-pokec/soc-pokec-metis-header"
//val metisAdjacencyFile = "/home/apacaci/datasets/soc-pokec/soc-pokec-metis-adjacency"

//val inputFile = "/home/apacaci/datasets/USA-road/USA-road-snap/part-00000"
//val lookupFile = "/home/apacaci/datasets/USA-road/USA-road-lookup"
//val metisHeader = "/home/apacaci/datasets/USA-road/USA-road-metis-header"
//val metisAdjacencyFile = "/home/apacaci/datasets/USA-road/USA-road-metis-adjacency"

val inputFile = "/home/apacaci/datasets/uk2007-05/uk2007-05-snap"
val inputFileNoLoop = "/home/apacaci/datasets/uk2007-05/uk2007-05-snap-noloop"
val lookupFile = "/home/apacaci/datasets/uk2007-05/uk2007-05-metis-lookup"
val metisHeader = "/home/apacaci/datasets/uk2007-05/uk2007-05-metis-header"
val metisAdjacencyFile = "/home/apacaci/datasets/uk2007-05/uk2007-05-metis-adjacency"
val metisOriginalAdjacencyFile = "/home/apacaci/datasets/uk2007-05/uk2007-05-metis-original-adjacency"

// val inputFile = "/home/apacaci/datasets/wrn/wrn-snap.txt"
// val lookupFile = "/home/apacaci/datasets/wrn/wrn-metis-lookup"
// val metisHeader = "/home/apacaci/datasets/wrn/wrn-metis-header"
// val metisAdjacencyFile = "/home/apacaci/datasets/wrn/wrn-metis-adjacency"
//
//val inputFile = "/home/apacaci/datasets/twitter_rv/twitter_rv_snap/twitter_rv.net"
//val lookupFile = "/home/apacaci/datasets/twitter_rv/twitter_rv_metis_lookup"
//val metisHeader = "/home/apacaci/datasets/twitter_rv/twitter_rv_metis_header"
//val metisAdjacencyFile = "/home/apacaci/datasets/twitter_rv/twitter_rv_metis_adjacency"

val lookup = sc.objectFile[(Long, Long)](lookupFile)

// val lookup = adjacency.map( t => t._1 ).zipWithIndex.map( t => (t._1, t._2 + 1))
//lookup.saveAsObjectFile(lookupFile)

// since lookup table is relatively small, we can broadcast it
val lookupMap = sc.broadcast(lookup.collectAsMap)
//val lookupMap = lookup.collectAsMap
val vertexCount = lookupMap.value.size


val snapText = sc.textFile(inputFile).coalesce(38400)

val snapNoLoop = snapText.map( s => s.split("\\s").map(_.toLong)).filter(s => s(0) != s(1)).map(s => ( s(0), s(1) ) )
snapNoLoop.map(s => s._1 + " " + s._2).saveAsTextFile(inputFileNoLoop)
// for undirected graph, create edges in both direction, metis uses undirected graphs
val edges = snapNoLoop.flatMap(s => Array( (s._1, Set(s._2)), (s._2, Set(s._1)) )).distinct

val edgeCount = edges.count

val adjacency = edges.reduceByKey((l1, l2) => l1 ++ l2)
adjacency.cache

val lookup = adjacency.map( t => t._1 ).zipWithIndex.map( t => (t._1, t._2 + 1))
lookup.saveAsObjectFile(lookupFile)

val lookupMap = sc.broadcast(lookup.collectAsMap)
//val lookupMap = lookup.collectAsMap
val vertexCount = lookupMap.value.size

adjacency.map( l => l._1 + " " + l._2.size + " " + l._2.mkString(" ")).saveAsTextFile(metisOriginalAdjacencyFile)

// now we just need to iterate over graph and replace ids
val adjacencyWithIdentifiers = adjacency.map( vertex => {
  val sourceid = lookupMap.value.get(vertex._1).get
  val neighbours = vertex._2.map( n => lookupMap.value.get(n).get).toList.sortWith(_ < _)
  (sourceid, neighbours)
})


// now we can output in METIS compatible format, eliminatng source id since METIS implicitly assumes line number is a source id
val metisAdjacency = adjacencyWithIdentifiers.sortByKey().map( l => l._2.size + " " + l._2.mkString(" "))
val header = sc.parallelize(Seq( vertexCount + " " + (edgeCount / 2) ))

header.saveAsTextFile(metisHeader)
metisAdjacency.saveAsTextFile(metisAdjacencyFile)




//val lookupFile = "/home/apacaci/datasets/USA-road/USA-road-lookup"
//val partitionPath = "/home/apacaci/datasets/USA-road/USA-road-metis-adjacency-combined.txt.part.128"
//val partitionLookup = "/home/apacaci/datasets/USA-road/USA-road-metis-partition128"

val lookupFile = "/home/apacaci/datasets/USA-road/USA-road-lookup"

// read both files and reverse the key-value order so that METIS ids are join key
val lookup = sc.objectFile[(Long, Long)](lookupFile).map(t => (t._2, t._1))

val partitionPath = "/home/apacaci/datasets/USA-road/USA-road-metis-adjacency-combined.txt.part.8"
val partitionLookup = "/home/apacaci/datasets/USA-road/USA-road-metis-partition8 "

val orderMap = sc.textFile(partitionPath).zipWithIndex.map(t => ((t._2 + 1).toLong, t._1.toLong))

val partitionMap = lookup.join(orderMap).map(t => (t._2._1, t._2._2))

partitionMap.map(t => String.valueOf(t._1) + " " + t._2).repartition(1).saveAsTextFile(partitionLookup)
