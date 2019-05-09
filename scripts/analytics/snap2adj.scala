import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

val inputFile = "/home/apacaci/datasets/uk2007-05/uk2007-05-snap-combined.txt"
val outputFile = "/home/apacaci/datasets/uk2007-05/uk2007-05-adjacency"

val snapText = sc.textFile(inputFile)

// for directed graph consider the direction of the edge
val edges = snapText.map(e => (e.split("\\s")(0).toLong, Set(e.split("\\s")(1).toLong) ) )
// for undirected graph, create edges in both direction
// val edges = snapText.map(e => Array((e.split("\\t")(0).toInt, Set(e.split("\\t")(1).toInt) ), (e.split("\\t")(1).toInt, Set(e.split("\\t")(0).toInt) ) )

//now reduce edges of same source vertex
val adjacency = edges.reduceByKey((l1, l2) => l1 ++ l2)

// create string for each line
val adjacencyFile = adjacency.map(v => v._1 + " " + v._2.size + " " + v._2.mkString(" "))

adjacencyFile.saveAsTextFile(outputFile)
