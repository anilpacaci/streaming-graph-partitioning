import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// val adjacencyText = sc.textFile("file:///home/apacaci/Projects/graph-partitioning/codebase/metis-partitioning/sparksee-validation-adjacency.txt")
// 

object MetisFormatter {

    // parse a single line, ignores the first part and creates and edge list
    def parseEdges(l : String) : Array[String] = {
        val edges = l.split(" ")
        val neighbours = edges.tail.map{ edge =>
            val fields = edge.split(":")
            fields(0) + ":" + fields(1)
        }
        
        return neighbours
    }

    def main(args: Array[String]) {
        //val conf = new SparkConf().setAppName("METIS-Formatter");
        //val sc = new SparkContext(conf)

        // read input file
        val adjacencyText = sc.textFile("file:///u4/apacaci/Projects/graph-partitioning/codebase/metis-partitioning/sparksee-validation/adjacency.txt")
        // trim unnecessary information
        val adjacency = adjacencyText.map( l => (l.split(" ")(0), MetisFormatter.parseEdges(l) ) )

        // METIS needs undirected graph, therefore we will add reverse of the each original edge
        val undirectedEdges = adjacency.flatMap(vertex => {
            val sourceid = vertex._1
            val neighbours = vertex._2
            neighbours.flatMap( n => Array( (sourceid, n), (n, sourceid)) )
        })
        val edgeCount = undirectedEdges.count
        val undirectedAdjacency = undirectedEdges.map( edge => (edge._1, Array(edge._2) ) ).reduceByKey( (l1, l2) => l1 ++ l2)

        // assign order to each vertex in adjacency list, which will create the actual lookup table for METIS partitioner
        // zipWithIndex indices start by 1 by default, so need post processing
        val lookup = undirectedAdjacency.map( t => t._1 ).zipWithIndex.map( t => (t._1, t._2 + 1))

        //generate oldid - newid lookup table on one pass
        lookup.repartition(1).saveAsTextFile("file:///u4/apacaci/Projects/graph-partitioning/codebase/metis-partitioning/sparksee-validation/lookup")

        // since lookup table is relatively small, we can broadcast it
        val lookupMap = sc.broadcast(lookup.collectAsMap).value
        val vertexCount = lookupMap.size

        // now we just need to iterate over graph and replace ids
        val undirectedAdjacencyWithIdentifiers = undirectedAdjacency.map( vertex => {
            val sourceid = lookupMap.get(vertex._1).get
            val neighbours = vertex._2.map( n => lookupMap.get(n).get)
            (sourceid, neighbours)
        })

        // now we can output in METIS compatible format, eliminatng source id since METIS implicitly assumes line number is a source id
        val metisAdjacency = undirectedAdjacencyWithIdentifiers.map( l => l._2.mkString(" "))
        val header = sc.parallelize(Seq( vertexCount + " " + (edgeCount / 2) ))
        header.union(metisAdjacency).repartition(1).saveAsTextFile("file:///u4/apacaci/Projects/graph-partitioning/codebase/metis-partitioning/sparksee-validation/metis")


    }
}