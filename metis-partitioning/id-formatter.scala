import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// val adjacencyText = sc.textFile("file:///home/apacaci/Projects/graph-partitioning/codebase/metis-partitioning/sparksee-validation-adjacency.txt")
// 

object MetisIDFormatter {

    // parse a single line, ignores the first part and creates and edge list
    def parseSNBEdges(l : String) : Array[String] = {
        val edges = l.split("\\|", 3)
        val outgoing = edges(1).split(" ").filter(!_.isEmpty).map{ edge => edge.split(",")(1) }
        
        val incoming = edges(2).split(" ").filter(!_.isEmpty).map{ edge => edge.split(",")(1) }

        return outgoing ++ incoming
    }

    // parse a single line, ignores the first part and creates and edge list
    def parseTwitterEdges(l : String) : Array[String] = {
        val edges = l.split(" ")
        val outgoing = edges(1).split(" ").filter(!_.isEmpty).map{ edge => edge.split(",")(1) }
        
        val incoming = edges(2).split(" ").filter(!_.isEmpty).map{ edge => edge.split(",")(1) }

        return outgoing ++ incoming
    }

    def main(args: Array[String]) {
        //val conf = new SparkConf().setAppName("METIS-Formatter");
        //val sc = new SparkContext(conf)

        // read input file
        val adjacencyText = sc.textFile("hdfs://192.168.152.200:9000/datasets/sf1000_updates_friendship/adjacency_format")
        // trim unnecessary information
        val adjacency = adjacencyText.map( l => (l.split("\\|")(0), parseSNBEdges(l) ) )

        // METIS needs undirected graph, therefore we will add reverse of the each original edge
        val undirectedEdges = adjacency.flatMap(vertex => {
            val sourceid = vertex._1
            val neighbours = vertex._2
            neighbours.map( n => Array( (n, sourceid)) )
        })
        val edgeCount = undirectedEdges.count

        // assign order to each vertex in adjacency list, which will create the actual lookup table for METIS partitioner
        // zipWithIndex indices start by 1 by default, so need post processing
        val lookup = adjacency.map( t => t._1 ).zipWithIndex.map( t => (t._1, t._2 + 1))

        //generate oldid - newid lookup table on one pass
        lookup.saveAsObjectFile("hdfs://192.168.152.200:9000/datasets/twitter_rv/metis-lookup")

        // since lookup table is relatively small, we can broadcast it
        val lookupMap = sc.broadcast(lookup.collectAsMap)
        val vertexCount = lookupMap.value.size

        // now we just need to iterate over graph and replace ids
        val adjacencyWithIdentifiers = adjacency.map( vertex => {
            val sourceid = lookupMap.value.get(vertex._1).get
            val neighbours = vertex._2.map( n => lookupMap.value.get(n).get)
            (sourceid, neighbours)
        })

        // now we can output in METIS compatible format, eliminatng source id since METIS implicitly assumes line number is a source id
        val metisAdjacency = adjacencyWithIdentifiers.map( l => l._2.mkString(" "))
        val header = sc.parallelize(Seq( vertexCount + " " + (edgeCount / 2) )).coalesce(1)

        header.saveAsTextFile("hdfs://192.168.152.200:9000/datasets/twitter/metis-header")
        metisAdjacency.saveAsTextFile("hdfs://192.168.152.200:9000/datasets/twitter/metis")
    }

    def generatePartitionLookup(lookupPath: String, partitionPath: String, outputPath: String) {
        // here we assume that lookupPath is the path to the lookup RDD
        // partitionPath is the METIS generated output file

        val lookupPath = "hdfs://192.168.152.200:9000/datasets/sf30_updates/lookup"
        val partitionPath = "hdfs://192.168.152.200:9000/datasets/sf30_updates//metis/metis-adjacency.txt.part.8"
        val outputPath = "hdfs://192.168.152.200:9000/datasets/sf1000_updates_friendship/metis16-lookup"

        // read both files and reverse the key-value order so that METIS ids are join key
        val lookup = sc.objectFile[(String, Long)](lookupPath).map(t => (t._2, t._1))
        val orderMap = sc.textFile(partitionPath).zipWithIndex.map(t => (t._2 + 1, t._1))    

        val partitionMap = lookup.join(orderMap).map(t => (t._2._1, t._2._2))

        partitionMap.map(t => t._1 + "," + t._2).repartition(1).saveAsTextFile(outputPath)

    }
}