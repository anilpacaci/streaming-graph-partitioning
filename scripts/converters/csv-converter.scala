import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


case class Edge(val target: String, val elabel: String, val creationDate: Long)


object CSVConverter {
def main(args: Array[String]) {
    //create spark context
    val conf = new SparkConf().setAppName("Adjacency List Converison");
    val sc = new SparkContext(conf)

    //set up paths
    val read_path = new StringBuilder();
    val write_path = new StringBuilder();
    read_path.append(args(0)).append("/")
    write_path.append(args(1));

    import org.apache.spark.sql.SQLContext
    import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
    import java.text.SimpleDateFormat
    import java.util.TimeZone


    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val customScheme = StructType(Array(
    StructField("fist_id", LongType, true),
    StructField("second_id", LongType, true),
    StructField("time_stamp", StringType, true)))

    val creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    creationDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))


    //to convert each Edge to string
    def edge_to_string(e: Edge) : String = {
        val output = new StringBuilder()
        output.append(e.elabel).append(",").append(e.target)
        if(e.creationDate != 0){
            output.append(",").append(e.creationDate);
        }
        return output.toString();
    }



    //to convert each row to string
    def output_csv_line (rdd: (String, Option[Array[Edge]], Option[Array[Edge]])) : String = {
        //throw each Edge to the above function then append then together with the Long
        val output = new StringBuilder()
        output.append(rdd._1).append("|")

        val outgoing_edges = rdd._2
        val incoming_edges = rdd._3

        if(!outgoing_edges.isEmpty) {
            val outgoing_edge_list = outgoing_edges.get
            output.append( outgoing_edge_list.map(e => edge_to_string(e)).mkString(" ") ) 
        }

        output.append("|")
        
        if(!incoming_edges.isEmpty) {
            val incoming_edge_list = incoming_edges.get
            output.append( outgoing_edge_list.map(e => edge_to_string(e)).mkString(" ") )
        }
        
        return output.toString
    }

    //do conversion as before


    //Souce vertex person
    val knows_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").option("nullValue", "").schema(customScheme).load(read_path.toString + "person_knows_person_0_0.csv")
    val knows = knows_person.map(row => ( "person:" + row.getLong(0).toString, Array(new Edge( "person:" + row.getLong(1).toString, "knows", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd


    val hasInterest_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_hasInterest_tag_0_0.csv")
    val hasInterest = hasInterest_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge("tag:" + row.getLong(1).toString, "hasInterest", 0l)))).rdd


    val isLocatedIn_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_isLocatedIn_place_0_0.csv")
    val person_isLocatedIn = isLocatedIn_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge("place:" + row.getLong(1), "isLocatedIn", 0l)))).rdd


    val likes_comment_person = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_likes_comment_0_0.csv")
    val likes_comment = likes_comment_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge( "comment:" + row.getLong(1), "likes", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd


    val likes_post_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_likes_post_0_0.csv")
    val likes_post = likes_post_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge( "post:" + row.getLong(1), "likes", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd

    val studyAt_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_studyAt_organisation_0_0.csv")
    val studyAt = studyAt_person.map(row => ( "person:" + row.getLong(0).toString, Array(new Edge( "organisation:" + row.getLong(1), "studyAt", row.getString(2).toLong )) ) ).rdd


    val workAt_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_workAt_organisation_0_0.csv")
    val workAt = workAt_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge( "organisation:" + row.getLong(1), "workAt", row.getString(2).toLong )) ) ).rdd


    //Source Vertex comment
    val hasCreator_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_hasCreator_person_0_0.csv")
    val hasCreator = hasCreator_comment.map(row => ("comment:" + row.getLong(0).toString, Array(new Edge("person:" + row.getLong(1), "hasCreator", 0l )))).rdd

    val hasTag_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_hasTag_tag_0_0.csv")
    val hasTag =  hasTag_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( "tag:" + row.getLong(1), "hasTag", 0l )))).rdd

    val isLocatedIn_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_isLocatedIn_place_0_0.csv")
    val comment_isLocatedIn =  isLocatedIn_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( "place:" + row.getLong(1), "isLocatedIn", 0l )))).rdd


    val replyOf_comment_comment = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_replyOf_comment_0_0.csv")
    val replyOf_comment =  replyOf_comment_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( "comment:" + row.getLong(1), "replyOf", 0l )))).rdd


    val replyOf_post_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_replyOf_post_0_0.csv")
    val replyOf_post = replyOf_post_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( "post:" + row.getLong(1), "replyOf", 0l )))).rdd


    //Source Vertex forum
    val containerOf_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_containerOf_post_0_0.csv")
    val containerOf = containerOf_forum.map(row => ("forum:" +  row.getLong(0).toString, Array(new Edge( "post:" + row.getLong(1), "containerOf", 0l )))).rdd

//    val hasMemberWithPosts_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasMemberWithPosts_person_0_0.csv")
//    val hasMemberWithPosts = hasMemberWithPosts_forum.map(row => ("forum:" +  row.getLong(0).toString, Array(new Edge( "person:" + row.getLong(1),"hasMemberWithPosts", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd
    val hasMember_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasMember_person_0_0.csv")
    val hasMember = hasMember_forum.map(row => ("forum:" +  row.getLong(0).toString, Array(new Edge( "person:" + row.getLong(1), "hasMember", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd

    val hasModerator_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasModerator_person_0_0.csv")
    val hasModerator = hasModerator_forum.map(row => ( "forum:" + row.getLong(0).toString, Array(new Edge( "person:" + row.getLong(1), "hasModerator", 0l )) ) ).rdd


    val hasTag_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasTag_tag_0_0.csv")
    val forum_hasTag = hasTag_forum.map(row => ( "forum:" + row.getLong(0).toString, Array(new Edge( "tag:" + row.getLong(1), "hasTag", 0l )) ) ).rdd


    //Source Vertex organisation
    val isLocatedIn_organisation = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "organisation_isLocatedIn_place_0_0.csv")
    val organisation_isLocatedIn = isLocatedIn_organisation.map(row => ("organisation:" +  row.getLong(0).toString, Array(new Edge("place:" + row.getLong(1), "isLocatedIn", 0l )) ) ).rdd


    //Souce Vertex post
    val hasCreator_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "post_hasCreator_person_0_0.csv")
    val post_hasCreator = hasCreator_post.map(row => ("post:" +  row.getLong(0).toString, Array(new Edge( "person:" + row.getLong(1), "hasCreator", 0l)) ) ).rdd

    val hasTag_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "post_hasTag_tag_0_0.csv")
    val post_hasTag = hasTag_post.map(row => ("post:" + row.getLong(0).toString, Array(new Edge( "tag:" + row.getLong(1), "hasTag", 0l )) ) ).rdd


    val isLocatedIn_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "post_isLocatedIn_place_0_0.csv")
    val post_isLocatedIn = isLocatedIn_post.map(row => ("post:" + row.getLong(0).toString, Array(new Edge( "place:" + row.getLong(1), "isLocatedIn", 0l )) ) ).rdd

    //Source Vertex place
    val isPartOf_place = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "place_isPartOf_place_0_0.csv")
    val isPartOf = isPartOf_place.map(row => ("place:" + row.getLong(0).toString, Array(new Edge( "place:" + row.getLong(1), "isPartOf", 0l )) ) ).rdd

    //source vertex tag
    val hasType_tag = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "tag_hasType_tagclass_0_0.csv")
    val hasType = hasType_tag.map(row => ("tag:" + row.getLong(0).toString, Array(new Edge( "tagclass:" + row.getLong(1), "hasType", 0l )) ) ).rdd

    //source vertex tagclass
    val isSubclassOf_tagclass = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "tagclass_isSubclassOf_tagclass_0_0.csv")
    val isSubclassOf = isSubclassOf_tagclass.map(row => ("tagclass:" + row.getLong(0).toString, Array(new Edge( "tagclass:" + row.getLong(1), "isSubclassOf", 0l )) ) ).rdd

    //Union all tables with source Vertex person
    val person_joined = knows.union(hasInterest).union(person_isLocatedIn).union(likes_comment).union(likes_post).union(studyAt).union(workAt).reduceByKey((l1, l2) => l1 ++ l2);

    //Union all tables with source Vertex comment
    val comment_joined = hasCreator.union(hasTag).union(comment_isLocatedIn).union(replyOf_comment).union(replyOf_post).reduceByKey((l1, l2) => l1 ++ l2);


    //Union all tables with source Vertex forum
    //val forum_joined = containerOf.union(hasMemberWithPosts).union(hasMember).union(hasModerator).union(forum_hasTag).reduceByKey((l1, l2) => l1 ++ l2);
    val forum_joined = containerOf.union(hasMember).union(hasModerator).union(forum_hasTag).reduceByKey((l1, l2) => l1 ++ l2);

    //Union all tables with source Vertex post
    val post_joined = post_hasCreator.union(post_hasTag).union(post_isLocatedIn).reduceByKey((l1, l2) => l1 ++ l2);

    val organisation_joined = organisation_isLocatedIn.reduceByKey((l1, l2) => l1 ++ l2)
    val place_joined = isPartOf.reduceByKey((l1, l2) => l1 ++ l2);
    val tag_joined = hasType.reduceByKey((l1, l2) => l1 ++ l2);
    val tagclass_joined = isSubclassOf.reduceByKey((l1, l2) => l1 ++ l2);


    val all_unioned = person_joined.union(comment_joined).union(forum_joined).union(organisation_joined).union(post_joined).union(place_joined).union(tag_joined).union(tagclass_joined)

    val reverse_edge = all_unioned.flatMap(vertex => {
        val sourceid = vertex._1
        val neighbours = vertex._2
        neighbours.flatMap(n => Array( (n.target, new Edge(sourceid, n.elabel, n.creationDate))))
    })

    val regular_edge = all_unioned.flatMap(vertex => {
        val sourceid = vertex._1
        val neighbours = vertex._2
        neighbours.flatMap(n => Array( (sourceid, n)))
    })


    val reduced_reverse_edges = reverse_edge.map(e => (e._1, Array(e._2))).reduceByKey( (l1, l2) => l1 ++ l2)
    val reduced_regular_edges = regular_edge.map(e => (e._1, Array(e._2))).reduceByKey( (l1, l2) => l1 ++ l2)
    val joined_edges = reduced_regular_edges.fullOuterJoin(reduced_reverse_edges)

    val flattened = joined_edges.map(r => (r._1, r._2._1, r._2._2))

    val result = flattened.map(r => output_csv_line(r))
    result.repartition(1).saveAsTextFile(write_path.toString)



    sc.stop()
    }
}

// specific for person_know_person where there are vertices with no incoming / outgoing edges
// val flattened = joined_edges.fullOuterJoin(person).map(r => (r._1, r._2._1.getOrElse[(Option[Array[Edge]], Option[Array[Edge]])]( ( Some(Array[Edge]()), Some(Array[Edge]()) ) )._1, r._2._1.getOrElse[(Option[Array[Edge]], Option[Array[Edge]])]( ( Some(Array[Edge]()), Some(Array[Edge]()) ) )._2)  )
// val person = person_csv.map(r => ("person:" + r.getString(0), Array[Edge]() ) ).rdd