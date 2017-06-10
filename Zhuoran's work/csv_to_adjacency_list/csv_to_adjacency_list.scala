import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

case class Edge(val target: Long, val elabel: String, val creationDate: Long)

object csvConversion {
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
            println(e.size)
            val output = new StringBuilder()

            output.append(e.elabel).append(":").append(e.target)

            if(e.creationDate != 0){
                output.append(":").append("StartDate:").append(e.creationDate);
            }
            return output.toString();
        }


        //to convert each row to string
        def output_csv_line (rdd: (String, Array[Edge], Option[Array[Edge]])) : String = {


             //throw each Edge to the above function then append then together with the Long
             val output = new StringBuilder()
             output.append(rdd._1).append("|")

             val outgoing_edges = rdd._2
             val incoming_edges = rdd._3

             for(e  <- outgoing_edges) {
                 output.append(edge_to_string(e)).append(",")
             }
             output.deleteCharAt(output.length()-1)
             output.append("|")

             if(incoming_edges.isEmpty) {
                 return output.toString
             }
             val converted = incoming_edges.get

             for(e <- converted) {
                 output.append(edge_to_string(e)).append(",")
             }
             return output.toString
         }


        //do conversion as before

        //Souce vertex person
        val knows_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").option("nullValue", "").schema(customScheme).load(read_path.toString + "person_knows_person_0_0.csv")
        val knows = knows_person.map(row => ( "person:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "person", "knows", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd


        val hasInterest_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_hasInterest_tag_0_0.csv")
        val hasInterest = hasInterest_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge(row.getLong(1), "tag", "hasInterest", 0l)))).rdd


        val isLocatedIn_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_isLocatedIn_place_0_0.csv")
        val person_isLocatedIn = isLocatedIn_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge(row.getLong(1), "place", "isLocatedIn", 0l)))).rdd


        val likes_comment_person = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_likes_comment_0_0.csv")
        val likes_comment = likes_comment_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "comment", "likes", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd


        val likes_post_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_likes_post_0_0.csv")
        val likes_post = likes_post_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "post", "likes", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd


        val studyAt_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_studyAt_organisation_0_0.csv")
        val studyAt = studyAt_person.map(row => ( "person:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "organisation", "studyAt", row.getString(2).toLong )) ) ).rdd


        val workAt_person = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "person_workAt_organisation_0_0.csv")
        val workAt = workAt_person.map(row => ("person:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "organisation", "workAt", row.getString(2).toLong )) ) ).rdd



        //Source Vertex comment
        val hasCreator_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_hasCreator_person_0_0.csv")

        val hasCreator = hasCreator_comment.map(row => ("comment:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "person", "hasCreator", 0l )))).rdd

        val hasTag_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_hasTag_tag_0_0.csv")
        val hasTag =  hasTag_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "tag", "hasTag", 0l )))).rdd


        val isLocatedIn_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_isLocatedIn_place_0_0.csv")
        val comment_isLocatedIn =  isLocatedIn_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "place", "isLocatedIn", 0l )))).rdd


        val replyOf_comment_comment = sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_replyOf_comment_0_0.csv")
        val replyOf_comment =  replyOf_comment_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "comment", "replyOf", 0l )))).rdd



        val replyOf_post_comment = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "comment_replyOf_post_0_0.csv")

        val replyOf_post = replyOf_post_comment.map(row => ("comment:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "post", "replyOf", 0l )))).rdd


        //Source Vertex forum
        val containerOf_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_containerOf_post_0_0.csv")

        val containerOf = containerOf_forum.map(row => ("forum:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "post", "containerOf", 0 )))).rdd

        val hasMemberWithPosts_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasMemberWithPosts_person_0_0.csv")
        val hasMemberWithPosts = hasMemberWithPosts_forum.map(row => ("forum:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "person", "hasMemberWithPosts", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd


        val hasMember_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasMember_person_0_0.csv")
        val hasMember = hasMember_forum.map(row => ("forum:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "person", "hasMember", creationDateFormat.parse(row.getString(2)).getTime() )) ) ).rdd


        val hasModerator_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasModerator_person_0_0.csv")
        val hasModerator = hasModerator_forum.map(row => ( "forum:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "person", "hasModerator", 0 )) ) ).rdd


        val hasTag_forum = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "forum_hasTag_tag_0_0.csv")
        val forum_hasTag = hasTag_forum.map(row => ( "forum:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "tag", "hasTag", 0 )) ) ).rdd


        //Source Vertex organisation
        val isLocatedIn_organisation = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "organisation_isLocatedIn_place_0_0.csv")


        val organisation_isLocatedIn = isLocatedIn_organisation.map(row => ("organisation:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "place", "isLocatedIn", 0 )) ) ).rdd



        //Souce Vertex post
        val hasCreator_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "post_hasCreator_person_0_0.csv")

        val post_hasCreator = hasCreator_post.map(row => ("post:" +  row.getLong(0).toString, Array(new Edge( row.getLong(1), "person", "hasCreator", 0)) ) ).rdd

        val hasTag_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "post_hasTag_tag_0_0.csv")
        val post_hasTag = hasTag_post.map(row => ("post:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "tag", "hasTag", 0 )) ) ).rdd


        val isLocatedIn_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "post_isLocatedIn_place_0_0.csv")
        val post_isLocatedIn = isLocatedIn_post.map(row => ("post:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "place", "isLocatedIn", 0 )) ) ).rdd

        //Source Vertex place
        val isPartOf_place = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(customScheme).option("delimiter", "|").load(read_path.toString + "place_isPartOf_place_0_0.csv")
        val isPartOf = isPartOf_place.map(row => ("place:" + row.getLong(0).toString, Array(new Edge( row.getLong(1), "place", "isPartOf", 0 )) ) ).rdd


        //Union all tables with source Vertex person
        val person_joined = knows.union(hasInterest).union(person_isLocatedIn).union(likes_comment).union(likes_post).union(studyAt).union(workAt).reduceByKey((l1, l2) => l1 ++ l2);

        //Union all tables with source Vertex comment
        val comment_joined = hasCreator.union(hasTag).union(comment_isLocatedIn).union(replyOf_comment).union(replyOf_post).reduceByKey((l1, l2) => l1 ++ l2);

        //Union all tables with source Vertex forum
        val forum_joined = containerOf.union(hasMemberWithPosts).union(hasMember).union(hasModerator).union(forum_hasTag).reduceByKey((l1, l2) => l1 ++ l2);

        //Union all tables with source Vertex post
        val post_joined = post_hasCreator.union(post_hasTag).union(post_isLocatedIn).reduceByKey((l1, l2) => l1 ++ l2);

        val organisation_joined = organisation_isLocatedIn.reduceByKey((l1, l2) => l1 ++ l2)
        val place_joined = isPartOf.reduceByKey((l1, l2) => l1 ++ l2);

        val all_unioned = person_joined.union(comment_joined).union(forum_joined).union(organisation_joined).union(post_joined).union(place_joined)

        val reverse_edge = all_unioned.flatMap(vertex => {
             val sourceid = vertex._1
             val neighbours = vertex._2
             val source_split = sourceid.split(":")
             neighbours.flatMap(n => Array( (n.vlabel + ":" + n.target.toString, new Edge(source_split(1).toLong, source_split(0), n.elabel, n.creationDate))))
        })

        val regular_edge = all_unioned.flatMap(vertex => {
            val sourceid = vertex._1
            val neighbours = vertex._2
            neighbours.flatMap(n => Array( (sourceid, n)))
        })

        val reduced_reverse_edges = reverse_edge.map(e => (e._1, Array(e._2))).reduceByKey( (l1, l2) => l1 ++ l2)
        val reduced_regular_edges = regular_edge.map(e => (e._1, Array(e._2))).reduceByKey( (l1, l2) => l1 ++ l2)
        val joined_edges = reduced_reverse_edges.leftOuterJoin(reduced_regular_edges)


        val all_unioned = person_joined.union(comment_joined).union(forum_joined).union(organisation_joined).union(post_joined).union(place_joined)

        val reverse_edge = all_unioned.flatMap(vertex => {
             val sourceid = vertex._1
             val neighbours = vertex._2
             val source_split = sourceid.split(":")
             neighbours.flatMap(n => Array( (n.vlabel + ":" + n.target.toString, new Edge(source_split(1).toLong, source_split(0), n.elabel, n.creationDate))))
        })

        val regular_edge = all_unioned.flatMap(vertex => {
            val sourceid = vertex._1
            val neighbours = vertex._2
            neighbours.flatMap(n => Array( (sourceid, n)))
        })


        val reduced_reverse_edges = reverse_edge.map(e => (e._1, Array(e._2))).reduceByKey( (l1, l2) => l1 ++ l2)
        val reduced_regular_edges = regular_edge.map(e => (e._1, Array(e._2))).reduceByKey( (l1, l2) => l1 ++ l2)
        val joined_edges = reduced_reverse_edges.leftOuterJoin(reduced_regular_edges)

        val flattened = joined_edges.map(r => (r._1, r._2._1, r._2._2))

        val result = flattened.map(r => output_csv_line(r))
        result.repartition(1).saveAsTextFile(write_path.toString)


        sc.stop()
    }
}
