import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

//Souce vertex person

val knows = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_knows_person_0_0.csv")

val knowsDF = knows.toDF("person1", "person2", "creationDate");

val email = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_email_emailaddress_0_0.csv")

val emailDF = email.toDF("person_email", "email")

val hasInterest = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_hasInterest_tag_0_0.csv")

val hasInterestDF = hasInterest.toDF("person_interest", "tag")

val person_isLocatedIn = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_isLocatedIn_place_0_0.csv")

val person_isLocatedInDF = person_isLocatedIn.toDF("person_locate", "place")

val likes_comment = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_likes_comment_0_0.csv")

val likes_commentDF = likes_comment.toDF("person", "comment", "creationDate")

val likes_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_likes_post_0_0.csv")

val likes_postDF = likes_post.toDF("person_post", "post", "creationDate")

val speaks = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_speaks_language_0_0.csv")

val speaksDF = speaks.toDF("person_speak", "language")

val studyAt = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_studyAt_organisation_0_0.csv")

val studyAtDF = studyAt.toDF("person_study", "study_organisatoin", "classYear")

val workAt = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/person_workAt_organisation_0_0.csv")

val workAtDF = workAt.toDF("person_work", "work_organisation", "workFrom")

//Source Vertex comment
val comment_hasCreator = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/comment_hasCreator_person_0_0.csv")

val comment_hasCreatorDF = comment_hasCreator.toDF("comment", "person")

val hasTag = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/comment_hasTag_tag_0_0.csv")

val hasTagDF = hasTag.toDF("comment_tag", "tag")

val comment_isLocatedIn = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/comment_isLocatedIn_place_0_0.csv")

val comment_isLocatedInDF = comment_isLocatedIn.toDF("comment_locate", "place")

val replyOf_comment = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/comment_replyOf_comment_0_0.csv")

val replyOf_commentDF = replyOf_comment.toDF("comment1", "comment2")

val replyOf_post = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/comment_replyOf_post_0_0.csv")

val replyOf_postDF = replyOf_post.toDF("comment_post", "post")


//Source Vertex forum
val containerOf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/forum_containerOf_post_0_0.csv")

val containerOfDF = containerOf.toDF("forum_contain", "post")

val hasMemberWithPosts = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/forum_hasMemberWithPosts_person_0_0.csv")

val hasMemberWithPostsDF = hasMemberWithPosts.toDF("forum", "member_with_post", "joinDate")

val hasMember = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/forum_hasMember_person_0_0.csv")

val hasMemberDF = hasMember.toDF("forum_member", "member", "joinDate")

val hasModerator = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/forum_hasModerator_person_0_0.csv")

val hasModeratorDF = hasModerator.toDF("forum_moderator", "person")

val forum_hasTag = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/forum_hasTag_tag_0_0.csv")

val forum_hasTagDF = forum_hasTag.toDF("forum_tag", "tag")

val joined_forum = hasMemberWithPostsDF.join(containerOfDF, containerOfDF("forum_contain") === hasMemberWithPostsDF("forum"), "inner").join(hasMemberDF, hasMemberDF("forum_member") === hasMemberWithPostsDF("forum"), "inner").join(hasModeratorDF, hasModeratorDF("forum_moderator") === hasMemberWithPostsDF("forum"), "inner").join(forum_hasTagDF, forum_hasTagDF("forum_tag") === hasMemberWithPostsDF("forum"), "inner").groupBy("forum")agg(collect_list("person") as "moderator")

//Source Vertex organisation
val organisation_isLocatedIn = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/organisation_isLocatedIn_place_0_0.csv")

val organisation_isLocatedInDF = organisation_isLocatedIn.toDF("organisation", "place")

//Souce Vertex post
val post_hasCreator = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/post_hasCreator_person_0_0.csv")

val post_hasCreatorDF = post_hasCreator.toDF("post", "person")

val post_hasTag = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/post_hasTag_tag_0_0.csv")

val post_hasTagDF = post_hasTag.toDF("post_tag", "tag")

val post_isLocatedIn = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/post_isLocatedIn_place_0_0.csv")

val post_isLocatedInDF = post_isLocatedIn.toDF("post_locate", "place")

//Source Vertex place
val isPartOf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("/Users/utakuzen/Documents/4b/URA/validation_set/place_isPartOf_place_0_0.csv")

val isPartOfDF = isPartOf.toDF("place1", "place2")

//join tables with source vertex person
val joined_person = likes_commentDF.join(knowsDF, likes_commentDF("person") === knowsDF("person1"), "inner").join(emailDF, emailDF("person_email") === likes_commentDF("person"), "inner").join(hasInterestDF, hasInterestDF("person_interest") === likes_commentDF("person"), "inner").join(person_isLocatedInDF, person_isLocatedInDF("person_locate") === likes_commentDF("person"), "inner").join(likes_postDF, likes_postDF("person_post") === likes_commentDF("person"), "inner").join(speaksDF, speaksDF("person_speak") === likes_commentDF("person"), "inner").join(studyAtDF, studyAtDF("person_study") === likes_commentDF("person"), "inner").join(workAtDF, workAtDF("person_work") === likes_commentDF("person"), "inner").groupBy("person").agg(collect_list("comment") as "comments", collect_list("person2") as "friends", collect_list("email") as "emails")

//join tables with source vertex comment
val joined_comment = comment_hasCreatorDF.join(hasTagDF, hasTagDF("comment_tag") === comment_hasCreatorDF("comment"), "inner").join(replyOf_postDF, replyOf_postDF("comment_post") === comment_hasCreatorDF("comment"), "inner").join(comment_isLocatedInDF, comment_isLocatedInDF("comment_locate") === comment_hasCreatorDF("comment"), "inner").join(replyOf_commentDF, replyOf_commentDF("comment1") === comment_hasCreatorDF("comment"), "inner").groupBy("comment").agg(collect_list("comment2") as "reply")



//join tables with source vertex forum

val joined_forum = hasMemberWithPostsDF.join(containerOfDF, containerOfDF("forum_contain") === hasMemberWithPostsDF("forum"), "inner").join(hasMemberDF, hasMemberDF("forum_member") === hasMemberWithPostsDF("forum"), "inner").join(hasModeratorDF, hasModeratorDF("forum_moderator") === hasMemberWithPostsDF("forum"), "inner").join(forum_hasTagDF, forum_hasTagDF("forum_tag") === hasMemberWithPostsDF("forum"), "inner").groupBy("forum")agg(collect_list("person") as "moderator")

//join tables with source vertex post

val joined_forum = post_hasCreatorDF.join(post_hasTagDF, post_hasCreatorDF("post") === post_hasTagDF("post_tag"), "inner").join(post_isLocatedInDF, post_isLocatedInDF("post_locate") === post_hasCreatorDF("post"), "inner").groupBy("post").agg(collect_list("person") as "creator")





