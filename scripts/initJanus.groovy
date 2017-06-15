/*
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


import org.janusgraph.core.Cardinality
import org.janusgraph.core.Multiplicity
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.schema.SchemaAction
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/janusGraphLoader.java">janusGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */

/**
 * Helper function to handle Titan Specific initialization, i.e. schema definition and index creation
 * @param janusGraph
 */
Graph initializeJanus(String propertiesFile) {

    janusGraph = JanusGraphFactory.open(propertiesFile)

    List<String> vertexLabels = [
            "person",
            "comment",
            "forum",
            "organisation",
            "place",
            "post",
            "tag",
            "tagclass"]

    List<String> edgeLabels = [
            "containerOf",
            "hasCreator",
            "hasInterest",
            "hasMember",
            "hasModerator",
            "hasTag",
            "hasType",
            "isLocatedIn",
            "isPartOf",
            "isSubclassOf",
            "knows",
            "likes",
            "replyOf",
            "studyAt",
            "workAt"]

    // All property keys with Cardinality.SINGLE
    List<String> singleCardStringPropKeys = [
            "browserUsed", // comment person post
            "classYear", // studyAt
            "content", // comment post
            "firstName", // person
            "gender", // person
            "imageFile", // post
            //"language", // post
            "lastName", // person
            "length", // comment post
            "locationIP", // comment person post
            "name", // organisation place tag tagclass
            "title", // forum
            "type", // organisation place
            "url", // organisation place tag tagclass
            "workFrom"] // workAt

    List<String> singleCardLongPropKeys = [
            "birthday", // person
            "creationDate", // comment forum person post knows likes
            "joinDate"] // workAt

    // All property keys with Cardinality.LIST
    List<String> listCardPropKeys = [
            "email", // person
            "language"] // person, post

    /*
* Explicitly define the graph schema.
*
* Note: For unknown reasons, it seems that each modification to the
* schema must be committed in its own transaction.
*/
    try {
        ManagementSystem mgmt;

        // Declare all vertex labels.
        for (String vLabel : vertexLabels) {
            System.out.println(vLabel);
            mgmt = (ManagementSystem) janusGraph.openManagement();
            mgmt.makeVertexLabel(vLabel).make();
            mgmt.commit();
        }

        // Declare all edge labels.
        for (String eLabel : edgeLabels) {
            System.out.println(eLabel);
            mgmt = (ManagementSystem) janusGraph.openManagement();
            mgmt.makeEdgeLabel(eLabel).multiplicity(Multiplicity.SIMPLE).make();
            mgmt.commit();
        }

        // Delcare all properties with Cardinality.SINGLE
        for (String propKey : singleCardStringPropKeys) {
            System.out.println(propKey);
            mgmt = (ManagementSystem) janusGraph.openManagement();
            mgmt.makePropertyKey(propKey).dataType(String.class)
                    .cardinality(Cardinality.SINGLE).make();
            mgmt.commit();
        }
        for (String propKey : singleCardLongPropKeys) {
            System.out.println(propKey);
            mgmt = (ManagementSystem) janusGraph.openManagement();
            mgmt.makePropertyKey(propKey).dataType(Long.class)
                    .cardinality(Cardinality.SINGLE).make();
            mgmt.commit();
        }

        // Delcare all properties with Cardinality.LIST
        for (String propKey : listCardPropKeys) {
            System.out.println(propKey);
            mgmt = (ManagementSystem) janusGraph.openManagement();
            mgmt.makePropertyKey(propKey).dataType(String.class)
                    .cardinality(Cardinality.LIST).make();
            mgmt.commit();
        }

        /*
     * Create a special ID property where we will store the IDs of
     * vertices in the SNB dataset, and a corresponding index. This is
     * necessary because TitanDB generates its own IDs for graph
     * vertices, but the benchmark references vertices by the ID they
     * were originally assigned during dataset generation.
     */
        mgmt = (ManagementSystem) janusGraph.openManagement();
        mgmt.makePropertyKey("iid").dataType(String.class)
                .cardinality(Cardinality.SINGLE).make();
        mgmt.commit();

        mgmt = (ManagementSystem) janusGraph.openManagement();
        PropertyKey iid = mgmt.getPropertyKey("iid");
        mgmt.buildIndex("byIid", Vertex.class).addKey(iid).buildCompositeIndex();
        mgmt.commit();

        mgmt.awaitGraphIndexStatus(janusGraph, "byIid").call();

        mgmt = (ManagementSystem) janusGraph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("byIid"), SchemaAction.REINDEX)
                .get();
        mgmt.commit();


        mgmt = (ManagementSystem) janusGraph.openManagement();
        mgmt.makePropertyKey("iid_long").dataType(Long.class)
                .cardinality(Cardinality.SINGLE).make();
        mgmt.commit();

        mgmt = (ManagementSystem) janusGraph.openManagement();
        PropertyKey iid_long = mgmt.getPropertyKey("iid_long");
        mgmt.buildIndex("byIidLong", Vertex.class).addKey(iid_long).buildCompositeIndex();
        mgmt.commit();

        mgmt.awaitGraphIndexStatus(janusGraph, "byIidLong").call();

        mgmt = (ManagementSystem) janusGraph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("byIidLong"), SchemaAction.REINDEX)
                .get();
        mgmt.commit();
        
        // finally BLVP ID Index
        mgmt = (ManagementSystem) janusGraph.openManagement();
        PropertyKey blid = mgmt.makePropertyKey("bulkLoader.vertex.id").dataType(String.class).make()
        mgmt.buildIndex("byBulkLoaderVertexId", Vertex.class).addKey(blid).buildCompositeIndex()
        mgmt.commit()

        mgmt.awaitGraphIndexStatus(janusGraph, "byBulkLoaderVertexId").call();

        mgmt = (ManagementSystem) janusGraph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("byBulkLoaderVertexId"), SchemaAction.REINDEX)
                .get();
        mgmt.commit();

    } catch (Exception e) {
        System.out.println("Exception: " + e);
        e.printStackTrace();
    }

    return janusGraph
}
