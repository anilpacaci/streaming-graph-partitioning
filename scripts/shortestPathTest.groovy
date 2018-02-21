// Required to serialize resultsl
:install com.opencsv opencsv 4.0

import com.opencsv.CSVWriter
import org.janusgraph.core.JanusGraphFactory
import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics
import org.apache.tinkerpop.gremlin.structure.Graph

import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;


import java.util.ArrayList
import java.util.concurrent.TimeUnit

/**
 * Created by apacaci on 2/5/17.
 *
 * Helper Groovy script to run 2-hop friendship query over JanusGraph
 * It measures the time to retrieve 2-hop friendship
 */

public class CassandraLocalReadCounter {

        String cassandraJMXPort;
        int numberOfInstances;

        List<MBeanServerConnection> serverConnections;

        Integer[] lastReads;

        ObjectName readCountAttribute;

        public CassandraLocalReadCounter(String configurationFile) {
            Configuration configuration = new PropertiesConfiguration(configurationFile);

            String[] ipAddresses = configuration.getStringArray("cassandra.host");
            cassandraJMXPort = configuration.getString("cassandra.jmx");
            numberOfInstances = configuration.getInt("cassandra.clustersize");

            serverConnections = new ArrayList<>(numberOfInstances);
            lastReads = new Integer[numberOfInstances];

            for(String ipAddress : ipAddresses) {
                JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + ipAddress + ":" + cassandraJMXPort + "/jmxrmi")
                JMXConnector jmxc = JMXConnectorFactory.connect(url, null)

                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                serverConnections.add(mbsc);
            }
	
	    log.info("All JMX connections initialized")

            readCountAttribute = new ObjectName("org.apache.cassandra.metrics:type=Keyspace,keyspace=janusgraph,name=ReadLatency");

            for(int i = 0 ; i < numberOfInstances ; i++) {
		MBeanServerConnection mbsc = serverConnections.get(i)
                int readCount = Integer.parseInt(mbsc.getAttribute(readCountAttribute, "Count").toString());
                lastReads[i] = readCount;
            }
	
	    log.info("Initial read counts are retrieved")
        }

        /**
         * Iterate over all nodes in the cluster and retrieve local counts since restart
         * @return
         */
        public List<Integer> getTotalReadCount() {
            List<Integer> reads = new ArrayList();

            for(int i = 0 ; i < numberOfInstances ; i++) {
		MBeanServerConnection mbsc = serverConnections.get(i)
                int readCount = Integer.parseInt(mbsc.getAttribute(readCountAttribute, "Count").toString());
                reads.add(readCount);
                lastReads[i] = readCount;
            }
        }

        /**
         * Iterate over all nodes in cluster and retrieve local counts in incremental manner
         * Every call to this function updates local data structures and returns read counts since last call
         * @return
         */
        public List<Integer> updateReadCount() {
            List<Integer> reads = new ArrayList();

            for(int i = 0 ; i < numberOfInstances ; i++) {
		MBeanServerConnection mbsc = serverConnections.get(i)
                int readCount = Integer.parseInt(mbsc.getAttribute(readCountAttribute, "Count").toString());
                reads.add(readCount - lastReads[i]);
                lastReads[i] = readCount;
            }
            return reads;
        }

    }






class ShortestPathTest {

    private static String[] CSV_HEADERS = ["IID", "TARGET_ID", "TOTAL_DURATION",
                                           "READS_C1",
                                           "READS_C2",
                                           "READS_C3",
                                           "READS_C4",
                                           "READS_C5",
                                           "READS_C6",
                                           "READS_C7",
                                           "READS_C8",
                                           "READS_C9",
                                           "READS_C10",
                                           "READS_C11",
                                           "READS_C12",
                                           "READS_C13",
                                           "READS_C14",
                                           "READS_C15",
                                           "READS_C16"
    ]

    static void start(Graph graph, String parametersFile, String outputFile, CassandraLocalReadCounter readCounter, int depth){

        GraphTraversalSource g = graph.traversal()

        LineIterator it = FileUtils.lineIterator(new File(parametersFile))

        // skip the first line
        it.next()

        // create the CSV Writer
        FileWriter fileWriter = new FileWriter(outputFile)
        CSVWriter csvPrinter = new CSVWriter(fileWriter)
        csvPrinter.writeNext(CSV_HEADERS)


        while(it.hasNext()) {
            // we know that query_1_param.txt has iid as first parameter
	        String current_line = it.nextLine()
            String iid = current_line.split('\\|')[0]
	    String targetId = current_line.split('\\|')[1]

	    Long sourceVertexId = (Long) g.V().has('iid', 'person:' + iid).next().id()
	    Long targetVertexId = (Long) g.V().has('iid', 'person:' + targetId).next().id()
	    Long partitionId = getPartitionId(sourceVertexId)	

	    log.info("New vertex id: " + iid)

	    boolean pathDetected = false;
	    long totalQueryDurationInMicroSeconds = 0;

	    ArrayList<Long> sourceCurrent = new ArrayList()
	    ArrayList<Long> sourceNext = new ArrayList()
	    ArrayList<Long> targetCurrent = new ArrayList()
	    ArrayList<Long> targetNext = new ArrayList()

	    sourceCurrent.add(sourceVertexId)
	    targetCurrent.add(targetVertexId)

	    int count = 0;

	    System.out.println("Entering loop with current depth: " + count)
	    while(count < depth){
		 /// metrics calculation
                DefaultTraversalMetrics metrics =  g.V(sourceCurrent).out('knows').properties().profile().next()
                totalQueryDurationInMicroSeconds += metrics.getDuration(TimeUnit.MICROSECONDS)
                metrics = g.V(targetCurrent).out('knows').properties().profile().next()
                totalQueryDurationInMicroSeconds += metrics.getDuration(TimeUnit.MICROSECONDS)

	    	// get first hop neighbor of source and target

		System.out.println("Getting vertex ids in next layer")
            	sourceNext.addAll(g.V(sourceCurrent).out('knows').id().fold().next())
	    	targetNext.addAll(g.V(targetCurrent).out('knows').id().fold().next())

		System.out.println("Done in getting ids")
		System.out.println("sourceNext with size: " + sourceNext.size() + " " + "targetNext with size: " + targetNext.size())
	    
	    	// check for intersection
		System.out.println("Checking id from sourceNext in targetNext....")
	    	for(long id: sourceNext){
			System.out.println("Checking id: " + id)
			if(targetNext.contains(id)){
				System.out.println("!!! id: " + id + "  in targetNext")
				pathDetected = true;
				break;
			}
			System.out.println("id: " + id + " not in targetNext")
	    	}
		System.out.println("Finished checking all ids in sourceNext")

		if(pathDetected) break;
	        count++;
		// update lists
		sourceCurrent.removeAll();
		sourceCurrent.addAll(sourceNext);
		targetCurrent.removeAll();
		targetCurrent.addAll(targetNext);
		sourceNext.removeAll();
		targetNext.removeAll();
		System.out.println("Cleared current buffer. Now sizeof sourceNext: " + sourceNext.size() + " sizeof targetNext: " + targetNext.size())
	    }

	    // if we still cannot detect the path, we may discard the pair
	    if(!pathDetected){
		continue;
	    }

	    // otherwise, proceed writing records
            List<String> queryRecord = new ArrayList();
            queryRecord.add(iid)
	    queryRecord.add(targetId)
            //queryRecord.add(partitionId)
	    //queryRecord.add(oneHopCount.toString())
     	    //queryRecord.add(twoHopCount.toString())
            //queryRecord.add(neighbourhoodRetrievalInMicroSeconds.toString())
            //queryRecord.add(propertiesRetrievalInMicroSeconds.toString())
            queryRecord.add(totalQueryDurationInMicroSeconds.toString())

	    List<Integer> readCounts = readCounter.updateReadCount()
            for(int i = 0 ; i < readCounts.size() ; i++) {
                queryRecord.add(readCounts.get(i).toString())
            }

	    log.info("Retrieved cassandra stats for query with vertex: " + iid)
          
            // add record to CSV
            csvPrinter.writeNext(queryRecord.toArray(new String[0]))

            // since it is read-only rollback is a less expensive option
            g.tx().rollback()
        }

        // close csvWriter
        csvPrinter.close()
    }

    static void run(String graphConfigurationFile, String parametersFile, String outputFile, String jmxConfigurationFile, int depth) {
	log.info("Opening the graph file")
        Configuration graphConfig = new PropertiesConfiguration(graphConfigurationFile)
        Graph graph = JanusGraphFactory.open(graphConfig)
	log.info("JanusGraph Opened")
	CassandraLocalReadCounter readCounter = new CassandraLocalReadCounter(jmxConfigurationFile)
        start(graph, parametersFile, outputFile, readCounter, depth)
    }

    static Long getPartitionId(Long id) {
        // because normal user vertex padding is 3 and partitionId bound is 3
        return ( id >>> 3 ) & 3
    }

}
