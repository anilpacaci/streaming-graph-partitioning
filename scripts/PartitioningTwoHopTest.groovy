// Required to serialize results
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

            readCount = new ObjectName("org.apache.cassandra.metrics:type=Keyspace,keyspace=janusgraph,name=ReadLatency");

            for(int i = 0 ; i < numberOfInstances ; i++) {
                int readCount = Integer.parseInt(mbsc.getAttribute(readCountAttribute, "Count").toString());
                lastReads[i] = readCount;
            }
        }

        /**
         * Iterate over all nodes in the cluster and retrieve local counts since restart
         * @return
         */
        public List<Integer> getTotalReadCount() {
            List<Integer> reads = new ArrayList();

            for(int i = 0 ; i < numberOfInstances ; i++) {
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
                int readCount = Integer.parseInt(mbsc.getAttribute(readCountAttribute, "Count").toString());
                reads.add(readCount - lastReads[i]);
                lastReads[i] = readCount;
            }
            return reads;
        }

    }


class PartitioningTwoHopTest {

    private static String[] CSV_HEADERS = ["IID", "VERTEX_PARTITION", "1HOP", "2HOP", "NEIGHBOURHOOD_RETRIEVAL_DURATION",
                                           "PROPERTIES_RETRIEVAL_DURATION", "TOTAL_DURATION",
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

    static void run(Graph graph, String parametersFile, String outputFile, CassandraLocalReadCounter readCounter) {

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
            String iid = it.nextLine().split('\\|')[0]

            DefaultTraversalMetrics metrics = g.V().has('iid', 'person:' + iid).out('knows').out('knows').properties().profile().next()
            Long vertexId = (Long) g.V().has('iid', 'person:' + iid).next().id()
            Long partitionId = getPartitionId(vertexId)

            long totalQueryDurationInMicroSeconds = metrics.getDuration(TimeUnit.MICROSECONDS)
            // index 2 corresponds to valueMap step, where properties of each neighbour is actually retrieved from backend
            long neighbourhoodRetrievalInMicroSeconds = metrics.getMetrics(2).getDuration(TimeUnit.MICROSECONDS)
            long propertiesRetrievalInMicroSeconds = metrics.getMetrics(3).getDuration(TimeUnit.MICROSECONDS)

            // index 1 corresponds to out step
            int oneHopCount = metrics.getMetrics(1).getCount('elementCount')
            int twoHopCount = metrics.getMetrics(2).getCount('elementCount')

            List<String> queryRecord = new ArrayList();
            queryRecord.add(iid)
            queryRecord.add(partitionId.toString())
            queryRecord.add(oneHopCount.toString())
            queryRecord.add(twoHopCount.toString())
            queryRecord.add(neighbourhoodRetrievalInMicroSeconds.toString())
            queryRecord.add(propertiesRetrievalInMicroSeconds.toString())
            queryRecord.add(totalQueryDurationInMicroSeconds.toString())

            List<Integer> readCounts = readCounter.updateReadCount()
            for(int i = 0 ; i < readCounts.size() ; i++) {
                queryRecord.add(readCounts.get(i).toString())
            }

            // add record to CSV
            csvPrinter.writeNext(queryRecord.toArray(new String[0]))

            // since it is read-only rollback is a less expensive option
            g.tx().rollback()
        }

        // close csvWriter
        csvPrinter.close()
    }

    static void run(String graphConfigurationFile, String parametersFile, String outputFile, String jmxConfigurationFile) {
        Configuration graphConfig = new PropertiesConfiguration(graphConfigurationFile)
        Graph graph = JanusGraphFactory.open(graphConfig)
        CassandraLocalReadCounter  readCounter = new CassandraLocalReadCounter(jmxConfigurationFile)
        run(graph, parametersFile, outputFile, readCounter)
    }

    static Long getPartitionId(Long id) {
        // because normal user vertex padding is 3 and partitionId bound is 3
        return ( id >>> 3 ) & 3
    }

}

