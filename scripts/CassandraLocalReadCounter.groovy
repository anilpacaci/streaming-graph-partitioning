import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;


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

        readCountAttribute = new ObjectName("org.apache.cassandra.metrics:type=Keyspace,keyspace=janusgraph,name=ReadLatency");

        for(int i = 0 ; i < numberOfInstances ; i++) {
	    MBeanServerConnection mbsc = serverConnections.get(i)
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
            MBeanServerConnection mbsc = serverConnections.get(i)
            int readCount = Integer.parseInt(mbsc.getAttribute(readCountAttribute, "Count").toString());
            reads.add(readCount);
            lastReads[i] = readCount;
        }
	return reads
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
