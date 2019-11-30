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


import com.whalin.MemCached.MemCachedClient
import com.whalin.MemCached.SockIOPool
import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.Cardinality
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.schema.SchemaAction
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.core.Multiplicity
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.schema.SchemaAction
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.apache.tinkerpop.gremlin.structure.Graph

import java.io.File

import java.nio.file.Path
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap;

import java.util.Timer
import java.util.TimerTask

/**
 * This is a Groovy Script to run inside gremlin console, for loading SNAP ADJ formatted data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
class ADJParser {

    static String DATASET_VOLUME = "/sgp/datasets/"
    static String RESULT_VOLUME = "/sgp/results/"
    static String PARAMETERS_VOLUME = "/sgp/parameters/"

    static int TX_MAX_RETRIES = 1000

    enum ElementType { VERTEX, PROPERTY, EDGE }

    static class SharedGraphReader {

        int batchSize
        LineIterator it
        String fileName
        String[] colNames
        long lineCount
        long startIndex
        int reportingPeriod
        Timer timer
        Thread writerThread
        boolean isGraphSNB

        ArrayBlockingQueue<List<String>> batchQueue
        AtomicBoolean isExhausted
        AtomicLong counter

        SharedGraphReader(Path filePath, int batchSize, reportingPeriod, boolean isGraphSNB) {
            this.batchSize = batchSize
            this.reportingPeriod = reportingPeriod

            this.it = FileUtils.lineIterator(filePath.toFile(), "UTF-8")
            this.fileName = filePath.getFileName().toString()
            this.colNames = isGraphSNB ? it.nextLine().split("\\|") : []
            this.lineCount = 0
            this.startIndex = 0

            this.isGraphSNB = isGraphSNB

            this.counter = new AtomicLong(0)
            this.isExhausted = new AtomicBoolean(false)
            this.batchQueue = new ArrayBlockingQueue<>(100 * batchSize)

            this.timer = new Timer()
        }

        synchronized List<String> next() {
            return batchQueue.poll()
        }

        synchronized boolean hasNext() {
            // meaning that there are still lines to get
            return !isExhausted.get() || !batchQueue.isEmpty()
        }

        void start() {
            System.out.println("Loading file: " + fileName)

            int secondCounter = 0

            // start populating the queue
            writerThread = new Thread() {
                @Override
                void run() {
                    while (it.hasNext()) {
                        List<String> lines = new ArrayList<>(batchSize)
                        for (int i = 0; i < batchSize && it.hasNext(); i++) {
                            lines.add(it.nextLine())
                        }
                        batchQueue.put(lines)
                    }
                    isExhausted.set(true)
                }
            }

            writerThread.start()

            // start reporter
            timer.schedule(new TimerTask() {
                @Override
                void run() {
                    // set to zero after reporting
                    int currentCount = counter.getAndSet(0)
                    println(String.format("Second: %d\t- Throughput: %d", secondCounter++, currentCount))
                }
            }, 0, reportingPeriod * 1000)
        }

        void stop() {
            timer.cancel()
            writerThread.interrupt()
        }
    }


    static class ADJGraphLoader implements Callable {

        JanusGraph graph
        SharedGraphReader graphReader
        ElementType elementType
        AtomicLong counter
        ConcurrentHashMap<String, Long> idMapping
        boolean isGraphSNB

        ADJGraphLoader(JanusGraph graph, SharedGraphReader graphReader, ElementType elementType, Map<String, Long> idMapping, boolean isGraphSNB) {
            this.graph = graph
            this.graphReader = graphReader
            this.elementType = elementType
            this.counter = graphReader.getCounter()
            this.idMapping = idMapping
            this.isGraphSNB = isGraphSNB
        }

        Vertex addVertex(String identifier) {
			String entityName = "person"
            Map<Object, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("iid", entityName + ":" + identifier);
            propertiesMap.put("iid_long", Long.parseLong(identifier))

            propertiesMap.put(T.label, entityName);

            List<Object> keyValues = new ArrayList<>();
            propertiesMap.forEach { key, val ->
                keyValues.add(key);
                keyValues.add(val);
            }

            Vertex vertex = graph.addVertex(keyValues.toArray());

            return vertex;
        }


        void snbLoader() {
            SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            SimpleDateFormat creationDateDateFormat =
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
            SimpleDateFormat joinDateDateFormat =
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            joinDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            String[] colNames = graphReader.getColNames()

            String edgeLabel = "knows"

            boolean txSucceeded;
            long txFailCount;

            while( graphReader.hasNext() ) {
                List<String> lines = graphReader.next()
                if(lines == null) {
                    // keep polling until queue is empty
                    continue
                }
                txSucceeded = false;
                txFailCount = 0;
                while (!txSucceeded) {
                    for (int i = 0; i < lines.size(); i++) {
                        try {
                            String line = lines.get(i);
                            String[] colVals = line.split("\\|");

                            if (elementType == ElementType.VERTEX) {
                                Map<Object, Object> propertiesMap = new HashMap<>();

                                String identifier;
                                for (int j = 0; j < colVals.length; ++j) {
                                    if (colNames[j].equals("id")) {
                                        identifier = colVals[j]
                                    }
                                }

                                Vertex vertex = addVertex(identifier)

                                //populate IDMapping if enabled
                                Long id = (Long) vertex.id()
                                idMapping.put(identifier, id)

                            } else if (elementType == ElementType.PROPERTY) {
                                GraphTraversalSource g = graph.traversal();

                                Vertex vertex = null;
                                Long id = idMapping.get(colVals[0]);
                                vertex = g.V(id).next()

                                for (int j = 1; j < colVals.length; ++j) {
                                    vertex.property(VertexProperty.Cardinality.list, colNames[j],
                                            colVals[j]);
                                }

                            } else {
                                GraphTraversalSource g = graph.traversal();
                                Vertex vertex1, vertex2;

                                Long id1 = idMapping.get(colVals[0])
                                Long id2 = idMapping.get(colVals[1])
                                vertex1 = g.V(id1).next()
                                vertex2 = g.V(id2).next()

                                List<Object> keyValues = new ArrayList<>();

                                vertex1.addEdge(edgeLabel, vertex2, keyValues.toArray());

                                if (edgeLabel.equals("knows")) {
                                    //TODO: this is implementation spefic, parameterize this
                                    vertex2.addEdge(edgeLabel, vertex1, keyValues.toArray());
                                }
                            }

                            this.counter.incrementAndGet()
                        } catch (Exception e) {
                            println(String.format("Inset failed on file: %s, index: %s, reason: ", graphReader.getFileName(), i, e.printStackTrace()))
                        }
                    }
                    try {
                        graph.tx().commit();
                        txSucceeded = true;
                    } catch (Exception e) {
                        txFailCount++;
                        println("failed")
                    }

                    if (txFailCount > TX_MAX_RETRIES) {
                        throw new RuntimeException(String.format(
                                "ERROR: Transaction failed %d times (file lines [%d,%d]), " +
                                        "aborting...", txFailCount, startIndex, endIndex - 1));
                    }
                }
            }
            // means that no more lines to return
            return
        }

        void adjLoader() {
            SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            SimpleDateFormat creationDateDateFormat =
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
            SimpleDateFormat joinDateDateFormat =
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            joinDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            // WARN default entity name we use to make existing SNB queries compatible with twitter and us and uk datasets
            String entityName = "person"
            String edgeLabel = "knows"

            boolean txSucceeded;
            long txFailCount;
            long currentBatchSize

            while (graphReader.hasNext()) {
                List<String> lines = graphReader.next()
                if (lines == null) {
                    // keep polling until queue is empty
                    continue
                }
                txSucceeded = false;
                txFailCount = 0;
                while (!txSucceeded) {
                    for (int i = 0; i < lines.size(); i++) {
                        try {
                            String line = lines.get(i);
                            String[] colVals = line.split("\\s");

                            if (elementType == ElementType.VERTEX) {


                                String identifier = colVals[0];
                                Vertex vertex = addVertex(identifier)

                                Long id = (Long) vertex.id()
                                idMapping.put(identifier, id)

                            } else {
                                GraphTraversalSource g = graph.traversal();
                                DefaultGraphTraversal t;
                                Vertex source;
                                Long degree = Long.parseLong(colVals[1])
                                List<Vertex> neighbours = new ArrayList<>();

                                Long id1 = idMapping.get(colVals[0])
                                t = g.V(id1)
                                if(t.count() == 0) {
                                    source = addVertex(Long.toString(id1))
                                } else {
                                    source = g.V(id1).next()
                                }

                                for (int j = 0; j < degree; j++) {
									String identifier2 = colVals[j+2]
                                    Long id2 = idMapping.get(identifier2)
                                    Vertex vertex2

                                    if(id2 == null) {
                                        vertex2 = addVertex(identifier2)
										Long id = (Long) vertex2.id()
                                		idMapping.put(identifier2, id)
                                    } else {
                                        vertex2 = g.V(id2).next()
                                    }
                                    
                                    neighbours.add(vertex2)
                                }


                                List<Object> keyValues = new ArrayList<>();

                                for (Vertex neighbour : neighbours) {
                                    source.addEdge(edgeLabel, neighbour, keyValues.toArray());
                                    currentBatchSize++

                                    if(currentBatchSize >= 100000) {
                                        try {
                                            graph.tx().commit()
                                        } catch ( Exception e) {
                                            println(String.format("Edge insert failed on file: %s, index: %s, reason: ", graphReader.getFileName(), i, e.printStackTrace()))
                                        } finally {
                                            currentBatchSize = 0
                                        }
                                    }
                                }
                            }

                            this.counter.incrementAndGet()
                        } catch (Exception e) {
                            println(String.format("Insert failed on file: %s, index: %s, reason: ", graphReader.getFileName(), i, e.printStackTrace()))
                        }
                    }
                    try {
                        graph.tx().commit();
                        txSucceeded = true;
                    } catch (Exception e) {
                        txFailCount++;
                        println("failed")
                    }

                    if (txFailCount > TX_MAX_RETRIES) {
                        throw new RuntimeException(String.format(
                                "ERROR: Transaction failed %d times (file lines [%d,%d]), " +
                                        "aborting...", txFailCount, startIndex, endIndex - 1));
                    }
                }
            }
            // means that no more lines to return
        }

        @Override
        Object call() {
            try {
                if (isGraphSNB) {
                    snbLoader()
                } else {
                    adjLoader()
                }
            } catch(Exception e) {
                System.out.println("Graph Loader failed due to", e.printStackTrace())
            }
        }
    }

    static void loadGraph(JanusGraph graph, String configurationFile) throws IOException {

        // create labels `person` and `knows` and `iid` indexes
        createIndexes(graph)

        Configuration configuration = new PropertiesConfiguration(configurationFile);

        //import partition lookup
        partitionLookupImport(configuration)


        // Full paths for edge and vertex files
        File nodeFile;
        File edgeFile;

        // WARN, we use the same file for nodes and edges, for nodes we simply rely on first vertex id on each line
        String inputBase = configuration.getString("input.base")
        File inputBaseDir = FileUtils.getFile(inputBase)

        if(!inputBaseDir.exists()) {
            inputBaseDir = FileUtils.getFile(DATASET_VOLUME, inputBase)
        }

        if(!inputBaseDir.exists()) {
            System.out.println("Input graph does NOT exists: " + inputBaseDir)
        }

        // SNB graphs have seperate edge and vertex files
        boolean isGraphSNB = configuration.getBoolean("graph.snb")

        // snb graph has multiple files
        if(isGraphSNB) {
            nodeFile = FileUtils.getFile( inputBaseDir, "social_network","person_0_0.csv")
            edgeFile = FileUtils.getFile( inputBaseDir, "social_network", "person_knows_person_0_0.csv")
        } else {
            nodeFile = FileUtils.getFile( inputBaseDir, "adj.txt")
            edgeFile = FileUtils.getFile( inputBaseDir, "adj.txt")
        }

        //check validity of graph file
        if(!nodeFile.exists()) {
            System.out.println("Node file does NOT exists: " + nodeFile)
            return
        }

        if(!edgeFile.exists()) {
            System.out.println("Edge file does NOT exists: " + edgeFile)
            return
        }


        int threadCount = configuration.getInt("thread.count")
        int batchSize = configuration.getInt("batch.size")
        int progReportPeriod = configuration.getInt("reporting.period")

        String[] servers = configuration.getStringArray("memcached.address")
        ConcurrentHashMap<String, Long> idMapping = new ConcurrentHashMap<String, Long>();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount)

        try {
            SharedGraphReader graphReader = new SharedGraphReader(Paths.get(nodeFile.getAbsolutePath()), batchSize, progReportPeriod, isGraphSNB)
            List<ADJGraphLoader> tasks = new ArrayList<ADJGraphLoader>()
            for (int i = 0; i < threadCount; i++) {
                tasks.add(new ADJGraphLoader(graph, graphReader, ElementType.VERTEX, idMapping, isGraphSNB))
            }
            graphReader.start()
            executor.invokeAll(tasks)
            graphReader.stop()


            graphReader = new SharedGraphReader(Paths.get(edgeFile.getAbsolutePath()), batchSize, progReportPeriod, isGraphSNB)
            tasks = new ArrayList<ADJGraphLoader>()
            for (int i = 0; i < threadCount; i++) {
                tasks.add(new ADJGraphLoader(graph, graphReader, ElementType.EDGE, idMapping, isGraphSNB))
            }
            graphReader.start()
            executor.invokeAll(tasks)
            graphReader.stop()


        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        } finally {
            graph.close();
        }
    }


	static void partitionLookupImport(String configurationFile) {
		Configuration configuration = new PropertiesConfiguration(configurationFile);
		partitionLookupImport(configuration);
	}

    static void partitionLookupImport(Configuration configuration) {
        String entityPrefix = "person:";

        String partitionLookup = configuration.getString("partition.lookup")
        File lookupFile = FileUtils.getFile(partitionLookup)
        if(!lookupFile.exists()) {
            // it is a relative path
            lookupFile = FileUtils.getFile(DATASET_VOLUME, partitionLookup)
        }

        if(!lookupFile.exists()) {
            //there is no partition lookup file, notify user
            System.out.println("Partition lookup file CANNOT be found: " + partitionLookup)
        }
        String[] servers = configuration.getStringArray("memcached.address")
        int batchSize = configuration.getInt("batch.size")
        int threadCount = configuration.getInt("thread.count")

        AtomicLong counter = new AtomicLong(0)

        ExecutorService executor = Executors.newFixedThreadPool(threadCount)

        List<String> partitionFiles = new ArrayList<>()
        if(lookupFile.isFile()) {
            // lookupFile is already pointing out the exact file
            partitionFiles.add("")
        } else {
            partitionFiles.addAll(Arrays.asList(lookupFile.list()))
        }

        List<Callable> partitionImporters = new ArrayList<>();

        for(String partitionFile : partitionFiles) {
            partitionImporters.add(new Callable() {
                @Override
                Object call() throws Exception {
                    try {
						MemcachedMap<String, Integer> partitionMappingServer = new MemcachedMap<String, Integer>("partition-lookup", servers)
                        LineIterator it = FileUtils.lineIterator(FileUtils.getFile(lookupFile.toString(), partitionFile), "UTF-8")
                        System.out.println("Start processing partition lookup file: " + partitionFile)
                        while (it.hasNext()) {
                            String[] parts = it.nextLine().split(",|\\s")
                            String id = parts[0]
                            Integer partition = Integer.valueOf(parts[1])

                            if(id.startsWith(entityPrefix)) {
                                partitionMappingServer(id, partition)
                            } else {
                                partitionMappingServer.set(entityPrefix + id, partition)
                            }


                            if (counter.incrementAndGet() % batchSize == 0) {
                                System.out.println("Imported: " + counter.get())
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Exception: " + e);
                        e.printStackTrace();
                    }
                }
            })
        }

        executor.invokeAll(partitionImporters)
    }

    /**
     * Helper method to create necessary index. It creates person and knows labels then indexes on iid property
     * @param janusGraph
     */
    static void createIndexes(JanusGraph janusGraph) {
        ManagementSystem mgmt;

        // index
        mgmt = (ManagementSystem) janusGraph.openManagement();
        if (mgmt.getVertexLabel("person") == null) {
            mgmt.makeVertexLabel("person").make();
        }
        mgmt.commit();

        mgmt = (ManagementSystem) janusGraph.openManagement();
        if (mgmt.getEdgeLabel("knows") == null) {
            mgmt.makeEdgeLabel("knows").make();
        }
        mgmt.commit();

        System.out.println("Labels are created")

        // creationDate
        mgmt = (ManagementSystem) janusGraph.openManagement();
        if (mgmt.getPropertyKey('creationDate') == null) {
            mgmt.makePropertyKey("creationDate").dataType(Long.class)
                    .cardinality(Cardinality.SINGLE).make();
        }
        mgmt.commit();

        // indexing iid and id_long properties
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

        System.out.println("Indices are created")
    }

    static class MemcachedMap<E, T> {
        private MemCachedClient client;

        public MemcachedMap(String instanceName, String... servers) {
            SockIOPool pool = SockIOPool.getInstance(instanceName);
            pool.setServers(servers);
            pool.setFailover(true);
            pool.setInitConn(10);
            pool.setMinConn(5);
            pool.setMaxConn(250);
            pool.setMaintSleep(30);
            pool.setNagle(false);
            pool.setSocketTO(3000);
            pool.setAliveCheck(true);
            pool.initialize();

            client = new MemCachedClient(instanceName);
            // client.flushAll();
        }

        public void set(E identifier, T id) {
            client.set(identifier, id)
        }

        public T get(E identifier) {
            Object value = client.get(identifier);
            if (value == null)
                return null;
            return (T) value;
        }

		public HashMap<E,T> getMulti(E[] identifiers) {
			HashMap<E,Object> values = (HashMap<E,Object>) client.getMulti(identifiers);
			HashMap<E,T> results = new HashMap<E,T>();
			
			for(Map.Entry<E, Object> e: results.entrySet()) {
				Object value = e.getValue();
				if(value == null) {
					results.put(e.getKey(), null)
				} else {
					results.put(e.getKey(), (T)value);
				}
			}
			return results;
		}
    }
}

System.out.println("Populate partitioning info using configuration: " + args[0]);
janusGraph = JanusGraphFactory.open('/sgp/scripts/conf/janusgraph-cassandra-es-server.properties')
ADJParser.loadGraph(janusGraph, args[0]);
System.out.println("Partitioning info is complete!");
System.exit(0);

