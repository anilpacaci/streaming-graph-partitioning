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
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.Cardinality
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.schema.SchemaAction
import org.janusgraph.graphdb.database.management.ManagementSystem

import java.nio.file.Path
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

import java.util.Timer
import java.util.TimerTask

/**
 * This is a Groovy Script to run inside gremlin console, for loading SNAP ADJ formatted data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
class ADJParser {

    static int TX_MAX_RETRIES = 1000

    enum ElementType { VERTEX, PROPERTY, EDGE }

    static class SharedGraphReader {

        int batchSize
        LineIterator it
        String fileName
        long lineCount
        long startIndex
        int reportingPeriod
        Timer timer
        Thread writerThread

        ArrayBlockingQueue<List<String>> batchQueue
        AtomicBoolean isExhausted
        AtomicLong counter

        SharedGraphReader(Path filePath, int batchSize, reportingPeriod) {
            this.batchSize = batchSize
            this.reportingPeriod = reportingPeriod

            this.it = FileUtils.lineIterator(filePath.toFile(), "UTF-8")
            this.fileName = filePath.getFileName().toString()
            this.lineCount = 0
            this.startIndex = 0

            this.counter = new AtomicLong(0)
            this.isExhausted = new AtomicBoolean(false)
            this.batchQueue = new ArrayBlockingQueue<>(100)

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

    static class GraphLoader implements Callable {

        JanusGraph graph
        SharedGraphReader graphReader
        ElementType elementType
        AtomicLong counter
        MemcachedMap<String, Long> idMapping

        GraphLoader(JanusGraph graph, SharedGraphReader graphReader, ElementType elementType, MemcachedMap<String, Long> idMapping) {
            this.graph = graph
            this.graphReader = graphReader
            this.elementType = elementType
            this.counter = graphReader.getCounter()
            this.idMapping = idMapping
        }

        @Override
        Object call() {
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
                                Map<Object, Object> propertiesMap = new HashMap<>();

                                String identifier = colVals[0];
                                propertiesMap.put("iid", entityName + ":" + identifier);
                                propertiesMap.put("iid_long", Long.parseLong(identifier))

                                propertiesMap.put(T.label, entityName);

                                List<Object> keyValues = new ArrayList<>();
                                propertiesMap.forEach { key, val ->
                                    keyValues.add(key);
                                    keyValues.add(val);
                                }

                                Vertex vertex = graph.addVertex(keyValues.toArray());

                                Long id = (Long) vertex.id()
                                idMapping.put(identifier, id)

                            } else {
                                GraphTraversalSource g = graph.traversal();
                                Vertex source;
                                Long degree = Long.parseLong(colVals[1])
                                List<Vertex> neighbours = new ArrayList<>();

                                Long id1 = idMapping.get(colVals[0])
                                source = g.V(id1).next()

                                for (int j = 0; j < degree; j++) {
                                    Long id2 = idMapping.get(colVals[j + 2])
                                    Vertex vertex2 = g.V(id2).next()
                                    neighbours.add(vertex2)
                                }


                                List<Object> keyValues = new ArrayList<>();

                                for (Vertex neighbour : neighbours) {
                                    source.addEdge(edgeLabel, neighbour, keyValues.toArray());
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
    }

    static void loadGraph(JanusGraph graph, String configurationFile) throws IOException {

        // create labels `person` and `knows` and `iid` indexes
        createIndexes(graph)

        Configuration configuration = new PropertiesConfiguration(configurationFile);

        //import partition lookup
        partitionLookupImport(configuration)

        // WARN, we use the same file for nodes and edges, for nodes we simply rely on first vertex id on each line
        String inputAdjFile = configuration.getString("input.base")

        int threadCount = configuration.getInt("thread.count")
        int batchSize = configuration.getInt("batch.size")
        int progReportPeriod = configuration.getInt("reporting.period")

        String[] servers = configuration.getStringArray("memcached.address")
        MemcachedMap<String, Long> idMapping = new MemcachedMap<String, Long>("id-mapping", servers);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount)

        try {
            SharedGraphReader graphReader = new SharedGraphReader(Paths.get(inputAdjFile), batchSize, progReportPeriod)
            List<GraphLoader> tasks = new ArrayList<GraphLoader>()
            for (int i = 0; i < threadCount; i++) {
                tasks.add(new GraphLoader(graph, graphReader, ElementType.VERTEX, idMapping))
            }
            graphReader.start()
            executor.invokeAll(tasks)
            graphReader.stop()


            graphReader = new SharedGraphReader(Paths.get(inputAdjFile), batchSize, progReportPeriod)
            tasks = new ArrayList<GraphLoader>()
            for (int i = 0; i < threadCount; i++) {
                tasks.add(new GraphLoader(graph, graphReader, ElementType.EDGE, idMapping))
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

    static void partitionLookupImport(Configuration configuration) {
        String entityPrefix = "person:";

        String lookupFile = configuration.getString("partition.lookup")
        String[] servers = configuration.getStringArray("memcached.address")
        MemcachedMap<String, Integer> partitionMappingServer = new MemcachedMap<String, Integer>("partition-lookup", servers)
        int batchSize = configuration.getInt("batch.size")

        try {
            LineIterator it = FileUtils.lineIterator(FileUtils.getFile(lookupFile), "UTF-8")
            System.out.println("Start processing partition lookup file: " + lookupFile)
            long counter = 0
            while (it.hasNext()) {
                String[] parts = it.nextLine().split("\\s")
                String id = parts[0]
                Integer partition = Integer.valueOf(parts[1])

                partitionMappingServer.set(entityPrefix + id, partition)
                counter++

                if (counter % batchSize == 0) {
                    System.out.println("Imported: " + counter)
                }
            }
            System.out.println("# of keys: " + counter)
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        }
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
    }
}
