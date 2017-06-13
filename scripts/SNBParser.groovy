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


import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.io.LineIterator
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty

import java.nio.file.Path
import java.nio.file.Paths
import java.text.SimpleDateFormat

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong;

import java.util.Timer
import java.util.TimerTask

/**
 * This is a Groovy Script to run inside gremlin console, for loading LDBC SNB data into Tinkerpop Competible Graph.
 * original written by Jonathan Ellithorpe <jde@cs.stanford.edu> <a href="https://github.com/PlatformLab/ldbc-snb-impls/blob/master/snb-interactive-titan/src/main/java/net/ellitron/ldbcsnbimpls/interactive/titan/TitanGraphLoader.java">TitanGraphLoader </>
 *
 * @author Anil Pacaci <apacaci@uwaterloo.ca>
 */
class SNBParser {

    static TX_MAX_RETRIES = 1000

    static isIdMappingEnabled = false

    static IDMapping idMappingServer = null

    static class SharedGraphReader {

        int batchSize
        LineIterator it
        String[] fileNameParts
        String[] colNames
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
            this.fileNameParts = fileName.split("_")
            this.colNames = it.nextLine().split("\\|");
            this.lineCount = 0
            this.startIndex = 1

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
                    while(it.hasNext()) {
                        List<String> lines = new ArrayList<>(batchSize)
                        for(int i = 0 ; i < batchSize && it.hasNext() ; i++) {
                            lines.add (it.nextLine() )
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

        Graph graph
        SharedGraphReader graphReader
        ElementType elementType
        AtomicLong counter

        GraphLoader(Graph graph, SharedGraphReader graphReader, ElementType elementType) {
            this.graph = graph
            this.graphReader = graphReader
            this.elementType = elementType
            this.counter = graphReader.getCounter()
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
            String[] colNames = graphReader.getColNames()
            String[] fileNameParts = graphReader.getFileNameParts()
            String entityName = fileNameParts[0];

            String edgeLabel = elementType.equals(ElementType.EDGE) ? fileNameParts[1] : null
            String v2EntityName = elementType.equals(ElementType.EDGE) ? fileNameParts[2] : null

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
                                        identifier = entityName + ":" + colVals[j]
                                        propertiesMap.put("iid", identifier);
                                        propertiesMap.put("iid_long", Long.parseLong(colVals[j]))
                                    } else if (colNames[j].equals("birthday")) {
                                        propertiesMap.put(colNames[j], birthdayDateFormat.parse(colVals[j]).getTime());
                                    } else if (colNames[j].equals("creationDate")) {
                                        propertiesMap.put(colNames[j], creationDateDateFormat.parse(colVals[j]).getTime());
                                    } else {
                                        propertiesMap.put(colNames[j], colVals[j]);
                                    }
                                }
                                propertiesMap.put(T.label, entityName);

                                List<Object> keyValues = new ArrayList<>();
                                propertiesMap.forEach { key, val ->
                                    keyValues.add(key);
                                    keyValues.add(val);
                                }

                                Vertex vertex = graph.addVertex(keyValues.toArray());

                                //populate IDMapping if enabled
                                if (isIdMappingEnabled) {
                                    Long id = (Long) vertex.id()
                                    idMappingServer.setId(identifier, id)
                                }

                            } else if (elementType == ElementType.PROPERTY) {
                                GraphTraversalSource g = graph.traversal();

                                Vertex vertex = null;
                                if (isIdMappingEnabled) {
                                    Long id = idMappingServer.getId(entityName + ":" + colVals[0]);
                                    vertex = g.V(id).next()
                                } else {
                                    vertex = g.V().has(entityName, "iid", entityName + ":" + colVals[0]).next();
                                }

                                for (int j = 1; j < colVals.length; ++j) {
                                    vertex.property(VertexProperty.Cardinality.list, colNames[j],
                                            colVals[j]);
                                }

                            } else {
                                GraphTraversalSource g = graph.traversal();
                                Vertex vertex1, vertex2;

                                if (isIdMappingEnabled) {
                                    Long id1 = idMappingServer.getId(entityName + ":" + colVals[0])
                                    Long id2 = idMappingServer.getId(v2EntityName + ":" + colVals[1])
                                    vertex1 = g.V(id1).next()
                                    vertex2 = g.V(id2).next()
                                } else {
                                    vertex1 =
                                            g.V().has(entityName, "iid", entityName + ":" + colVals[0]).next();
                                    vertex2 =
                                            g.V().has(v2EntityName, "iid", v2EntityName + ":" + colVals[1]).next();
                                }

                                Map<Object, Object> propertiesMap = new HashMap<>();
                                for (int j = 2; j < colVals.length; ++j) {
                                    if (colNames[j].equals("creationDate")) {
                                        propertiesMap.put(colNames[j], creationDateDateFormat.parse(colVals[j]).getTime());
                                    } else if (colNames[j].equals("joinDate")) {
                                        propertiesMap.put(colNames[j], joinDateDateFormat.parse(colVals[j]).getTime());
                                    } else {
                                        propertiesMap.put(colNames[j], colVals[j]);
                                    }
                                }

                                List<Object> keyValues = new ArrayList<>();
                                propertiesMap.forEach { key, val ->
                                    keyValues.add(key);
                                    keyValues.add(val);
                                }

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
    }

    static void loadSNBGraph(Graph graph, String configurationFile) throws IOException {

        Configuration configuration = new PropertiesConfiguration(configurationFile);

        String[] nodeFiles = configuration.getStringArray("nodes")
        String[] propertiesFiles = configuration.getStringArray("properties")
        String[] edgeFiles = configuration.getStringArray("edges")

        String inputBaseDir = configuration.getString("input.base")

        int threadCount = configuration.getInt("thread.count")
        int batchSize = configuration.getInt("batch.size")
        int progReportPeriod = configuration.getInt("reporting.period")

        isIdMappingEnabled = configuration.getBoolean("id.mapping")

        if(isIdMappingEnabled) {
            String[] servers = configuration.getStringArray("memcached.address")
            idMappingServer = new IDMapping(servers)
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount)

        try {
            for (String fileName : nodeFiles) {
                SharedGraphReader graphReader = new SharedGraphReader(Paths.get(inputBaseDir, fileName), batchSize, progReportPeriod)
                List<GraphLoader> tasks = new ArrayList<GraphLoader>()
                for(int i = 0 ; i < threadCount ; i++) {
                    tasks.add(new GraphLoader(graph, graphReader, ElementType.VERTEX))
                }
                graphReader.start()
                executor.invokeAll(tasks)
                graphReader.stop()
            }


            for (String fileName : propertiesFiles) {
                SharedGraphReader graphReader = new SharedGraphReader(Paths.get(inputBaseDir, fileName), batchSize, progReportPeriod)
                List<GraphLoader> tasks = new ArrayList<GraphLoader>()
                for(int i = 0 ; i < threadCount ; i++) {
                    tasks.add(new GraphLoader(graph, graphReader, ElementType.PROPERTY))
                }
                graphReader.start()
                executor.invokeAll(tasks)
                graphReader.stop()
            }


            for (String fileName : edgeFiles) {
                SharedGraphReader graphReader = new SharedGraphReader(Paths.get(inputBaseDir, fileName), batchSize, progReportPeriod)
                List<GraphLoader> tasks = new ArrayList<GraphLoader>()
                for(int i = 0 ; i < threadCount ; i++) {
                    tasks.add(new GraphLoader(graph, graphReader, ElementType.EDGE))
                }
                graphReader.start()
                executor.invokeAll(tasks)
                graphReader.stop()
            }


        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        } finally {
            graph.close();
        }
    }

    static class IDMapping {

        private static String INSTANCE_NAME = "id-mapping";

        private MemCachedClient client;

        public IDMapping(String... servers) {
            SockIOPool pool = SockIOPool.getInstance(INSTANCE_NAME);
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

            client = new MemCachedClient(INSTANCE_NAME);
            client.flushAll();
        }

        public Long getId(String identifier) {
            Object value = client.get(identifier);
            if (value == null)
                return null;
            return (Long) value;
        }

        public void setId(String identifier, Long id) {
            client.set(identifier, id)
        }
    }

    enum ElementType {VERTEX, PROPERTY, EDGE}

}
