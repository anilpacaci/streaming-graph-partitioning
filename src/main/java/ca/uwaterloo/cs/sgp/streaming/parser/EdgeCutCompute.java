package ca.uwaterloo.cs.sgp.streaming.parser;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.FastMath;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by anilpacaci on 2018-06-25.
 * This class reads the partitioning results produced by {@link ADJParser}
 * and produces the edge-cut cost of computed partitioning
 */
public class EdgeCutCompute {

    public static void main(String[] args) throws InterruptedException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.properties().setFileName(args[0]));

        Configuration config = null;

        try {
            config = builder.getConfiguration();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        String graphFile = config.getString("sgp.inputfile");
        String partitionFilePrefix = config.getString("sgp.outputfile");
        Integer numberOfPartitions = config.getInt("sgp.partitioncount");
        Integer numberOfVertices = config.getInt("sgp.vertexcount");
        Long numberOfEdges = config.getLong("sgp.edgecount");
        Boolean undirect = config.getBoolean("sgp.undirected");
        String parser = config.getString("sgp.parser");

        // read partition file into memory
        List<PartitionLookupImporter> taskList = new ArrayList<>();
        PartitioningInformation hashInfo = new PartitioningInformation("HASH", numberOfPartitions, numberOfVertices);
        PartitioningInformation fennelInfo = new PartitioningInformation("FENNEL", numberOfPartitions, numberOfVertices);
        PartitioningInformation ldgInfo = new PartitioningInformation("LDG", numberOfPartitions, numberOfVertices);

        taskList.add(new PartitionLookupImporter(partitionFilePrefix, hashInfo));
        taskList.add(new PartitionLookupImporter(partitionFilePrefix, fennelInfo));
        taskList.add(new PartitionLookupImporter(partitionFilePrefix, ldgInfo));

        // start partition lookup importing in parallel
        ExecutorService executor = Executors.newFixedThreadPool(3);
        executor.invokeAll(taskList);

        // start reading the input file
        ADJParser lineParser = new ADJParser();

        try{
            LineIterator lineIterator = FileUtils.lineIterator(FileUtils.getFile(graphFile), "UTF-8");

            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                String vertex = lineParser.getSource(line);
                List<String> neighbours = lineParser.getOutEdges(line);

                hashInfo.processVertex(vertex, neighbours);
                fennelInfo.processVertex(vertex, neighbours);
                ldgInfo.processVertex(vertex, neighbours);
            }

            // processing file is over, now report results
            Arrays.asList(hashInfo, fennelInfo, ldgInfo).stream().forEach(u -> {
                System.out.println(u.getName() + "\t-> EdgeCount: " + u.getEdgeCount() + " EdgeCut: " + u.getEdgeCut() +
                        " LoadImbalance: " + u.getImbalance() + " AverageLoad" + u.getAverageLoad() + " RSD: " + u.getRSD());
            });


        }  catch(FileNotFoundException e){
            System.out.println("DataSet file not found.");
        }  catch(IOException e){
            e.printStackTrace();
        }
    }

    public static class PartitionLookupImporter implements Callable<Long> {
        private String partitionFilePrefix;
        private String name;
        private PartitioningInformation partitioningInformation;

        public PartitionLookupImporter(String partitionFilePrefix, PartitioningInformation partitioningInformation) {
            this.partitionFilePrefix = partitionFilePrefix;
            this.name = partitioningInformation.getName();
            this.partitioningInformation = partitioningInformation;
        }

        @Override
        public Long call() throws Exception {
            String lookupFile = partitionFilePrefix + name;
            int batchSize = 100000;
            long counter = 0;

            try {
                LineIterator it = FileUtils.lineIterator(FileUtils.getFile(lookupFile), "UTF-8");
                System.out.println("Start processing partition lookup file: " + lookupFile);
                while (it.hasNext()) {
                    String[] parts = it.nextLine().split("\\s");
                    String id = parts[0];
                    Integer partition = Integer.valueOf(parts[1]);

                    this.partitioningInformation.setPartition(id, partition);
                    counter++;

                    if (counter % batchSize == 0) {
                        System.out.println("Imported for " + this.name + ":\t" + counter);
                    }
                }
            } catch (Exception e) {
                System.out.println("Exception: " + e);
                e.printStackTrace();
            }

            return counter;
        }
    }

    public static class PartitioningInformation {
        private int numberOfPartitions;
        private int vertexCount;
        private long edgeCount;
        private double[] partitionCounts;
        private long edgeCut;
        private HashMap<String, Integer> partitionLookup;
        private String name;

        public PartitioningInformation(String name, int numberOfPartitions, int vertexCount) {
            this.name = name;
            this.numberOfPartitions = numberOfPartitions;
            this.vertexCount = vertexCount;
            this.edgeCount = 0;
            this.edgeCut = 0;
            this.partitionCounts = new double[this.numberOfPartitions];
            this.partitionLookup = new HashMap<String, Integer>(this.vertexCount);
        }

        public void setPartition(String vertex, int partition) {
            this.partitionLookup.put(vertex, partition);
        }

        public int getPartition(String vertex) {
            return this.partitionLookup.get(vertex);
        }

        public void processVertex(String vertex, List<String> adjacenyList) {
            int sourcePartition = this.partitionLookup.get(vertex);
            //update capacity array
            this.partitionCounts[sourcePartition]++;

            for(String neighbour : adjacenyList) {
                // increase edgecount for all edges
                this.edgeCount++;
                // by getting -1 by default, we guarentee that a v ertex does not appear on partition lookup will increase the edgecount
                int neighbourPartition = this.partitionLookup.getOrDefault(neighbour, -1);
                if(sourcePartition != neighbourPartition) {
                    // it means it is an edge cut
                    this.edgeCut++;
                }
            }
        }

        public String getName() {
            return this.name;
        }

        public long getEdgeCount() {
            return this.edgeCount;
        }

        public long getEdgeCut() {
            return this.edgeCut;
        }

        public double getAverageLoad() {
            DescriptiveStatistics ds = new DescriptiveStatistics(this.partitionCounts);
            return ds.getMean();
        }

        public double getImbalance() {
            DescriptiveStatistics ds = new DescriptiveStatistics(this.partitionCounts);
            return ds.getMax() / ds.getMean();
        }

        public double getRSD() {
            DescriptiveStatistics ds = new DescriptiveStatistics(this.partitionCounts);
            return ds.getStandardDeviation() / ds.getMean();
        }
    }

}
