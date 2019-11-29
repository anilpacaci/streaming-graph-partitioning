package ca.uwaterloo.cs.sgp.streaming;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.*;
import java.lang.*;
import java.io.FileNotFoundException;

public class EdgeCutSGP {
    private int size_of_graph;
    private int capacity;
    private int numberOfPartitions;
    private double slack;
    private double gamma;
    private double alpha;
    private int[] partitionSizes;

    private long numberOfEdges = 0;
    private long numberOfEdgecut = 0;

    private HashSet<String> edgeLabels = new HashSet<>();

    public HashMap<String, Integer> vertex_to_partition;

    LineIterator lineIterator;
    
    public void setGamma(double gamma){
    	this.gamma = gamma;
    }

    public void setAlpha(double alpha){
        this.alpha = alpha;
    }

    // Constructor of the LDG class, it creates the graph, set up the
    // neighbor relationships for each vertex, calculate the capacity
    // of each partition, and set the size of each partition to 0 by
    // default
    public EdgeCutSGP(String file, int k, int numberOfVertices, Double slack, String[] edgeLabels){
        // Read all lines of the file to get the size of graph
        try{
            lineIterator = FileUtils.lineIterator(FileUtils.getFile(file), "UTF-8");
            this.numberOfPartitions = k;
            this.size_of_graph = numberOfVertices;
            this.slack = slack;
            this.capacity = (int) (  ( this.size_of_graph / k ) * (1 + slack) );
            // add all the given labels into set of permitted labels
            Arrays.stream(edgeLabels).forEach(label -> this.edgeLabels.add(label));

        }  catch(FileNotFoundException e){
            System.out.println("Dataset file not found: " +  file);
        }  catch(IOException e){
            e.printStackTrace();
        }
        // Array for partitionSizes, values reveal the current size of
        // each partition
        partitionSizes = new int[k];
        for(int i = 0; i < k; i++){
            partitionSizes[i] = 0;
        }

        vertex_to_partition = new HashMap<>(numberOfVertices);
    }

    List<String> LineParser(String[] Edges){
        List<String> result = new ArrayList<>();
        int index = 0;
        for(String next : Edges){
            if( next.isEmpty())
                continue;

            String[] temp = next.split(",");
            // if user specified set of labels and given label is not in the set, skip this edge
            if(!this.edgeLabels.isEmpty() && !this.edgeLabels.contains(temp[0]))
                continue;

            String composed_id = temp[1];
            result.add( composed_id );
            index++;
        }
        return result;
    }
    // Count the number of neighbors of V which are partitioned to
    // partition i
    int neighbors_in_partition(int i, List<String> edgeList){
        if(edgeList.size() == 0) return 0;
        int count = 0;
        // Iterate through the LinkedList until reach the end
        for(String nextNeighbour : edgeList){
            int next_partition = vertex_to_partition.getOrDefault(nextNeighbour, -1);
            // if the neighbor is partitioned to i-partition already,
            // increment the counter
            if(i == next_partition){
                count++;
            }
        }
        return count;
    }

    int hash_partition(String vertexID, List<String> edgeList) {
        int[] numberOfNeighbours = new int[this.numberOfPartitions];
        int argmax = -1;

        LinkedList<Integer> TieBreaker = new LinkedList<Integer>();
        for(int i = 0; i < this.numberOfPartitions; i++){
            numberOfNeighbours[i] = neighbors_in_partition(i, edgeList);
            TieBreaker.add(i);
        }

       argmax = vertexID.hashCode() % this.numberOfPartitions;

        // compute the edge cut
        for(int i = 0 ; i < this.numberOfPartitions ; i++) {
            this.numberOfEdges += numberOfNeighbours[i];
            if(i != argmax) {
                this.numberOfEdgecut += numberOfNeighbours[i];
            }
        }

        return argmax;
    }

    // Choose the partition for next vertex based on the formula in LDG
    int ldg_partition(String vertexID, List<String> edgeList){
        double result = -1;
        int argmax = -1;
        int[] numberOfNeighbours = new int[this.numberOfPartitions];
        //loop through all partitionSizes
        LinkedList<Integer> TieBreaker = new LinkedList<Integer>();
        for(int i = 0; i < this.numberOfPartitions; i++){
            numberOfNeighbours[i] = neighbors_in_partition(i, edgeList);
            // calculate the formulated value for each partition
            double next = (1 - (partitionSizes[i] / capacity)) * numberOfNeighbours[i];
            if(next > result){
                if(partitionSizes[i] < capacity){
                    result = next;
                    argmax = i;
                    TieBreaker.clear();
                    TieBreaker.add(argmax);
                }
            }
            if(next == result){
                TieBreaker.add(i);
            }
        }
        Random rand = new Random();
        int value = rand.nextInt(TieBreaker.size());
        argmax = TieBreaker.get(value);

        // compute the edge cut
        for(int i = 0 ; i < this.numberOfPartitions ; i++) {
            this.numberOfEdges += numberOfNeighbours[i];
            if(i != argmax) {
                this.numberOfEdgecut += numberOfNeighbours[i];
            }
        }

        return argmax;
    }
    
    int fennel_partition(String vertexID, List<String> edgeList){
    	double result = Integer.MIN_VALUE;
        int argmax = -1;
        int[] numberOfNeighbours = new int[this.numberOfPartitions];
        //loop through all partitionSizes
        LinkedList<Integer> TieBreaker = new LinkedList<Integer>();
        for(int i = 0; i < this.numberOfPartitions; i++){
            numberOfNeighbours[i] = neighbors_in_partition(i, edgeList);
            // calculate the formulated value for each partition
            double next = numberOfNeighbours[i]
            		- (gamma * alpha * Math.pow(partitionSizes[i], gamma - 1));
            if(next > result){
                if(partitionSizes[i] < capacity){
                    result = next;
                    argmax = i;
                    TieBreaker.clear();
                    TieBreaker.add(argmax);
                }
            }
            else if (next == result) {
                TieBreaker.add(i);
            }
        }
        Random rand = new Random();
        int value = rand.nextInt(TieBreaker.size());
        argmax = TieBreaker.get(value);

        // compute the edge cut
        for(int i = 0 ; i < this.numberOfPartitions ; i++) {
            this.numberOfEdges += numberOfNeighbours[i];
            if(i != argmax) {
                this.numberOfEdgecut += numberOfNeighbours[i];
            }
        }

        return argmax;
    }

    public void streamingPartition( boolean undirect, String algorithm){
            //loop through all lines in the file
            while(lineIterator.hasNext()){
                String next = lineIterator.next();
                //split each line by whitespace
                if(next.startsWith("#")) continue; // skip first line
                String[] splitLine = next.split("\\|", -1);
                String vertexIdentifier = splitLine[0];
                String outEdges = splitLine[1];
                String inEdges = splitLine[2];

                // System.out.println(VertexString);

                List<String> outgoingEdges = LineParser(outEdges.split("\\s+"));
                List<String> incomingEdges = LineParser(inEdges.split("\\s+"));

                int next_partition;
                //if the graph is treated as undirected, take incoming edges into account
                if(undirect ) {
                    List<String> combinedEdges = ListUtils.union(outgoingEdges, incomingEdges);
                    if(algorithm.equals("ldg")){
                    	next_partition = ldg_partition(vertexIdentifier, combinedEdges);
                    } else if (algorithm.equals("hash")) {
                        next_partition = hash_partition(vertexIdentifier, combinedEdges);
                    }
                    else{
                    	next_partition = fennel_partition(vertexIdentifier, combinedEdges);
                    }
                }
                //otherwise, only consider outgoing edges
                else {
                	if(algorithm.equals("ldg")){
                		next_partition = ldg_partition(vertexIdentifier, outgoingEdges);
                	} else if(algorithm.equals("hash")){
                        next_partition = hash_partition(vertexIdentifier, outgoingEdges);
                    }
                	else{
                		next_partition = fennel_partition(vertexIdentifier, outgoingEdges);
                	}
                }

                vertex_to_partition.put(vertexIdentifier, next_partition);
                partitionSizes[next_partition]++;
            }

    }
    public void print(String outputFile){
        try {
            PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
            Iterator<Map.Entry<String, Integer>> it = vertex_to_partition.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<String, Integer> entry = it.next();
                writer.println(entry.getKey() + "," + entry.getValue());
            }
            writer.close();
            System.out.println("# of edges:\t" + this.numberOfEdges);
            System.out.println("# of edgecut:\t" + this.numberOfEdgecut);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
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

        String inputFile = Paths.get(config.getString("sgp.inputfile") , "adj.txt").toString();
        String[] edgeLabels = config.getStringArray("sgp.edgelabels");
        Integer numberOfVertices = config.getInt("sgp.vertexcount");
        Integer numberOfEdges = config.getInt("sgp.edgecount");
        Boolean undirect = config.getBoolean("sgp.undirected");
        Double balanceSlack = config.getDouble("sgp.balanceslack");
        Double gamma = config.getDouble("sgp.fennel.gamma");

        int[] numberOfPartitions = new int[]{4, 8, 16};
        String[] algorithms = new String[]{"hash", "ldg", "fennel"};

        for(Integer partition : numberOfPartitions) {
            for(String algorithm : algorithms) {
                System.out.println("Partitioning  " + algorithm + " with partition count " + partition);

                EdgeCutSGP pa = new EdgeCutSGP(inputFile, partition, numberOfVertices, balanceSlack, edgeLabels);

                pa.setGamma(gamma);

                Double alpha = Math.sqrt(partition) * numberOfEdges / Math.pow(numberOfVertices, 1.5);
                pa.setAlpha(alpha);

                pa.streamingPartition(undirect, algorithm);

                String outputFile = Paths.get(inputFile, algorithm + "-" + partition + ".txt").toString();
                pa.print(outputFile);
                System.out.println("Partitioning information is written into: " + outputFile);
            }
        }

    }
}