package ca.uwaterloo.cs.sgp.streaming.zhuoran;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.FileNotFoundException;

public class LDG {
    private int size_of_graph;
    private int capacity;
    private int numberOfPartitions;
    private double slack;
    private int[] partitionSizes;

    private long numberOfEdges = 0;
    private long numberOfEdgecut = 0;

    public HashMap<String, Integer> vertex_to_partition;

    LineIterator lineIterator;

    // Constructor of the LDG class, it creates the graph, set up the
    // neighbor relationships for each vertex, calculate the capacity
    // of each partition, and set the size of each partition to 0 by
    // default
    public LDG(String file, int k, double balance_slack, int numberOfVertices){
        // Read all lines of the file to get the size of graph
        try{
            lineIterator = FileUtils.lineIterator(FileUtils.getFile(file), "UTF-8");
            this.numberOfPartitions = k;
            this.slack = balance_slack;

        }  catch(FileNotFoundException e){
            System.out.println("DataSet file not found.");
        }  catch(IOException e){
            e.printStackTrace();
        }
        // Array for partitionSizes, values reveal the current size of
        // each partition
        partitionSizes = new int[k];
        for(int i = 0; i < k; i++){
            partitionSizes[i] = 0;
        }
        //capacity for each partition
        double exact_capacity = (numberOfVertices / k) * (1+slack);
        capacity = (int)exact_capacity;

        vertex_to_partition = new HashMap<>(numberOfVertices);
    }

    String[] LineParser(String[] Edges){
        String[] result = Arrays.copyOf(Edges, Edges.length);
        int index = 0;
        for(String next : Edges){
            if( next.isEmpty())
                continue;

            String[] temp = next.split(",");
            String composed_id = temp[1];
            result[index] = composed_id;
            index++;
        }
        return result;
    }
    // Count the number of neighbors of V which are partitioned to
    // partition i
    int neighbors_in_partition(int i, String[] edgeList){
        if(edgeList.length == 0) return 0;
        int count = 0;
        // Iterate through the LinkedList until reach the end
        for(int j = 0; j < edgeList.length; j++ ){
            String nextNeighbour = edgeList[j];
            int next_partition = vertex_to_partition.getOrDefault(nextNeighbour, -1);
            // if the neighbor is partitioned to i-partition already,
            // increment the counter
            if(i == next_partition){
                count++;
            }
        }
        return count;
    }
    // Choose the partition for next vertex based on the formula in LDG
    int choose_partition(String vertexID, String[] edgeList){
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
                if(partitionSizes[i] != capacity){
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

    public void LDG_partitioning( boolean undirect){
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

                String[] outgoingEdges = LineParser(outEdges.split("\\s+"));
                String[] incomingEdges = LineParser(inEdges.split("\\s+"));
                int inDegree = incomingEdges.length;
                int outDegree = outgoingEdges.length;

                int next_partition;
                //if the graph is treated as undirected, take incoming edges into account
                if(undirect ) {
                    String[] combinedEdges = ArrayUtils.addAll(outgoingEdges, incomingEdges);
                    next_partition = choose_partition(vertexIdentifier, combinedEdges);
                }
                //otherwise, only consider outgoing edges
                else {
                    next_partition = choose_partition(vertexIdentifier, outgoingEdges);
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
            System.out.println("# of edges:\t" + this.numberOfEdges);
            System.out.println("# of edgecut:\t" + this.numberOfEdgecut);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];
        Integer numberOfPartitions = Integer.parseInt(args[2]);
        Double balanceSlack = Double.parseDouble(args[3]);
        Integer numverOfVertices = Integer.parseInt(args[4]);

        LDG ldg = new LDG(inputFile, numberOfPartitions, balanceSlack, numverOfVertices);
        ldg.LDG_partitioning( true);

        ldg.print(outputFile);
    }
}