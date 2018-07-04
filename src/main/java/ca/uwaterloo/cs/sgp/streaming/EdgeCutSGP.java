package ca.uwaterloo.cs.sgp.streaming;

import ca.uwaterloo.cs.sgp.streaming.parser.ADJParser;
import ca.uwaterloo.cs.sgp.streaming.parser.LineParser;
import ca.uwaterloo.cs.sgp.streaming.parser.SNBParser;
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
import java.util.*;
import java.lang.*;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeCutSGP {

    List<SGPAlgorithm> algorithms;
    LineParser lineParser;

    LineIterator lineIterator;
    Timer timer;

    // Constructor of the LDG class, it creates the graph, set up the
    // neighbor relationships for each vertex, calculate the capacity
    // of each partition, and set the size of each partition to 0 by
    // default
    public EdgeCutSGP(String file, List<SGPAlgorithm> algorithms, LineParser lineParser){
        // Read all lines of the file to get the size of graph
        try{
            lineIterator = FileUtils.lineIterator(FileUtils.getFile(file), "UTF-8");
        }  catch(FileNotFoundException e){
            System.out.println("DataSet file not found.");
        }  catch(IOException e){
            e.printStackTrace();
        }
        this.algorithms = algorithms;
        this.lineParser = lineParser;
 		this.timer = new Timer();
   }


    public void streamingPartition( boolean undirect){
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger minutes = new AtomicInteger(0);

        // start reporter
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                // set to zero after reporting
                int currentCount = counter.getAndSet(0);
                System.out.println(String.format("Second: %d\t- Throughput: %d", minutes.incrementAndGet(), currentCount));
            }
        }, 0, 60 * 1000);

        //loop through all lines in the file
        while(lineIterator.hasNext()){
            String next = lineIterator.next();
            //split each line by whitespace
            if(next.startsWith("#")) continue; // skip first line
            String vertexIdentifier = lineParser.getSource(next);
            List<String> outgoingEdges = lineParser.getOutEdges(next);
            List<String> incomingEdges = lineParser.getInEdges(next);
            Integer degree = lineParser.getDegree(next);

            List<String> adjacencyList = outgoingEdges;

            if(undirect) {
                // combine all edges for undirected graphs
                adjacencyList.addAll(incomingEdges);
            }

            for(SGPAlgorithm algorithm : algorithms) {
                algorithm.partitionVertex(vertexIdentifier, adjacencyList);
            }
			counter.incrementAndGet();
        }

        timer.cancel();

    }
    public void finalize(String outputFile){

        for(SGPAlgorithm algorithm : algorithms) {
            try {
                PrintWriter writer = new PrintWriter(outputFile + "-" + algorithm.getName());

                Iterator<Map.Entry<String, Integer>> it = algorithm.getPartitionMap().entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, Integer> entry = it.next();
                    writer.println(entry.getKey() + " " + entry.getValue());
                }
                writer.close();
                System.out.println("Edgecut for " + algorithm.getName() + "\t:" + algorithm.getEdgeCut() );

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
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

        String inputFile = config.getString("sgp.inputfile");
        String outputFile = config.getString("sgp.outputfile");
        String[] edgeLabels = config.getStringArray("sgp.edgelabels");
        Integer numberOfPartitions = config.getInt("sgp.partitioncount");
        Long numberOfVertices = config.getLong("sgp.vertexcount");
        Long numberOfEdges = config.getLong("sgp.edgecount");
        Boolean undirect = config.getBoolean("sgp.undirected");
        String parser = config.getString("sgp.parser");
        Double balanceSlack = config.getDouble("sgp.balanceslack");
        Double gamma = config.getDouble("sgp.fennel.gamma");

        List<SGPAlgorithm> algorithms = new ArrayList<>();
        algorithms.add(new SGPAlgorithm.FennelSGP(numberOfPartitions, numberOfVertices, numberOfEdges, balanceSlack, gamma));
        algorithms.add(new SGPAlgorithm.LDGSGP(numberOfPartitions, numberOfVertices, numberOfEdges, balanceSlack));
        algorithms.add(new SGPAlgorithm.RandomSGP(numberOfPartitions, numberOfVertices, numberOfEdges));

        LineParser lineParser;
        if(parser.toLowerCase().equals("snb")) {
            lineParser = new SNBParser(edgeLabels);
        } else {
            lineParser = new ADJParser();
        }

        EdgeCutSGP pa = new EdgeCutSGP(inputFile, algorithms, lineParser);

        // start of streaming partition
        pa.streamingPartition(undirect);

        pa.finalize(outputFile);
    }

}
