package ca.uwaterloo.cs.sgp.streaming.parser;

import sun.java2d.loops.ProcessPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by anilpacaci on 2018-06-18.
 */
public class SNBParser implements LineParser {

    public String REGEXP_EDGE_SEPERATOR = "\\s+";
    public String REGEXP_VERTEX_SEPERATOR = "\\|";
    public String REGEXP_PROPERTY_SEPERATOR = ",";

    private List<String> labels;

    public SNBParser(String[] labels) {
        this.labels = Arrays.asList(labels);
    }

    public String getSource(String line) {
        return line.split(REGEXP_VERTEX_SEPERATOR)[0];
    }
    public Integer getDegree(String line) {
        int outDegree = getOutEdges(line).size();
        int inDegree = getInEdges(line).size();
        return outDegree + inDegree;
    }

    public List<String> getOutEdges(String line) {
        String[] splitLine = line.split(REGEXP_VERTEX_SEPERATOR, -1);
        if(splitLine.length >= 2 && !splitLine[1].isEmpty()) {
            List<String> outEdges = new ArrayList<>();
            String[] edges = (splitLine[1].split(REGEXP_EDGE_SEPERATOR));
            for(String edge : edges) {
                String[] properties = edge.split(REGEXP_PROPERTY_SEPERATOR);
                if(labels.contains(properties[0])) {
                    outEdges.add(properties[1]);
                }
            }
            return outEdges;
        } else {
            return new ArrayList<>();
        }
    }

    public List<String> getInEdges(String line) {
        String[] splitLine = line.split(REGEXP_VERTEX_SEPERATOR, -1);
        if(splitLine.length >= 3 && !splitLine[2].isEmpty()) {
            List<String> outEdges = new ArrayList<>();
            String[] edges = (splitLine[2].split(REGEXP_EDGE_SEPERATOR));
            for(String edge : edges) {
                String[] properties = edge.split(REGEXP_PROPERTY_SEPERATOR);
                if(labels.contains(properties[0])) {
                    outEdges.add(properties[1]);
                }
            }
            return outEdges;
        } else {
            return new ArrayList<>();
        }
    }
}
