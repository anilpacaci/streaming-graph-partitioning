package ca.uwaterloo.cs.sgp.streaming.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by anilpacaci on 2018-06-18.
 */
public class SNBParser implements LineParser {

    public String REGEXP_EDGE_SEPERATOR = "\\s+";
    public String REGEXP_VERTEX_SEPERATOR = "\\|";

    public String getSource(String line) {
        return line.split(REGEXP_VERTEX_SEPERATOR)[0];
    }
    public Integer getDegree(String line) {
        String[] splitLine = line.split(REGEXP_VERTEX_SEPERATOR, -1);
        int outDegree = getOutEdges(line).size();
        int inDegree = getInEdges(line).size();
        return outDegree + inDegree;
    }

    public List<String> getOutEdges(String line) {
        String[] splitLine = line.split(REGEXP_VERTEX_SEPERATOR, -1);
        if(splitLine.length >= 2 && !splitLine[1].isEmpty()) {
            return Arrays.asList(splitLine[1].split(REGEXP_EDGE_SEPERATOR));
        } else {
            return new ArrayList<>();
        }
    }

    public List<String> getInEdges(String line) {
        String[] splitLine = line.split(REGEXP_VERTEX_SEPERATOR, -1);
        if(splitLine.length >= 3 && !splitLine[2].isEmpty()) {
            return Arrays.asList(splitLine[2].split(REGEXP_EDGE_SEPERATOR));
        } else {
            return new ArrayList<>();
        }
    }
}
