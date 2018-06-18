package ca.uwaterloo.cs.sgp.streaming.parser;

import javax.sound.sampled.Line;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by anilpacaci on 2018-06-18.
 */
public class ADJParser implements LineParser {
    public String REGEXP_EDGE_SEPERATOR = "\\s+";
    public String REGEXP_VERTEX_SEPERATOR = "\\s+";

    public String getSource(String line) {
        return line.split(REGEXP_VERTEX_SEPERATOR)[0];
    }
    public Integer getDegree(String line) {
        String[] splitLine = line.split(REGEXP_VERTEX_SEPERATOR, -1);
        return Integer.parseInt(splitLine[1]);
    }

    public List<String> getOutEdges(String line) {
        String[] splitLine = line.split(REGEXP_VERTEX_SEPERATOR, -1);
        if(splitLine.length >= 3 && !splitLine[2].isEmpty()) {
            return Arrays.asList(splitLine).subList(2, splitLine.length);
        } else {
            return new ArrayList<>();
        }
    }

    public List<String> getInEdges(String line) {
        // ADJParser only includes out edges
        return new ArrayList<>();
    }
}
