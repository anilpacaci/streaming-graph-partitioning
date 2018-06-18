package ca.uwaterloo.cs.sgp.streaming.parser;

import java.util.List;

/**
 * Created by anilpacaci on 2018-06-18.
 */
public interface LineParser {
    /**
     * Extract source vertex id from given line
     * @param line
     * @return
     */
    String getSource(String line);

    /**
     * Extract degree from given line
     * @param line
     * @return
     */
    Integer getDegree(String line);

    /**
     * Extract list of neighbours in OUT direction
     * @param line
     * @return
     */
    List<String> getOutEdges(String line);

    /**
     * Extract list of neighbours in IN direction
     * @param line
     * @return
     */
    List<String> getInEdges(String line);
}
