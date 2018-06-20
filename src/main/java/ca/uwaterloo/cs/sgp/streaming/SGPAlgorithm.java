package ca.uwaterloo.cs.sgp.streaming;

import javax.rmi.CORBA.Tie;
import java.util.*;

/**
 * Created by anilpacaci on 2018-06-18.
 */
public abstract class SGPAlgorithm {

    protected Integer numberOfPartition;
    protected Long numberOfVertices;
    protected Long numberOfEdges;
    protected Long capacity;

    protected int[] partitionSize;

    protected HashMap<String, Integer> vertexToPartition;
    protected Long numberOfEdgeCut = 0L;

    protected Random rand = new Random();

    abstract void partitionVertex(String sourceId, List<String> adjacencyList);

    // Count the number of neighbors of V which are partitioned to
    // partition i
    int neighbors_in_partition(int i, List<String> edgeList) {
        if (edgeList.size() == 0) return 0;
        int count = 0;
        // Iterate through the LinkedList until reach the end
        for (String nextNeighbour : edgeList) {
            int next_partition = vertexToPartition.getOrDefault(nextNeighbour, -1);
            // if the neighbor is partitioned to i-partition already,
            // increment the counter
            if (i == next_partition) {
                count++;
            }
        }
        return count;
    }

    abstract String getName();

    public Long getEdgeCut() {
        return numberOfEdgeCut;
    }

    public HashMap<String, Integer> getPartitionMap() {
        return vertexToPartition;
    }

    public static class FennelSGP extends  SGPAlgorithm {
        private static String NAME = "FENNEL";

        private Double balanceSlack;
        private Double gamma;
        private Double alpha;

        public FennelSGP(Integer numberOfPartition, Long numberOfVertices, Long numberOfEdges, Double balanceSlack, Double gamma) {
            this.numberOfPartition = numberOfPartition;
            this.numberOfVertices = numberOfVertices;
            this.numberOfEdges = numberOfEdges;
            this.balanceSlack = balanceSlack;
            this.gamma = gamma;
            this.alpha = Math.sqrt(numberOfPartition) * numberOfEdges / Math.pow(numberOfVertices, 1.5);

            this.capacity =  (long) ( ( this.numberOfVertices / numberOfPartition ) * (1 + balanceSlack) );

            partitionSize = new int[numberOfPartition];
            vertexToPartition = new HashMap<String, Integer>(numberOfVertices.intValue());
        }


        @Override
        public void partitionVertex(String sourceId, List<String> adjacencyList) {
            double result = Integer.MIN_VALUE;
            int argmax = -1;
            int[] numberOfNeighbours = new int[this.numberOfPartition];
            //loop through all partitionSizes
            LinkedList<Integer> TieBreaker = new LinkedList<Integer>();
            for(int i = 0; i < this.numberOfPartition; i++){
                numberOfNeighbours[i] = neighbors_in_partition(i, adjacencyList);
                // calculate the formulated value for each partition
                double next = numberOfNeighbours[i]
                        - (gamma * alpha * Math.pow(partitionSize[i], gamma - 1));
                if(next > result){
                    if(partitionSize[i] < capacity){
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

            if(TieBreaker.isEmpty()) {
                argmax = rand.nextInt(this.numberOfPartition);
            } else {
                int value = rand.nextInt(TieBreaker.size());
                argmax = TieBreaker.get(value);
            }


            // compute the edge cut
            for(int i = 0 ; i < this.numberOfPartition ; i++) {
                this.numberOfEdges += numberOfNeighbours[i];
                if(i != argmax) {
                    this.numberOfEdgeCut += numberOfNeighbours[i];
                }
            }

            vertexToPartition.put(sourceId, argmax);
            partitionSize[argmax]++;
        }

        @Override
        String getName() {
            return NAME;
        }
    }

    public static class LDGSGP extends SGPAlgorithm {
        private static String NAME = "LDG";

        private Double balanceSlack;

        public LDGSGP(Integer numberOfPartition, Long numberOfVertices, Long numberOfEdges, Double balanceSlack) {
            this.numberOfPartition = numberOfPartition;
            this.numberOfVertices = numberOfVertices;
            this.numberOfEdges = numberOfEdges;
            this.balanceSlack = balanceSlack;

            this.capacity =  (long) ( ( this.numberOfVertices / numberOfPartition ) * (1 + balanceSlack) );

            partitionSize = new int[numberOfPartition];
            vertexToPartition = new HashMap<String, Integer>(numberOfVertices.intValue());
        }


        @Override
        public void partitionVertex(String sourceId, List<String> adjacencyList) {
            double result = -1;
            int argmax = -1;
            int[] numberOfNeighbours = new int[this.numberOfPartition];
            //loop through all partitionSizes
            LinkedList<Integer> TieBreaker = new LinkedList<Integer>();
            for(int i = 0; i < this.numberOfPartition; i++){
                numberOfNeighbours[i] = neighbors_in_partition(i, adjacencyList);
                // calculate the formulated value for each partition
                double next = (1 - (partitionSize[i] / capacity)) * numberOfNeighbours[i];
                if(next > result){
                    if(partitionSize[i] < capacity){
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

            if(TieBreaker.isEmpty()) {
                argmax = rand.nextInt(this.numberOfPartition);
            } else {
                int value = rand.nextInt(TieBreaker.size());
                argmax = TieBreaker.get(value);
            }

            // compute the edge cut
            for(int i = 0 ; i < this.numberOfPartition ; i++) {
                this.numberOfEdges += numberOfNeighbours[i];
                if(i != argmax) {
                    this.numberOfEdgeCut += numberOfNeighbours[i];
                }
            }

            vertexToPartition.put(sourceId, argmax);
            partitionSize[argmax]++;
        }

        @Override
        String getName() {
            return NAME;
        }
    }

    public static class RandomSGP extends SGPAlgorithm{
        private static String NAME = "HASH";

        public RandomSGP(Integer numberOfPartition, Long numberOfVertices, Long numberOfEdges) {
            this.numberOfPartition = numberOfPartition;
            this.numberOfVertices = numberOfVertices;
            this.numberOfEdges = numberOfEdges;

            partitionSize = new int[numberOfPartition];
            vertexToPartition = new HashMap<String, Integer>(numberOfVertices.intValue());
        }


        @Override
        public void partitionVertex(String sourceId, List<String> adjacencyList) {
            int[] numberOfNeighbours = new int[this.numberOfPartition];
            int argmax = -1;

            LinkedList<Integer> TieBreaker = new LinkedList<Integer>();
            for(int i = 0; i < this.numberOfPartition; i++){
                TieBreaker.add(i);
            }

            int value = rand.nextInt(TieBreaker.size());
            argmax = TieBreaker.get(value);

            // compute the edge cut
            for(int i = 0 ; i < this.numberOfPartition ; i++) {
                this.numberOfEdges += numberOfNeighbours[i];
                if(i != argmax) {
                    this.numberOfEdgeCut += numberOfNeighbours[i];
                }
            }

            vertexToPartition.put(sourceId, argmax);
            partitionSize[argmax]++;
        }

        @Override
        String getName() {
            return NAME;
        }
    }
}
