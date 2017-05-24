package LinearDeterministic;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.FileNotFoundException;
import graph.Graph;
import graph.Vertex;

public class LDG {
	Graph graph;
	int size_of_graph;
	int capacity;
	double slack;
	int[] partitions;
	
	// Constructor of the LDG class, it creates the graph, set up the 
	// neighbor relationships for each vertex, calculate the capacity 
	// of each partition, and set the size of each partition to 0 by
	// default
	public LDG(String file, int k, double balance_slack){
		int Graph_size = 0;
		// Read all lines of the file to get the size of graph
		try{
			List<String> lines = Files.readAllLines(Paths.get(file));
			//minus 1 for the first line
			Graph_size = lines.size() - 1;
			graph = new Graph(Graph_size);
			size_of_graph = Graph_size;
			slack = balance_slack;
			
		}  catch(FileNotFoundException e){
			System.out.println("DataSet file not found.");
		}  catch(IOException e){
			e.printStackTrace();
		}
		// Array for partitions, values reveal the current size of
		// each partition
		partitions = new int[k];
		for(int i = 0; i < k; i++){
			partitions[i] = 0;
		}
		//capacity for each partition
		double exact_capacity = (Graph_size / k) * (1+slack);
		capacity = (int)exact_capacity;
		
	}
	// Count the number of neighbors of V which are partitioned to 
	// partition i
	int neighbors_in_partition(Vertex V, int i, String[] splitLine, int outDegree){
		int count = 0;
		// Iterate through the LinkedList until reach the end

		for(int j = 2; j < outDegree; j++ ){
			int next_value = Integer.parseInt(splitLine[j]);
			int next_partition = graph.vertex_to_partition.getOrDefault(next_value, -1);
			// if the neighbor is partitioned to i-partition already, 
			// increment the counter
			if(i == next_partition){
				count++;
			}
		}
		return count;
	}
	// Choose the partition for next vertex based on the formula in LDG
	int choose_partition(int index, int k, String[] splitLine, int outDegree){
		double result = -1;
		int argmax = -1;
		Vertex V = graph.list_of_vertices[index];
		//loop through all partitions
		LinkedList<Integer>TieBreaker = new LinkedList<Integer>();
		for(int i = 0; i < k; i++){
			int num_of_neighbors = neighbors_in_partition(V, i, splitLine, outDegree);
			// calculate the formulated value for each partition
			double next = (1 - (partitions[i] / capacity)) * num_of_neighbors;
			if(next > result){
				if(partitions[i] != capacity){
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
		return argmax;
	}
	
	public void LDG_partitioning(String file, int k, double slack){
		try{
			List<String> lines = Files.readAllLines(Paths.get(file));
			//loop through all lines in the file
			int index = 0;
			for(String next : lines){
				//split each line by whitespace
				if(next.contains("#")) continue; // skip first line
				String[] splitLine = next.split("\\s+");
				int VertexValue = Integer.parseInt(splitLine[0]);
				int outDegree = Integer.parseInt(splitLine[1]);
				Vertex newVertex = new Vertex(VertexValue, index, outDegree);
				
				graph.list_of_vertices[index] = newVertex;
				int next_partition = choose_partition(index, k, splitLine, outDegree);
				
				graph.vertex_to_partition.put(VertexValue, next_partition);
				graph.list_of_vertices[index].setPartition(next_partition);
				partitions[next_partition]++;
				index++;
			}
		}catch (FileNotFoundException e){
			System.out.println("DataSet File not found");
		}catch (IOException e){
			e.printStackTrace();
		}
	}
	public void print(){
		graph.print();
	}
}
