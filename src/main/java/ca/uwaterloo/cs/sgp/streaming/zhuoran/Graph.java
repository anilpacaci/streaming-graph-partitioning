package ca.uwaterloo.cs.sgp.streaming.zhuoran;
import java.util.*;
import java.io.PrintWriter;
import java.io.IOException;

public class Graph {
	int size;
	public Vertex[] list_of_vertices;
	public HashMap<Long, Integer> vertex_to_partition;
	
	public Graph(int init_size){
		size = init_size;
		vertex_to_partition = new HashMap<Long, Integer>();
		list_of_vertices = new Vertex[init_size];
	}
	
	
	
	public void check_print(){
		for(Vertex v: list_of_vertices){
			System.out.println("Index: " + v.getIndex() + "Value: " + v.getValue());
		}
	}
	// Print value of each vertex along with the partition
	// Finally calculate the overall edge-cut ratio
	public void print(String outputFile){
		try{
		PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
		for(int i = 0; i < size; i++){
			writer.println(list_of_vertices[i].getValue() +
					", " + list_of_vertices[i].getPartition());
		}
		int edge_cut = 0;
		int numEdges = 0;
		for(int i = 0; i< size; i++){
			Vertex current = list_of_vertices[i];
			//loop through all neighbors of current, if the partition is 
			//different, increment edge_cut
			for(int j = 0; j < current.num_neighbors; j++){
				numEdges++;
				if(list_of_vertices[j].getPartition() != current.getPartition()){
					edge_cut++;
				}
			}
		}
		double ratio = (double)edge_cut / numEdges;
		writer.println("The edge-cut ratio is: " + ratio);
		writer.close();
		}catch (IOException e){
			e.printStackTrace();
		}
	}
}
