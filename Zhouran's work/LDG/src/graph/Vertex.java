package graph;


// value is the information given by the file 
// since the value is significantly large than the size of graph, 
// we need an additional index to keep track of each vertex in our 
// LinkedList

public class Vertex {
	int value;
	int index;
	int partition;
	
	public int num_neighbors;
	
	public Vertex[] neighbors;
	
	public Vertex(int init_value, int i, int outDegree){
		value = init_value;
		index = i;
		neighbors = new Vertex[outDegree];
		num_neighbors = outDegree;
		
		partition = -1;
	}
	public void setPartition(int k){
		partition = k;
	}
	
	public int getIndex(){
		return index;
	}
	
	public int getValue(){
		return value;
	}
	
	public int getPartition(){
		return partition;
	}
}
