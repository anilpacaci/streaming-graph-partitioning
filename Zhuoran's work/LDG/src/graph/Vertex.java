package graph;


// value is the information given by the file 
// since the value is significantly large than the size of graph, 
// we need an additional index to keep track of each vertex in our 
// LinkedList

public class Vertex {
	long value;
	int index;
	String vlabel;
	int partition;
	
	public int num_neighbors;
	
	public Vertex[] neighbors;
	
	public Vertex(long init_value, int i, int outDegree, String label){
		value = init_value;
		index = i;
		vlabel = label;
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
	
	public String getValue(){
		return vlabel;
	}
	
	public int getPartition(){
		return partition;
	}
}
