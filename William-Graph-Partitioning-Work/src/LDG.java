import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;

public class LDG {
	int capacity;
	int k;
	ArrayList<ArrayList<Vertex>> partitions;
	FileInputStream fstream;
	
	public LDG(String fin,int k, double p) throws FileNotFoundException{
		this.k = k;
		fstream = new FileInputStream(fin);
		capacity = (int)(27770/k * (1+p)) + 1;
		partitions = new ArrayList<ArrayList<Vertex>>(k);
	}
	private int adjacent_part(ArrayList<Vertex> partition, Vertex vertex){// number of vertices that are neighbours in the partition
		int count = 0;
		for (int i = 0; i < partition.size(); ++i){
			for (int j = 0; j < partition.get(i).neighbourHood.length; ++j){
				if (partition.get(i).neighbourHood[j] == vertex.id){
					++count;
					break;
				}
			}
		}
		for (int j = 0; j < partition.size(); ++j){
			for (int l = 0; l < vertex.neighbourHood.length;++l){
				if (partition.get(j).id == vertex.neighbourHood[l]){
					++count; // counts other way
				}
			}
		}
		return count;
	}
//	private int findEdge(ArrayList<Vertex> partition, Vertex vertex){
//		int counter =0;
//		for (int j = 0; j < partition.size(); ++j){
//			for (int l = 0; l < vertex.neighbourHood.length;++l){
//				if (partition.get(j).id == vertex.neighbourHood[l]){
//					++counter; // counts other way
//				}
//			}
//		}
//		return counter;
//	}
	private int findEdgeCuts(ArrayList<Vertex> partition1,ArrayList<Vertex> partition2){
		int counter = 0;
		for (int i = 0; i < partition1.size(); ++i){
			Vertex vertex = partition1.get(i);
			counter += adjacent_part(partition2, vertex);
		}
		return counter;
	}
	public void run() throws IOException{
		
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		PrintWriter writer = new PrintWriter(new File("testout.txt"));
		String strLine;
		String arr[];
		int vertex_id;
		int degree;
		Vertex vertex;
		while ((strLine = br.readLine()) != null){
			arr = strLine.split(" ",3);
			vertex_id = Integer.parseInt(arr[0]);
			degree = Integer.parseInt(arr[1]);
			int neighbours[] = new int[degree];
			String arr_ids[]={};
			
			
			if (degree > 0){
			 arr_ids = arr[2].split(" ");
			}
			
			for (int i = 0; i < degree; ++i){
				neighbours[i] = Integer.parseInt(arr_ids[i]);
			}
			vertex = new Vertex(vertex_id,degree,neighbours);
			
			double score,max_score = 0;
			int partition_id = -1;
			for (int i = 0; i < partitions.size(); ++i){
				score = (adjacent_part(partitions.get(i),vertex) * (Math.abs(1-(double)(partitions.get(i).size()/capacity))));
				if (score > max_score){
					partition_id = i;
					max_score = score;
				}
			}
			
			if (partition_id == -1) {
				if (partitions.size() < k) {
					//create a new partition and add this vertex in the partition
					ArrayList<Vertex> aPartition = new ArrayList<Vertex>();
					aPartition.add(vertex);
					writer.println("Vertex ID: "  +vertex.id + " Partition_id: "+ partitions.size());
					partitions.add(aPartition);
				} else {
					//need to add this vertex to the partition with least number of vertices
					int idWithLeastVertices = -1;
					int least = 3000000;
					for (int i = 0; i < k; i++) {
						if (partitions.get(i).size() < least) {
							idWithLeastVertices = i;
							least = partitions.get(i).size();
						}
					}
					partitions.get(idWithLeastVertices).add(vertex);
					writer.println("Vertex ID: "  +vertex.id + " Partition_id: "+ idWithLeastVertices);
				}
			} else {
				partitions.get(partition_id).add(vertex);// TODO: NOT SURE IF THIS WORKS
				writer.println("Vertex ID: "  +vertex_id + " Partition_id: "+ partition_id);
			}
		}
		writer.flush();
		
		double totalEdgeCuts = 0;
		double edgeCutRatio = 0;
		for (int i = 0; i < k-1; ++i){
			for (int j = i+1; j < k; ++j){
				totalEdgeCuts += findEdgeCuts(partitions.get(i),partitions.get(j));
			}
		}
		edgeCutRatio = totalEdgeCuts/352807;
		System.out.println("Total Edge Cut Ratio: "+edgeCutRatio);
		for (int i = 0; i < k; ++i){
			System.out.println("Partition ID: " + i + " Number of Vertices: "+ partitions.get(i).size());
		}
	}
	

}
