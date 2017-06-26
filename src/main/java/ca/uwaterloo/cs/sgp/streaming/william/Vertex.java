package ca.uwaterloo.cs.sgp.streaming.william;

public class Vertex {
	int id;
	int degree;
	int neighbourHood[];
	public Vertex(int id, int degree, int neighbourHood[]){
		this.id = id;
		this.degree = degree;
		this.neighbourHood = neighbourHood;
	}
}
