package ca.uwaterloo.cs.sgp.streaming.william;

import java.io.*;
import java.util.*;
public class WeekOneLDG {

	public static void main(String[] args) throws IOException {
		LDG ldg = new LDG("cit-HepTh-adjacency.txt",5,0.06);
		ldg.run();
		System.out.println("completed writing");
	}

}
