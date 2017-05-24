import LinearDeterministic.LDG;

public class main {

	public static void main(String[] args) {
		LDG ldg = new LDG("src/cit-HepTh-adjacency.txt", 20, 0.05);
		ldg.LDG_partitioning("src/cit-HepTh-adjacency.txt", 20, 0.05);

		ldg.print();
	}

}
