import LinearDeterministic.LDG;

public class main {

	public static void main(String[] args) {
		LDG ldg = new LDG("src/part-00000", 100, 0.05);
		ldg.LDG_partitioning("src/part-00000", 100, 0.05, true);

		ldg.print();
	}

}
