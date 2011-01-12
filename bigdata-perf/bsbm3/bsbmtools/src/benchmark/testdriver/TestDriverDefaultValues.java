package benchmark.testdriver;

import java.io.File;

public class TestDriverDefaultValues {
	public static int warmups = 50;//how many Query mixes are run for warm up
	public static File usecaseFile = new File("usecases/explore/sparql.txt");
	public static int nrRuns = 500;
	public static long seed = 808080L;
	public static String defaultGraph = null;
	public static String resourceDir = "td_data";
	public static String xmlResultFile = "benchmark_result.xml";
	public static int timeoutInMs = 0;
	public static String driverClassName = "com.mysql.jdbc.Driver";
	public static int fetchSize = 100;
	public static boolean qualification = false;
	public static String qualificationFile = "run.qual";
	public static int qmsPerPeriod = 50;
	public static double percentDifference = 0.02;
	public static int nrOfPeriods = 5;
}
