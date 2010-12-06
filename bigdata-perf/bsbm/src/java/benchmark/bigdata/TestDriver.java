	package benchmark.bigdata;

import java.io.IOException;

import org.apache.log4j.xml.DOMConfigurator;

import benchmark.testdriver.Query;

public class TestDriver extends benchmark.testdriver.TestDriver {

	public TestDriver(String[] args) {
		super(args);
	}
	
	public void printQueries(final int nrRun) {
		
		queryMix.setRun(nrRun);
		while(queryMix.hasNext()) {
			Query next = queryMix.getNext();
			Object[] queryParameters = parameterPool.getParametersForQuery(next);
			next.setParameters(queryParameters);
//			if(ignoreQueries[next.getNr()-1])
				queryMix.setCurrent(0, -1.0);
//			else {
//				server.executeQuery(next, next.getQueryType());
//			}
			System.out.println("query " + next.getNr() + ":");
			System.out.println(next.getQueryString());
			System.out.println("");
//			try {
//				System.in.read();
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			}
		}

	}
	
	public static void main(String[] argv) {
		DOMConfigurator.configureAndWatch( "log4j.xml", 60*1000 );
		TestDriver testDriver = new TestDriver(argv);
		testDriver.init();
		
//		for (int i = 0; i < 3; i++)
//			testDriver.printQueries(i);
		testDriver.printQueries(0);
		
	}
	
}
