package com.bigdata.rdf.sparql.ast.eval;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import junit.framework.TestCase;

import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase.TestHelper;

public class TestBlockingBufferPerformance740 extends AbstractDataDrivenSPARQLTestCase {

	public TestBlockingBufferPerformance740() {
	}

	public TestBlockingBufferPerformance740(String name) {
		super(name);
	}

    public void test_ticket_747a() throws Exception {

        new TestHelper("blocking-buffer-740",// testURI,
                "blocking-buffer-740.rq",// queryFileURL
                "blocking-buffer-740.rdf",// dataFileURL
                "blocking-buffer-740.srx"// resultFileURL
        		){
            @Override

            public ASTContainer runTest() throws Exception {
            	ThreadMXBean mgmt = ManagementFactory.getThreadMXBean();
            		//	mgmt.getThreadCPUTime() ;
            	long totalThreadTime = sumForAllThreads(mgmt);
            	long milli = System.currentTimeMillis();
            	try {
                	return super.runTest();
            	}
            	finally {
            		//totalThreadTime -= sumForAllThreads(mgmt);
            		long cpuTime = sumForAllThreads(mgmt) - totalThreadTime;
            		long clockTime = System.currentTimeMillis() - milli;
            		cpuTime /= 1000000;
            		//System.err.println(clockTime+":"+cpuTime);
            		double percentUtilization = cpuTime * 1.0 / clockTime;
            		TestCase.assertTrue("Performance was poor: "+(int)(percentUtilization*100)+"% [< 80%]",percentUtilization>0.8);
            		System.out.println("BlockingBuffer: performance was good: "+(int)(percentUtilization*100)+"%");
            	}
            }

			private long sumForAllThreads(ThreadMXBean mgmt) {
				long rslt = 0;
				for (long tid:mgmt.getAllThreadIds()) {
					long t = mgmt.getThreadCpuTime(tid);
					if (t != -1)
						rslt += t;
				}
				return rslt;
			}
    		
        	
        	
        }.runTest();

    }
}
