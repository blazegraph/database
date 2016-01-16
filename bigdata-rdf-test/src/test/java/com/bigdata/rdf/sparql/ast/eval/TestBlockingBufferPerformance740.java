/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sparql.ast.eval;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import junit.framework.TestCase;


import com.bigdata.rdf.sparql.ast.ASTContainer;

public class TestBlockingBufferPerformance740 extends AbstractDataDrivenSPARQLTestCase {

	public TestBlockingBufferPerformance740() {
	}

	public TestBlockingBufferPerformance740(String name) {
		super(name);
	}

	/**
	 * This test measures the percent utilization by comparing CPU time
	 * with wall clock time. The test fails if this is less than 80%.
	 * Obviously, the expected value is greater for machines with more CPUs, the
	 * test does not take this into account.
	 * @throws Exception
	 */
    public void test_ticket_740() throws Exception {

        new TestHelper("blocking-buffer-740",// testURI,
                "blocking-buffer-740.rq",// queryFileURL
                "blocking-buffer-740.rdf",// dataFileURL
                "blocking-buffer-740.srx"// resultFileURL
        		){
            @Override

            public ASTContainer runTest() throws Exception {
            	ThreadMXBean mgmt = ManagementFactory.getThreadMXBean();
            		//	mgmt.getThreadCPUTime() ;
            	long startCpuTime = currentTotalCpuTime(mgmt);
            	long startWallClock = System.currentTimeMillis();
            	try {
                	return super.runTest();
            	}
            	finally {
            		long cpuTime = currentTotalCpuTime(mgmt) - startCpuTime;
            		long clockTime = System.currentTimeMillis() - startWallClock;
            		cpuTime /= 1000000;
            		double utilization = cpuTime * 1.0 / clockTime;
            		TestCase.assertTrue("Performance was poor: "+(int)(utilization*100)+"% [< 80%]",utilization>0.8);
            		System.out.println("BlockingBuffer: performance was good: "+(int)(utilization*100)+"%");
            	}
            }

            /**
             * This is a pretty optimistic implementation ...
             * it does not deal with threads dying.
             * It seems to suffice for this test though.
             * @param mgmt
             * @return
             */
			private long currentTotalCpuTime(ThreadMXBean mgmt) {
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
