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
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.quorum;

import java.util.concurrent.atomic.AtomicLong;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestListener;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.ResultPrinter;

import org.apache.log4j.Logger;

/**
 * Aggregates test suites in increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    private final static Logger log = Logger.getLogger(TestAll.class);

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite() {

        final TestSuite suite = new TestSuite("quorum");

        /*
         * Test the fixture used to test the quorums (the fixture builds on the
         * same base class).
         */
        suite.addTestSuite(TestMockQuorumFixture.class);

        /*
         * Test the quorum semantics for a singleton quorum. This unit test
         * allows us to verify that each quorum state change is translated into
         * the appropriate methods against the public API of the quorum client
         * or quorum member.
         */
        suite.addTestSuite(TestSingletonQuorumSemantics.class);

        /*
         * Test the quorum semantics for a highly available quorum of 3 nodes.
         * The main points to test here are the particulars of events not
         * observable with a singleton quorum, including a service join which
         * does not trigger a quorum meet, a service leave which does not
         * trigger a quorum break, a leader leave, etc.
         */
        suite.addTestSuite(TestHA3QuorumSemantics.class);

        /*
         * Run the test HA3 suite a bunch of times.
         */
        suite.addTest(StressTestHA3.suite());

        return suite;
        
    }

    /**
     * Run the test suite many times.
     * 
     * @param args
     *            The #of times to run the test suite (defaults to 100).
     */
    public static void main(final String[] args) {
        
        final int LIMIT = args.length == 0 ? 100 : Integer.valueOf(args[0]);

        final AtomicLong nerrs = new AtomicLong(0);
        final AtomicLong nfail = new AtomicLong(0);
        
        // Setup test result.
        final TestResult result = new TestResult();
        
        // Setup listener, which will write the result on System.out
        result.addListener(new ResultPrinter(System.out));

        result.addListener(new TestListener() {
            
            public void startTest(Test arg0) {
                log.info(arg0);
            }
            
            public void endTest(Test arg0) {
                log.info(arg0);
            }
            
            public void addFailure(Test arg0, AssertionFailedError arg1) {
                nfail.incrementAndGet();
                log.error(arg0,arg1);
            }
            
            public void addError(Test arg0, Throwable arg1) {
                nerrs.incrementAndGet();
                log.error(arg0,arg1);
            }
        });
        
        final Test suite = TestAll.suite();

        int i = 0;
        for (; i < LIMIT && nerrs.get() == 0 && nfail.get() == 0; i++) {

            System.out.println("Starting iteration: " + i);

            suite.run(result);

        }

        System.out
                .println("Finished " + i + " out of " + LIMIT + " iterations");

    }

}
