/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Oct 1, 2007
 */

package com.bigdata.concurrent;

import java.util.Properties;

import com.bigdata.test.ExperimentDriver.Result;

/**
 * Stress tests where a {@link TxDag} is used to detect deadlock.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestNonBlockingLockManagerWithTxDag extends
        AbstractStressTestNonBlockingLockManager {

    /**
     * 
     */
    public StressTestNonBlockingLockManagerWithTxDag() {
        super();
    }

    public StressTestNonBlockingLockManagerWithTxDag(String name) {
        super(name);
    }

    /**
     * Test where each operation locks only a single resource (low concurrency
     * condition w/ 5 threads). There is no timeout. All tasks should run to
     * completion. Since modestly large number of resources (100) is used and
     * since each task declares a single resource lock, the maximum observed
     * concurrency should be the #of tasks in the thread pool.
     */
    public void test_singleResourceLocking_waitsFor_lowConcurrency5() throws Exception {

        final Properties properties = new Properties();

        final int nthreads = 5;
        final int ntasks = 1000;
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);

        // all tasks completed successfully.
        assertEquals(ntasks,Integer.parseInt(result.get("nsuccess")));

        // tasks were run in parallel.
        assertEquals(nthreads, Integer.parseInt(result.get("maxrunning")));

    }
    
    /**
     * Test where each operation locks only a single resource using a thread
     * pool with 20 core threads. Since there is no timeout, all tasks should
     * run to completion. The maximum observed concurrency SHOULD be equal to
     * the size of the thread pool.
     */
    public void test_singleResourceLocking_waitsFor_defaultConcurrency20()
            throws Exception {

        final Properties properties = new Properties();

        final int nthreads = 20;
        final int ntasks = 1000;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);
        
        // all tasks complete successfully.
        assertEquals(ntasks, Integer.parseInt(result.get("nsuccess")));

        // tasks were run in parallel.
        assertEquals(nthreads, Integer.parseInt(result.get("maxrunning")));

    }
    
    /**
     * Test where each operation locks only a single resource (high concurrency
     * condition with 100 threads).
     */
    public void test_singleResourceLocking_waitsFor_highConcurrency100() throws Exception {

        final Properties properties = new Properties();
        
        final int nthreads = 100;
        final int ntasks = 1000;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);
        
        // all tasks complete successfully.
        assertEquals(ntasks, Integer.parseInt(result.get("nsuccess")));

        /*
         * Tasks were run in parallel with at least 50% of the threads executing
         * concurrently.
         */
        final int maxrunning = Integer.parseInt(result.get("maxrunning"));
        
        assertTrue("nthreads=" + nthreads + ", but maxrunning is only"
                + maxrunning, maxrunning >= nthreads / 2);

    }
    
    /**
     * Test where each operation locks only a single resource and there is only one
     * resource to be locked so that all operations MUST be serialized.
     * 
     * FIXME postcondition tests.
     */
    public void test_singleResourceLocking_serialized_waitsFor_lowConcurrency2() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"2");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized.
     * 
     * FIXME postcondition tests.
     */
    public void test_singleResourceLocking_serialized_waitsFor_lowConcurrency5() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"3");
        // Note: Small ntasks since this case otherwise is slow.
        properties.setProperty(TestOptions.NTASKS,"100");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0"); // Note: timeout==0 when debugging.
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized and
     * where 10% of all tasks die a horrid death.
     * 
     * FIXME postcondition tests.
     */
    public void test_singleResourceLocking_serialized_waitsFor_lowConcurrency5_withTaskDeath() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"3");
        // Note: Small ntasks since this case otherwise is slow.
        properties.setProperty(TestOptions.NTASKS,"100");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0"); // Note: timeout==0 when debugging.
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        properties.setProperty(TestOptions.PERCENT_TASK_DEATH,".10");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized.
     */
    public void test_singleResourceLocking_serialized_waitsFor_highConcurrency() throws Exception {

        final Properties properties = new Properties();
        
        final int nthreads = 100;
        final int ntasks = 100;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        // Note: Small ntasks since otherwise this case takes very long.
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);
        
        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // Should have been serialized.
        assertEquals("maxrunning",1, Integer.parseInt(result.get("maxrunning")));

        // All tasks complete successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized with
     * a non-zero lock timeout. This test stresses the logic in lock() that is
     * responsible for backing out a lock request on timeout.
     */
    public void test_singleResourceLocking_serialized_waitsFor_highConcurrency_lockTimeout()
            throws Exception {

        final Properties properties = new Properties();
        
        final int nthreads = 100;
        final int ntasks = 100;
        
        properties.setProperty(TestOptions.NTHREADS,""+nthreads);
        // Note: Small ntasks since otherwise this case takes very long.
        properties.setProperty(TestOptions.NTASKS,""+ntasks);
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.LOCK_TIMEOUT,"1000");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        final Result result = doComparisonTest(properties);
        
        // Deadlocks should not be possible with only one resource.
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // Should have been serialized.
        assertEquals("maxrunning",1, Integer.parseInt(result.get("maxrunning")));
        
        // Note: Timeouts should be expected. They will show up as cancelled
        // tasks.
        final int ntimeout = Integer.parseInt(result.get("ntimeout"));
        assertTrue("No timeouts?", ntimeout > 0);
        
    }

    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks.
     * 
     * FIXME postcondition tests.
     */
    public void test_multipleResourceLocking_resources3_waitsFor_locktries3() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"20");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"3");
        properties.setProperty(TestOptions.MAX_LOCKS,"3");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES,"3");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks. In fact, since we
     * have 10 resource locks for each operation and only 100 operations the
     * chances of a deadlock on any given operation are extremely high.
     * 
     * FIXME postcondition tests.
     */
    public void test_multipleResourceLocking_resources10_waitsFor_locktries10() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"20");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"10");
        properties.setProperty(TestOptions.MAX_LOCKS,"10");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES,"10");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
                
        doComparisonTest(properties);
        
    }

}
