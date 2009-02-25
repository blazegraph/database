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
 * Stress tests where we predeclare locks and sort the lock requests. Under
 * these conditions we DO NOT maintain a {@link TxDag} since deadlocks CAN NOT
 * arise.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestNonBlockingLockManagerWithPredeclaredLocks extends
        AbstractStressTestNonBlockingLockManager {

    /**
     * 
     */
    public StressTestNonBlockingLockManagerWithPredeclaredLocks() {
        super();
    }

    public StressTestNonBlockingLockManagerWithPredeclaredLocks(String name) {
        super(name);
    }

    /**
     * Test where no locks are declared. This should run all tasks with the
     * maximum concurrency. Since there is no timeout, all tasks should complete
     * successfully.
     * 
     * @throws Exception
     */
    public void test_noResourcesDoesNotWait_predeclareLocks() throws Exception {
        
        final Properties properties = new Properties();
        
        final int nthreads = 5;
        final int ntasks = 1000;
        properties.setProperty(TestOptions.NTHREADS, "" + nthreads);
        properties.setProperty(TestOptions.NTASKS, "" + ntasks);
        properties.setProperty(TestOptions.NRESOURCES, "10");
        properties.setProperty(TestOptions.MIN_LOCKS, "0");
        properties.setProperty(TestOptions.MAX_LOCKS, "0");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS, "true");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS, "true");
        
        final Result result = doComparisonTest(properties);

        // all tasks completed successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));

        /*
         * Make sure that the tasks were not single threaded. ideally they will
         * run with full concurrency (NTHREADS == maxrunning).
         */
        assertEquals("maxrunning", nthreads, Integer.parseInt(result
                .get("maxrunning")));

    }
    
    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks. In fact, since we
     * have 10 resource locks for each operation and only 100 operations the
     * chances of a deadlock on any given operation are extremely high. However,
     * since we are predeclaring our locks and the lock requests are being
     * sorted NO deadlocks should result.
     * <p>
     * Note: Tasks are not necessarily serialized for this case since there are
     * 100 resources, so even though each task obtains 10 locks it is possible
     * for tasks to have non-overlapping lock requests and therefore there can
     * be more than one task executing concurrently.
     */
    public void test_multipleResourceLocking_resources10_predeclareLocks_locktries10()
            throws Exception {

        final Properties properties = new Properties();

        final int nthreads = 20;
        final int ntasks = 1000;
        properties.setProperty(TestOptions.NTHREADS, "" + nthreads);
        properties.setProperty(TestOptions.NTASKS, "" + ntasks);
        properties.setProperty(TestOptions.NRESOURCES, "100");
        properties.setProperty(TestOptions.MIN_LOCKS, "10");
        properties.setProperty(TestOptions.MAX_LOCKS, "10");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES, "10");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"true");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"true");
                
        Result result = doComparisonTest(properties);
        
        /*
         * Deadlocks should not be possible when we predeclare and sort locks.
         */
        assertEquals("ndeadlock", 0, Integer.parseInt(result.get("ndeadlock")));

        // all tasks completed successfully.
        assertEquals("nsuccess", ntasks, Integer.parseInt(result
                .get("nsuccess")));

    }

}
