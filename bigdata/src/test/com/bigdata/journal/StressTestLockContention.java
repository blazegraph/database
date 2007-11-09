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
 * Created on Oct 15, 2007
 */

package com.bigdata.journal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Lock contention results when unisolated writers seek conflicting locks. In
 * all cases lock contention reduces the possible parallelism. However, in the
 * extreme case, lock contention forces the serialization of unisolated writers.
 * <p>
 * This test suite may be used to examine the responsiveness of the
 * {@link WriteExecutorService} under lock contention. Performance will be less
 * than that for the equivilent tasks without lock contention, but group commit
 * should still block many serialized writer tasks together for good throughput.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestLockContention extends ProxyTestCase {

    /**
     * 
     */
    public StressTestLockContention() {
        super();
    }

    /**
     * @param name
     */
    public StressTestLockContention(String name) {
        super(name);
    }

    /**
     * Test that no tasks are failed when a large set of <strong>writer</strong>
     * tasks that attempt to lock the same resource(s) are submitted at once
     * (write tasks use the lock system to control access to the unisolated
     * indices).
     * <p>
     * Note: Tasks will be serialized since they are contending for the same
     * resources.
     * 
     * @throws InterruptedException
     */
    public void test_lockContention() throws InterruptedException {

        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo","bar","baz"};

        final int ntasks = 1000;
        
        Collection<AbstractTask> tasks = new HashSet<AbstractTask>(ntasks);

        for (int i = 0; i < ntasks; i++) {

            tasks.add(new AbstractTask(journal, ITx.UNISOLATED,
                    false/* readOnly */, resource) {

                protected Object doTask() throws Exception {

                    return null;

                }

            });

        }

        /*
         * Submit all tasks. Tasks can begin executing right away. If the write
         * service is using a blocking queue with a limited capacity then some
         * or all of the tasks may complete before this method returns.
         */
        
        List<Future<Object>> futures = journal.invokeAll(tasks, 3,
                TimeUnit.SECONDS);

        /*
         * Shutdown the journal.
         * 
         * Note: It is possible for shutdownNow() to close the store before all
         * worker threads have been cancelled, in which case you may see some
         * strange errors being thrown.
         */

        journal.shutdownNow();
        
        journal.delete();

        Iterator<Future<Object>> itr = futures.iterator();

        int ncancelled = 0;
        int ncomplete = 0;
        int nerror = 0;
        
        while(itr.hasNext()) {
            
            Future<Object> future = itr.next();
            
            if (future.isCancelled()) {

                ncancelled++;

            } else if (future.isDone()) {
              
                try {
                
                    future.get();
                    
                    ncomplete++;
                    
                } catch (ExecutionException ex) {
                    
                    nerror++;
                    
                    log.warn("Not expecting: "+ex, ex);
                    
                }
                
            }
            
        }
        
        System.err.println("#tasks=" + ntasks + " : ncancelled=" + ncancelled
                + ", ncomplete=" + ncomplete + ", nerror=" + nerror);
        
        /*
         * No errors are allowed, but some tasks may never start due to the high
         * lock contention.
         */
        
        assertEquals("nerror", 0, nerror);
        
    }
    
}
