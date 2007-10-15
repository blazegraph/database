/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
