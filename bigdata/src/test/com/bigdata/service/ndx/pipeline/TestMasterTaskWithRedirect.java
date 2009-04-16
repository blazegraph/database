/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.keys.KVO;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Test ability to handle a redirect (subtask learns that the target service no
 * longer accepts data for some locator and instead must send the data somewhere
 * else).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMasterTaskWithRedirect extends AbstractMasterTestCase {

    public TestMasterTaskWithRedirect() {
    }

    public TestMasterTaskWithRedirect(String name) {
        super(name);
    }

    /**
     * Unit test verifies correct redirect of a write.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteRedirectStop() throws InterruptedException,
            ExecutionException {

        /*
         * Note: The master is overriden so that the 1st chunk written onto
         * locator(13) will cause an StaleLocatorException to be thrown.
         */
        
        final M master = new M(masterStats, masterBuffer, executorService) {

            @Override
            protected S newSubtask(L locator, BlockingBuffer<KVO<O>[]> out) {

                if (locator.locator == 13) {

                    return new S(this, locator, out) {

                        @Override
                        protected boolean nextChunk(final KVO<O>[] chunk)
                                throws Exception {

                            // the write will be redirected into partition#14.
                            redirects.put(13, 14);
                            
                            handleRedirect(this, chunk);
                            
                            // stop processing.
                            return false;
                            
                        }

                    };
                    
                }

                return super.newSubtask(locator, out);
                
            }
            
        };
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[] {
                new KVO<O>(new byte[]{1},new byte[]{2},null/*val*/),
                new KVO<O>(new byte[]{13},new byte[]{3},null/*val*/)
        };

        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", a.length, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", a.length, masterStats.elementsOut);
        assertEquals("chunksOut", 2, masterStats.chunksOut);
        assertEquals("partitionCount", 3, masterStats.partitionCount);

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 1, subtaskStats.elementsOut);
            
        }
        
        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(13));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 0, subtaskStats.chunksOut);
            assertEquals("elementsOut", 0, subtaskStats.elementsOut);
            
        }

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(14));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 1, subtaskStats.elementsOut);
            
        }

    }

    /**
     * Unit test verifies correct redirect of a write arising during awaitAll()
     * in the master and occuring after there has already been a write on the
     * partition which is the target of the redirect. This explores the ability
     * of the master to correctly re-open a sink which had been closed.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteRedirectWithReopenStop() throws InterruptedException,
            ExecutionException {

        /*
         * Note: The set of conditions that we want to test here are quite
         * tricky. Therefore I have put a state machine into the fixture for the
         * purposes of this test using a lock and a variety of conditions to
         * ensure that the things happen in the desired sequence.
         */

        final ReentrantLock lck = new ReentrantLock(true/*fair*/);
        
        /*
         * The redirect has to wait until this condition is signalled. It is
         * signaled from within master#awaitAll() once the master has closed the
         * buffers for the existing output sinks.
         */
        final Condition c1 = lck.newCondition();
        /*
         * Set true when L(14) is closed (this is not cleared when it is
         * reopened).
         */
        final AtomicBoolean L14WasClosed = new AtomicBoolean(false);
        
        final M master = new M(masterStats, masterBuffer, executorService) {

            @Override
            protected void removeOutputBuffer(final L locator,
                    final AbstractSubtask sink) {

                super.removeOutputBuffer(locator, sink);

                if (locator.locator == 14) {

                    /*
                     * Signal when L(14) is removed. That should happen in
                     * master.awaitAll() when it closes the output buffers for
                     * the existing sinks.
                     */
                    lck.lock();
                    try {
                        if(log.isInfoEnabled())
                            log.info("Signaling now.");
                        c1.signal();
                        L14WasClosed.set(true);
                    } finally {
                        lck.unlock();
                    }

                }

            }
            
            @Override
            protected S newSubtask(L locator, BlockingBuffer<KVO<O>[]> out) {

                if (locator.locator == 13) {

                    /*
                     * The L(13) sink will wait until it is signalled before
                     * issueing an L(13) => L(14) redirect.
                     */
                    
                    return new S(this, locator, out) {

                        @Override
                        protected boolean nextChunk(final KVO<O>[] chunk)
                                throws Exception {

                            lck.lock();
                            try {
                                if (!L14WasClosed.get()) {
                                    // wait up to 1s for the signal and then
                                    // die.
                                    if (!c1.await(1000, TimeUnit.MILLISECONDS))
                                        fail("Not signaled?");
                                }
                            } finally {
                                lck.unlock();
                            }

                            // the write will be redirected into partition#14.
                            redirects.put(13, 14);
                            
                            handleRedirect(this, chunk);
                            
                            // stop processing.
                            return false;
                            
                        }

                    };
                    
                }

                return super.newSubtask(locator, out);
                
            }
            
        };
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        // write on L(1) and L(14).
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */),
                    new KVO<O>(new byte[] { 14 }, new byte[] { 3 }, null/* val */) };

            masterBuffer.add(a);
        }

        /*
         * Sleep for a bit so that the first chunk gets pulled out of the
         * master's buffer. This way chunksIn will be reported as (2). Otherwise
         * it is quite likely to be reported as (1) because the asynchronous
         * iterator will generally combine the two input chunks.
         */
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(masterBuffer
                .getChunkTimeout() * 2));
        
        /*
         * Write on L(13). This will be redirected to L(14). The redirect will
         * not be issured until the master has CLOSED the output buffer for
         * L(14). This will force the master to re-open that output buffer.
         */
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 13 }, new byte[] { 3 }, null/* val */) };

            masterBuffer.add(a);
        }
        
        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", 3, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        assertEquals("elementsOut", 3, masterStats.elementsOut);
        assertEquals("chunksOut", 3, masterStats.chunksOut);
        assertEquals("partitionCount", 3, masterStats.partitionCount);

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 1, subtaskStats.elementsOut);
            
        }
        
        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(13));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 0, subtaskStats.chunksOut);
            assertEquals("elementsOut", 0, subtaskStats.elementsOut);
            
        }

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(14));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 2, subtaskStats.chunksOut);
            assertEquals("elementsOut", 2, subtaskStats.elementsOut);
            
        }

    }

}
