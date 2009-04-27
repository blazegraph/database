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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.keys.KVO;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Unit tests for the shutdown logic when the subtask chunk timeout and the
 * subtask idle timeout are {@link Long#MAX_VALUE}. In this situation the
 * subtask must notice that the master's input buffer has been closed and close
 * its input buffer so that it will process any elements remaining in that
 * buffer and then terminate.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMasterTaskWithInfiniteTimeout extends AbstractMasterTestCase {

    public TestMasterTaskWithInfiniteTimeout() {
    }

    public TestMasterTaskWithInfiniteTimeout(String name) {
        super(name);
    }

    /**
     * Unit test verifies correct shutdown when writing chunks onto a master
     * whose subtask has an infinite chunk timeout and an infinite idle timeout.
     * This test verifies that the subtask will recognize that the input buffer
     * for the master has been closed and not continue to wait for more data in
     * order to create a full chunk.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteErrorStop() throws InterruptedException,
            ExecutionException {

        final M master = new M(masterStats, masterBuffer, executorService,
                Long.MAX_VALUE/* sinkIdleTimeout */, M.DEFAULT_SINK_POLL_TIMEOUT) {

            /**
             * The subtask will use a buffer with an infinite chunk timeout.
             */
            @Override
            protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

                return new BlockingBuffer<KVO<O>[]>(
                        new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_SIZE,// 
                        Long.MAX_VALUE, TimeUnit.SECONDS,// chunkTimeout
                        true // ordered
                );

            }
            
        };
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        // write a chunk on the master.
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 1 }, null/* val */)
            };

            masterBuffer.add(a);

        }
        
        /*
         * Sleep long enough that we may assume that the chunk has been
         * propagated by the master to the subtask.
         */ 
        Thread.sleep(1000/* ms */);

        // verify that the master has accepted the 1st chunk. 
        assertEquals("elementsIn", 1, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        
        // verify that nothing has been output yet.
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);

        // write another chunk on the master (distinct values).
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */)
            };

            masterBuffer.add(a);
            
        }

        // sleep some more.
        Thread.sleep(1000/* ms */);

        // verify that the master has accepted the 2nd chunk. 
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        
        // verify that nothing has been output yet.
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);

        // close the master - the output sink should now emit a chunk.
        masterBuffer.close();

        // await the future.
        masterBuffer.getFuture().get();
        
        // verify elements in/out; chunks in/out
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        
        assertEquals("elementsOut", 2, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);
        
        assertEquals("partitionCount", 1, masterStats.partitionCount);

    }

}
