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

import com.bigdata.btree.keys.KVO;

/**
 * Unit tests for the control logic used by {@link AbstractMasterTask} and
 * friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMasterTask extends AbstractMasterTestCase {

    public TestMasterTask() {
    }

    public TestMasterTask(String name) {
        super(name);
    }
    
    final M master = new M(masterStats, masterBuffer, executorService);
    
    /**
     * Test verifies start/stop of the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startStop() throws InterruptedException,
            ExecutionException {

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", 0, masterStats.elementsIn);
        assertEquals("chunksIn", 0, masterStats.chunksIn);
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);
        assertEquals("partitionCount", 0, masterStats.partitionCount);

    }

    /**
     * Unit test writes an empty chunk and then stops the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startEmptyWriteStop() throws InterruptedException,
            ExecutionException {

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[0];
        
        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", 0, masterStats.elementsIn);
        assertEquals("chunksIn", 0, masterStats.chunksIn);
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);
        assertEquals("partitionCount", 0, masterStats.partitionCount);

    }

    /**
     * Unit test writes a chunk and then stops the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteStop1() throws InterruptedException,
            ExecutionException {

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[] {
                new KVO<O>(new byte[]{1},new byte[]{2},null/*val*/),
                new KVO<O>(new byte[]{1},new byte[]{3},null/*val*/)
        };

        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", a.length, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", a.length, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);
        assertEquals("partitionCount", 1, masterStats.partitionCount);

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 2, subtaskStats.elementsOut);
            
        }

    }

    /**
     * Unit test writes a chunk that is split onto two subtasks and then stops
     * the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteStop2() throws InterruptedException,
            ExecutionException {

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[] {
                new KVO<O>(new byte[]{1},new byte[]{2},null/*val*/),
                new KVO<O>(new byte[]{2},new byte[]{3},null/*val*/),
                new KVO<O>(new byte[]{2},new byte[]{4},null/*val*/)
        };

        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", a.length, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", a.length, masterStats.elementsOut);
        assertEquals("chunksOut", 2, masterStats.chunksOut);
        assertEquals("partitionCount", 2, masterStats.partitionCount);

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 1, subtaskStats.elementsOut);
            
        }
        
        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(2));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 2, subtaskStats.elementsOut);
            
        }

    }

}
