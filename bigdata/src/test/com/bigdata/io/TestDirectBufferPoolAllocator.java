/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 8, 2010
 */

package com.bigdata.io;

import java.nio.ByteBuffer;

import junit.framework.TestCase2;

import com.bigdata.io.DirectBufferPoolAllocator.Allocation;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocation;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;

/**
 * Test suite for {@link DirectBufferPoolAllocator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDirectBufferPoolAllocator extends TestCase2 {

    /**
     * 
     */
    public TestDirectBufferPoolAllocator() {
    }

    /**
     * @param name
     */
    public TestDirectBufferPoolAllocator(String name) {
        super(name);
    }

    final DirectBufferPool pool = DirectBufferPool.INSTANCE;

    /**
     * Opens and closes the allocator.
     */
    public void test_close() {

        final DirectBufferPoolAllocator fixture = new DirectBufferPoolAllocator(
                pool);

        fixture.close();
        
    }

    /**
     * Unit test verifies that {@link IAllocationContext}s created for different
     * keys are distinct.
     */
    public void test_allocationContextsAreDistinct() {
        
        final DirectBufferPoolAllocator fixture = new DirectBufferPoolAllocator(
                pool);

        try {

            final Object key1 = "1";
            final Object key2 = "2";

            final IAllocationContext context1 = fixture
                    .getAllocationContext(key1);
            assertNotNull(context1);

            final IAllocationContext context2 = fixture
                    .getAllocationContext(key2);
            assertNotNull(context2);

            assertTrue(context1 != context2);

        } finally {
         
            fixture.close();
            
        }
        
    }
    
    /**
     * Opens the allocator, creates an {@link IAllocationContext}, obtains an
     * {@link IAllocation} from that context, and then releases the
     * {@link IAllocationContext}. The test also verifies that the
     * {@link ByteBuffer} was released back to the {@link DirectBufferPool}.
     * 
     * @throws InterruptedException
     */
    public void test_allocateThenClose() throws InterruptedException {

        final DirectBufferPoolAllocator fixture = new DirectBufferPoolAllocator(
                pool);

        try {

            final Object key = getName();

            final IAllocationContext context1 = fixture
                    .getAllocationContext(key);

            /*
             * Note: We can not test the poolSize change because it may have
             * unallocated ByteBuffer's lying about which it can reuse without
             * allocating a new buffer.
             */
//            final int poolSizeBefore = pool.getPoolSize();

            final int maxSize = fixture.getMaxSlotSize();

            // get a maximum size allocation.
            final IAllocation[] allocations = context1.alloc(maxSize);

//            assertEquals(poolSizeBefore + 1, pool.getPoolSize());

            // should be a single allocation
            assertEquals(1, allocations.length);

            // the allocation.
            final IAllocation tmp = allocations[0];

            // valid id.
            assertNotNull(tmp.getId());

            // valid slice.
            {
                final ByteBuffer b = tmp.getSlice();
                assertNotNull(b);
                assertEquals("position", 0, b.position());
                assertEquals("limit", maxSize, b.limit());
                assertEquals("capacity", maxSize, b.capacity());
                assertTrue("isDirect", b.isDirect());
                assertFalse("isReadOnly", b.isReadOnly());
            }

            // release the allocation.
            context1.release();

            /*
             * Note: while the buffer was released to the pool, the pool size
             * does not decrease so we can not check the post condition here.
             */
            // assertEquals(poolSizeBefore, pool.getPoolSize());

            try {
                tmp.getSlice();
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

        } finally {

            fixture.close();

        }

    }

    /**
     * @todo Write a unit test to look for a memory leak in the backing
     *       {@link DirectBufferPool} as allocations are released from the
     *       {@link DirectBufferPoolAllocator}. However, not that the
     *       {@link DirectBufferPool} DOES NOT release buffers back to the JVM
     *       so the pool size will not decrease. Instead, what you have to do is
     *       look to see that alloc/free alloc/free patterns do not cause the
     *       #of allocated buffers on the {@link DirectBufferPool} to increase.
     */
    public void test_memoryLeak() {

        fail("write test");

    }

    /**
     * Unit tests for multiple allocations within the same. This verifies both
     * the the manner in which the position and limit are updated as we walk
     * through the buffer.
     * 
     * @throws InterruptedException 
     */
    public void test_multipleBufferAllocation() throws InterruptedException {

        final DirectBufferPoolAllocator fixture = new DirectBufferPoolAllocator(
                pool);

        try {

            final IAllocationContext ctx = fixture.getAllocationContext("1");
            
            final int allocSize = 10;

            final IAllocation[] a1 = ctx.alloc(allocSize);
            
            final IAllocation[] a2 = ctx.alloc(allocSize);
            
            assertEquals(1,a1.length);
            assertEquals(1,a2.length);
            assertEquals(allocSize,a1[0].getSlice().capacity());
            assertEquals(allocSize,a2[0].getSlice().capacity());

            final Allocation x0 = (Allocation)a1[0];
            final Allocation x1 = (Allocation)a2[0];
            
            // both allocations are slices onto the same backing buffer.
            assertTrue(x0.nativeBuffer == x1.nativeBuffer);
            
            // the position was advanced by the #of bytes allocated.
            assertEquals(allocSize * 2, x0.nativeBuffer.position());

            // the limit on the native byte buffer has not been changed.
            assertEquals(x0.nativeBuffer.capacity(), x0.nativeBuffer.limit());
            
        } finally {

            fixture.close();

        }

    }

    /**
     * @todo write a unit test for
     *       {@link DirectBufferPoolAllocator#put(byte[], IAllocation[])}.
     */
    public void test_put() {
        fail("write tests");
    }

}
