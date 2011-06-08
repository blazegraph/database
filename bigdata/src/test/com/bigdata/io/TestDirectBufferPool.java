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
 * Created on May 16, 2008
 */

package com.bigdata.io;

import java.nio.ByteBuffer;

import junit.framework.TestCase2;

/**
 * Test suite for {@link DirectBufferPool}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDirectBufferPool extends TestCase2 {

    /**
     * 
     */
    public TestDirectBufferPool() {
    }

    /**
     * @param arg0
     */
    public TestDirectBufferPool(String arg0) {
        super(arg0);
    }

    @Override
    protected void tearDown() throws Exception {

        // Verify that all allocated buffers were released.
        DirectBufferPoolTestHelper.checkBufferPools(this);
        
        super.tearDown();
        
    }

    public void test_allocateRelease() throws InterruptedException {

        final int poolSizeBefore = DirectBufferPool.INSTANCE.getPoolSize();
        final int poolAcquiredBefore = DirectBufferPool.INSTANCE
                .getAcquiredBufferCount();

        final int poolSizeDuring;
        final int poolAcquiredDuring;
        {
            IBufferAccess b = null;
            try {
                b = DirectBufferPool.INSTANCE.acquire();

                poolSizeDuring = DirectBufferPool.INSTANCE.getPoolSize();
                poolAcquiredDuring = DirectBufferPool.INSTANCE
                        .getAcquiredBufferCount();

                assertEquals(poolSizeBefore + 1, poolSizeDuring);
                assertEquals(poolAcquiredBefore + 1, poolAcquiredDuring);

            } finally {
                if (b != null)
                    b.release();
            }
        }

        final int poolSizeAfter = DirectBufferPool.INSTANCE.getPoolSize();
        final int poolAcquiredAfter = DirectBufferPool.INSTANCE
                .getAcquiredBufferCount();

        // the pool size does not decrease.
        assertEquals(poolSizeBefore + 1, poolSizeAfter);

        // the #of acquired buffers does decrease.
        assertEquals(poolAcquiredBefore, poolAcquiredAfter);

    }

    /**
     * Test verifies that a pool will not allocate a new buffer when it can
     * recycle one instead.
     * 
     * @throws InterruptedException
     */
    public void test_buffersRecycled() throws InterruptedException {

        /*
         * Acquire/release one buffer before we look at the pool size. This
         * should give us at least one available buffer in the pool. That way
         * when we run through the allocation loop the pool size should not
         * change.
         */
        {
            IBufferAccess b = null;
            try {
                b = DirectBufferPool.INSTANCE.acquire();
            } finally {
                if (b != null)
                    b.release();
            }
        }

        final int poolSizeBefore = DirectBufferPool.INSTANCE.getPoolSize();

        for (int i = 0; i < 10; i++) {
            IBufferAccess b = null;
            try {
                b = DirectBufferPool.INSTANCE.acquire();
                // pool size remains constant.
                assertEquals(poolSizeBefore, DirectBufferPool.INSTANCE
                        .getPoolSize());
            } finally {
                if (b != null)
                    b.release();
            }
        }

        // pool size remains constant.
        assertEquals(poolSizeBefore, DirectBufferPool.INSTANCE.getPoolSize());

    }

    /**
     * Unit test to verify that a pool will reject an attempt to
     * "double release" a buffer (only currently acquired buffers can be
     * released).
     * 
     * @throws InterruptedException
     */
    public void test_doubleRelease() throws InterruptedException {

        IBufferAccess b = null;
        try {
            b = DirectBufferPool.INSTANCE.acquire();
        } finally {
            if (b != null)
                b.release();
        }

        if (b != null) {
            try {
                // Attempt to double-release the buffer.
                b.release();
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }
        }

    }

    /**
     * Unit test to verify that a pool will reject a buffer not acquired from
     * that pool.
     */
    public void test_rejectBufferFromAnotherPool() throws InterruptedException {

        // A distinct pool with the same buffer capacity
        final DirectBufferPool testPool = new DirectBufferPool("test",
                1/* poolCapacity */, DirectBufferPool.INSTANCE
                        .getBufferCapacity());

        IBufferAccess b = null;
        try {
            b = DirectBufferPool.INSTANCE.acquire();
            try {
            	// Only possible to test since in same package
                testPool.release(b.buffer());
                fail("Release should not be permitted to a different pool. Expecting: "
                        + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }
        } finally {
            b.release();
        }

    }

}
