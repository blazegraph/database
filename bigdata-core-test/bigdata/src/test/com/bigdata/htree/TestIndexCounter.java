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
 * Created on May 17, 2007
 */

package com.bigdata.htree;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for the {@link IIndex#getCounter()} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexCounter extends AbstractHTreeTestCase {

    /**
     * 
     */
    public TestIndexCounter() {
    }

    /**
     * @param name
     */
    public TestIndexCounter(String name) {
        super(name);
    }

    /**
     * Unit test for {@link Counter} to verify basic increment behavior.
     */
    public void test_counter() {
        
    	final IRawStore store = new SimpleMemoryRawStore();
    	try {
    	
        final HTree btree = getHTree(store, 3);
        
        final ICounter counter = btree.getCounter();

        // initial value is zero for an unpartitioned index
        assertEquals(0, counter.get());

        // get() does not have a side-effect on the counter.
        assertEquals(0, counter.get());

        // inc() increments the value and _then_ returns the counter.
        assertEquals(1, counter.incrementAndGet());
        assertEquals(1, counter.get());
        assertEquals(2, counter.incrementAndGet());
        
    	} finally {

    		store.destroy();
    	
    	}

    }

    /**
     * Unit test for overflow conditions at the int32 and int64 boundary for
     * {@link Counter}.
     */
    public void test_counter_overflow() {
        
    	final IRawStore store = new SimpleMemoryRawStore();
    	try {
    	
		final HTree btree = getHTree(store, 3/* addressBits */);

        final ICounter counter = btree.getCounter();

        final long maxSignedInt = (long) Integer.MAX_VALUE;
        final long minSignedInt = 0xFFFFFFFFL & (long) Integer.MIN_VALUE;
        final long minusOneInt = 0xFFFFFFFFL & (long) -1;
        final long int32Overflow = 1L << 32;// bit 33 is on.

        /*
         * First explore when the counter crosses from max signed int to min
         * signed int.
         */
        
        // Artificially set the counter to max signed int.
        btree.counter.set(maxSignedInt);

        // Verify current value.
        assertEquals(maxSignedInt, counter.get());

        // Increment. Should now be min signed int.
        assertEquals(minSignedInt, counter.incrementAndGet());

        // Increment. Should now be moving towards zero.
        assertEquals(minSignedInt + 1L, counter.incrementAndGet());

        // Increment. Should now be moving towards zero.
        assertEquals(minSignedInt + 2L, counter.incrementAndGet());

        /*
         * Now explore when the counter approaches a value which can only be
         * expressed in 33 bits (bigger than an unsigned int).
         */

        btree.counter.set(minusOneInt - 1);

        // Verify current value.
        assertEquals(minusOneInt - 1, counter.get());

        // Increment. Should now be a long whose lower word is minus one signed
        // int.
        assertEquals(minusOneInt, counter.incrementAndGet());

        // Increment. Should now be a long whose lower word is ZERO and whose
        // upper word as the low bit set.
        assertEquals(int32Overflow, counter.incrementAndGet());

        /*
         * Now explore when the counter approaches -1L and 0L.
         */
        btree.counter.set(-2L);

        // Verify current value.
        assertEquals(-2L, counter.get());

        // Increment to -1L.
        assertEquals(-1L, counter.incrementAndGet());

        // Increment to 0L fails (counter overflow).
        try {
            counter.incrementAndGet();
            fail("Expecting counter overflow");
        } catch (RuntimeException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    	} finally {
    		
    		store.destroy();
    	}
    	
    }
    
}
