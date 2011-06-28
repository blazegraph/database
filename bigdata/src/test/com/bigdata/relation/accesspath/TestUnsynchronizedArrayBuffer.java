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
 * Created on Sep 10, 2008
 */

package com.bigdata.relation.accesspath;

import junit.framework.TestCase2;

/**
 * Test suite for the {@link UnsynchronizedArrayBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnsynchronizedArrayBuffer extends TestCase2 {

    /**
     * 
     */
    public TestUnsynchronizedArrayBuffer() {
   
    }

    /**
     * @param arg0
     */
    public TestUnsynchronizedArrayBuffer(String arg0) {
   
        super(arg0);
   
    }

    final int chunkCapacity = 10;
    
    final int chunkOfChunksCapacity = 5;
    
    TestArrayBuffer<String[]> syncBuffer = new TestArrayBuffer<String[]>(
            chunkOfChunksCapacity);

    /**
     * Helper class exposes the backing buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    protected static class TestArrayBuffer<E> extends AbstractArrayBuffer<E> {

        TestArrayBuffer(final int capacity) {
            
            super(capacity, Object.class, null/* filter */);
            
        }

        @Override
        protected long flush(final int n, final E[] a) {

            fail("Not expecting the syncBuffer to be flushed by this test");

            return 0;

        }

        /**
         * Return the chunk by reference at the given index.
         * 
         * @param index
         *            The index into the internal buffer.
         *            
         * @return The chunk at that index.
         */
        public E get(final int index) {

//            if (index < 0 || index >= size)
//                throw new IndexOutOfBoundsException("index=" + index);
            
            return buffer[index];
            
        }
        
    }

    UnsynchronizedArrayBuffer<String> unsyncBuffer = new UnsynchronizedArrayBuffer<String>(
            syncBuffer, chunkCapacity);

    /**
     * Test verifies state of both the {@link UnsynchronizedArrayBuffer} and the
     * target {@link IBuffer} before and after an element is added to the
     * {@link UnsynchronizedArrayBuffer} and after it is
     * {@link UnsynchronizedArrayBuffer#flush() flushed} to target
     * {@link IBuffer}.
     */
    public void test_unsynchronizedBuffer1() {
        
        assertTrue("isEmpty",unsyncBuffer.isEmpty());
        
        assertEquals("size",0,unsyncBuffer.size());

        assertTrue("isEmpty",syncBuffer.isEmpty());
        
        assertEquals("size",0,syncBuffer.size());

        unsyncBuffer.add("a");
        
        assertFalse("isEmpty",unsyncBuffer.isEmpty());
        
        assertEquals("size",1,unsyncBuffer.size());
        
        assertTrue("isEmpty",syncBuffer.isEmpty());
        
        assertEquals("size",0,syncBuffer.size());

        assertEquals("flush",1,unsyncBuffer.flush());

        assertTrue("isEmpty",unsyncBuffer.isEmpty());
        
        assertEquals("size",0,unsyncBuffer.size());
        
        assertFalse("isEmpty",syncBuffer.isEmpty());
        
        assertEquals("size",1,syncBuffer.size());
        
        final String[] chunk0 = syncBuffer.get(0);
        
        assertEquals(new String[] { "a" }, chunk0);
        
    }

    public void test_unsynchronizedBuffer2() {

        unsyncBuffer.add("a");
        unsyncBuffer.add("b");

        assertEquals("size", 2, unsyncBuffer.size());

        assertEquals("flush", 2, unsyncBuffer.flush());

        assertEquals("size", 0, unsyncBuffer.size());

        assertEquals("size", 1, syncBuffer.size());

        assertEquals(new String[] { "a", "b" }, syncBuffer.get(0));

        unsyncBuffer.add("c");
        
        assertEquals("size", 1, unsyncBuffer.size());

        assertEquals("flush", 2+1, unsyncBuffer.flush());

        assertEquals("size", 0, unsyncBuffer.size());

        assertEquals("size", 2, syncBuffer.size());

        assertEquals(new String[] { "a", "b" }, syncBuffer.get(0));

        assertEquals(new String[] { "c" }, syncBuffer.get(1));

        // Note: flush does not change anything since buffer is empty.
        assertEquals("flush", 2+1, unsyncBuffer.flush());
        
        assertEquals("size", 0, unsyncBuffer.size());

        assertEquals("size", 2, syncBuffer.size());
        
        assertEquals(new String[] { "a", "b" }, syncBuffer.get(0));

        assertEquals(new String[] { "c" }, syncBuffer.get(1));
        
    }

}
