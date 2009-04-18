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
 * Created on Apr 18, 2009
 */

package com.bigdata.relation.accesspath;

import junit.framework.TestCase2;

import com.bigdata.striterator.IChunkedIterator;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnsynchronizedUnboundedChunkBuffer extends TestCase2 {

    /**
     * 
     */
    public TestUnsynchronizedUnboundedChunkBuffer() {
       
    }

    /**
     * @param arg0
     */
    public TestUnsynchronizedUnboundedChunkBuffer(String arg0) {
        super(arg0);

    }

    /**
     * Test empty iterator.
     */
    public void test_emptyIterator() {
        
        final UnsynchronizedUnboundedChunkBuffer<String> buffer = new UnsynchronizedUnboundedChunkBuffer<String>(
                3/* chunkCapacity */);

        // the iterator is initially empty.
        assertFalse(buffer.iterator().hasNext());
        
    }
    
    /**
     * Verify that elements are flushed when an iterator is requested so
     * that they will be visited by the iterator.
     */
    public void test_bufferFlushedByIterator() {
        
        final UnsynchronizedUnboundedChunkBuffer<String> buffer = new UnsynchronizedUnboundedChunkBuffer<String>(
                3/* chunkCapacity */);
        
        buffer.add("a");
        
        assertSameIterator(new String[] { "a" }, buffer.iterator());
                
    }
    
    /**
     * Verify that the iterator has snapshot semantics.
     */
    public void test_snapshotIterator() {
        
        final UnsynchronizedUnboundedChunkBuffer<String> buffer = new UnsynchronizedUnboundedChunkBuffer<String>(
                3/* chunkCapacity */);
        
        buffer.add("a");
        buffer.add("b");
        buffer.add("c");

        // visit once.
        assertSameIterator(new String[] { "a", "b", "c" }, buffer.iterator());

        // will visit again.
        assertSameIterator(new String[] { "a", "b", "c" }, buffer.iterator());

    }

    /**
     * Verify iterator visits chunks as placed onto the queue.
     */
    public void test_chunkedIterator() {
        
        final UnsynchronizedUnboundedChunkBuffer<String> buffer = new UnsynchronizedUnboundedChunkBuffer<String>(
                3/* chunkCapacity */);
        
        buffer.add("a");
        
        buffer.flush();
        
        buffer.add("b");
        buffer.add("c");

        // visit once.
        assertSameChunkedIterator(new String[][] { new String[] { "a" },
                new String[] { "b", "c" } }, buffer.iterator());

    }
    
    /**
     * Test class of chunks created by the iterator (the array class should be
     * taken from the first visited chunk's class).
     */
    @SuppressWarnings("unchecked")
    public void test_chunkClass() {

        final UnsynchronizedUnboundedChunkBuffer buffer = new UnsynchronizedUnboundedChunkBuffer(
                3/* chunkCapacity */);
        
        buffer.add("a");
        
        buffer.flush();
        
        buffer.add("b");
        buffer.add("c");

        // visit once.
        assertSameChunkedIterator(new String[][] { new String[] { "a" },
                new String[] { "b", "c" } }, buffer.iterator());
        
    }

    /**
     * Verify that the iterator visits the expected chunks in the expected
     * order.
     * 
     * @param <E>
     * @param chunks
     * @param itr
     */
    protected <E> void assertSameChunkedIterator(final E[][] chunks,
            final IChunkedIterator<E> itr) {

        for(E[] chunk : chunks) {
            
            assertTrue(itr.hasNext());
            
            final E[] actual = itr.nextChunk();

            assertSameArray(chunk, actual);
            
        }
        
        assertFalse(itr.hasNext());
        
    }

}
