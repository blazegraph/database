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
 * Created on Sep 3, 2008
 */

package com.bigdata.striterator;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rawstore.Bytes;

/**
 * Unit tests for {@link DistinctFilter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDistinctFilter extends TestCase2 {

    /**
     * 
     */
    public TestDistinctFilter() {
    }

    /**
     * @param arg0
     */
    public TestDistinctFilter(String arg0) {
        super(arg0);
    }

    /**
     * Unit test where all elements are visited in the first chunk. This case is
     * optimized to avoid the creation of the {@link BTree}.
     */
    public void test_distinctOneChunk() {
        
        final IIndexManager indexManager = new TemporaryStore();

        final IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
        
        final DistinctFilter<Long> filter = new DistinctFilter<Long>(
                indexManager) {

            @Override
            protected byte[] getSortKey(Long e) {

                return keyBuilder.reset().append(e.longValue()).getKey();

            }

        };

        final Long[] a = {

        12L, 1L, 3L, 1L

        };

        final IChunkedOrderedIterator<Long> src = new ChunkedArrayIterator<Long>(
                a.length, a, null/* keyOrder */);

        final IChunkedOrderedIterator<Long> dst = new ChunkedConvertingIterator<Long, Long>(
                src, filter);

        assertSameIterator(new Long[] { 1L, 3L, 12L }, dst);
        
    }

    /**
     * Unit test where the source iterator is empty.
     */
    public void test_distinctOneChunkEmptyIterator() {

        final IIndexManager indexManager = new TemporaryStore();

        final IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
        
        final DistinctFilter<Long> filter = new DistinctFilter<Long>(
                indexManager) {

            @Override
            protected byte[] getSortKey(Long e) {

                return keyBuilder.reset().append(e.longValue()).getKey();

            }

        };

        final Long[] a = {

        };

        final IChunkedOrderedIterator<Long> src = new ChunkedArrayIterator<Long>(
                a.length, a, null/* keyOrder */);

        final IChunkedOrderedIterator<Long> dst = new ChunkedConvertingIterator<Long, Long>(
                src, filter);

        assertSameIterator(new Long[] { }, dst);

    }
    
    /**
     * Unit test where multiple chunks are processed. One of the chunks consists
     * entirely of duplicate elements.
     */
    public void test_distinctManyChunks() {
        
        final IIndexManager indexManager = new TemporaryStore();

        final IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
        
        final DistinctFilter<Long> filter = new DistinctFilter<Long>(
                indexManager) {

            @Override
            protected byte[] getSortKey(Long e) {

                return keyBuilder.reset().append(e.longValue()).getKey();

            }

        };

        final Long[][] a = {

                // 1st chunk
                new Long[]{12L, 1L, 3L, 1L},
                
                // 2nd chunk (only 4 is new).
                new Long[]{3L, 4L, 12L},
                
                // 3rd chunk (all duplicates).
                new Long[]{12L, 1L, 3L, 4L},
                
                // 4th chunk (only 5 is new).
                new Long[]{5L, 12L, 4L}

        };

        final IChunkedOrderedIterator<Long> src = new MyChunkSource<Long>(a);

        final IChunkedOrderedIterator<Long> dst = new ChunkedConvertingIterator<Long, Long>(
                src, filter);

        assertSameIterator(new Long[] {
                
                /* 1st chunk*/
                1L, 3L, 12L,
                
                /* 2nd chunk*/
                4L,
                
                /* 3rd chunk is empty*/
                
                /* 4th chunk */
                5L
                
                }, dst);
                
    }
    
    /**
     * Helper class visits a sequence of chunks specified to its ctor.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    static class MyChunkSource<E> implements IChunkedOrderedIterator<E> {

        final private E[][] chunks;

        private int i = 0;
        
        public MyChunkSource(E[][] chunks) {

            this.chunks = chunks;
            
        }
        
        public IKeyOrder<E> getKeyOrder() {
        
            return null;
            
        }

        public E[] nextChunk(IKeyOrder<E> keyOrder) {
            
            throw new UnsupportedOperationException();
            
        }

        public E next() {
            
            throw new UnsupportedOperationException();
            
        }

        public E[] nextChunk() {
            
            return chunks[i++];
            
        }

        public void remove() {

            throw new UnsupportedOperationException();
            
        }

        public void close() {

            // NOP.
            
        }

        public boolean hasNext() {
            
            return i < chunks.length;
            
        }
        
    }
    
}
