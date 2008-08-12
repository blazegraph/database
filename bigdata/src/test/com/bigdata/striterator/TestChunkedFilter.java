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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.util.Arrays;

import junit.framework.TestCase2;

import com.bigdata.striterator.ChunkedFilter;
import com.bigdata.striterator.ChunkedStriterator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * Unit tests for {@link ChunkedFilter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestChunkedFilter extends TestCase2 {

    /**
     * 
     */
    public TestChunkedFilter() {
    }

    /**
     * @param arg0
     */
    public TestChunkedFilter(String arg0) {
        super(arg0);
    }

    /**
     * Unit test for chunk-at-a-time rewrite (multiplies the values in the chunk
     * by two).
     */
    public void test_filter() {
        
        IChunkedIterator<Long> itr = new ChunkedStriterator(Arrays.asList(
                new Long[] { 1L, 2L, 3L }).iterator())
                .addFilter(new ChunkedFilter<IChunkedIterator<Long>, Long, Long>() {
                    @Override
                    protected Long[] filterChunk(Long[] chunk) {
                        for (int i = 0; i < chunk.length; i++) {
                            chunk[i] *= 2;
                        }
                        return chunk;
                    }
                });

        assertEquals(new Long[] { 2L, 4L, 6L }, itr.nextChunk());
        
    }

    /**
     * Unit test for correct rendering of a partial chunk when an element has
     * already been drawn from the iterator.
     */
    public void test_filter2() {

        IChunkedIterator<Long> itr = new ChunkedStriterator(Arrays.asList(
                new Long[] { 1L, 2L, 3L }).iterator())
                .addFilter(new ChunkedFilter<IChunkedIterator<Long>, Long, Long>() {
                    @Override
                    protected Long[] filterChunk(Long[] chunk) {
                        for (int i = 0; i < chunk.length; i++) {
                            chunk[i] *= 2;
                        }
                        return chunk;
                    }
                });

        assertEquals(Long.valueOf(2L), itr.next());

        assertEquals(new Long[] { 4L, 6L }, itr.nextChunk());

    }
    
}
