/*

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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.util.Arrays;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link Appender}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAppendFilter extends TestCase2 {

    /**
     * 
     */
    public TestAppendFilter() {
     
    }

    /**
     * @param arg0
     */
    public TestAppendFilter(String arg0) {
        super(arg0);
     
    }

    public void test_filter() {

        final IChunkedIterator<Long> actual = (IChunkedIterator<Long>) new ChunkedStriterator<IChunkedIterator<Long>, Long>(
                Arrays.asList(new Long[] { 1L, 3L, 5L }).iterator())
                .addFilter(new Appender<IChunkedIterator<Long>, Long>(
                        new ChunkedWrappedIterator<Long>(Arrays.asList(
                                new Long[] { 2L, 3L, 4L }).iterator())));

        assertEquals(new Long[] { //
                1L, 3L, 5L,  // src1
                2L, 3L, 4L,  // src2
                }, actual.nextChunk());

    }

}
