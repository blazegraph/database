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
 * Created on Sep 16, 2010
 */

package com.bigdata.bop.engine;

import junit.framework.TestCase2;

import com.bigdata.io.SerializerUtil;

/**
 * Test suite for {@link BOpStats}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBOpStats extends TestCase2 {

    /**
     * 
     */
    public TestBOpStats() {

    }

    /**
     * @param name
     */
    public TestBOpStats(String name) {
        super(name);
    }

    public void test_stats() {

        final BOpStats totals = new BOpStats();

        final BOpStats stats = new BOpStats();
        
        assertEquals("chunksIn", 0L, stats.chunksIn.get());
        assertEquals("unitsIn", 0L, stats.unitsIn.get());
        assertEquals("unitsOut", 0L, stats.unitsOut.get());
        assertEquals("chunksOut", 0L, stats.chunksOut.get());
        assertEquals("mutationCount", 0L, totals.mutationCount.get());

        stats.chunksIn.increment(); // 1
        stats.unitsIn.increment(); 
        stats.unitsIn.increment(); //2 
        
        assertEquals("chunksIn", 1L, stats.chunksIn.get());
        assertEquals("unitsIn", 2L, stats.unitsIn.get());
        assertEquals("unitsOut", 0L, stats.unitsOut.get());
        assertEquals("chunksOut", 0L, stats.chunksOut.get());
        assertEquals("mutationCount", 0L, totals.mutationCount.get());

        stats.unitsOut.increment(); 
        stats.chunksOut.increment(); 
        stats.unitsOut.increment(); 
        stats.unitsOut.increment(); // 3
        stats.chunksOut.increment(); // 2
        
        assertEquals("chunksIn", 1L, stats.chunksIn.get());
        assertEquals("unitsIn", 2L, stats.unitsIn.get());
        assertEquals("unitsOut", 3L, stats.unitsOut.get());
        assertEquals("chunksOut", 2L, stats.chunksOut.get());
        assertEquals("mutationCount", 0L, totals.mutationCount.get());

        totals.add(stats);
        
        assertEquals("chunksIn", 1L, totals.chunksIn.get());
        assertEquals("unitsIn", 2L, totals.unitsIn.get());
        assertEquals("unitsOut", 3L, totals.unitsOut.get());
        assertEquals("chunksOut", 2L, totals.chunksOut.get());
        assertEquals("mutationCount", 0L, totals.mutationCount.get());

        stats.unitsIn.increment(); // 3
        
        totals.add(stats);
        
        assertEquals("chunksIn", 2L, totals.chunksIn.get());
        assertEquals("unitsIn", 5L, totals.unitsIn.get());
        assertEquals("unitsOut", 6L, totals.unitsOut.get());
        assertEquals("chunksOut", 4L, totals.chunksOut.get());
        assertEquals("mutationCount", 0L, totals.mutationCount.get());

        stats.mutationCount.increment();
        
        totals.add(stats);
        
        assertEquals("chunksIn", 3L, totals.chunksIn.get());
        assertEquals("unitsIn", 8L, totals.unitsIn.get());
        assertEquals("unitsOut", 9L, totals.unitsOut.get());
        assertEquals("chunksOut", 6L, totals.chunksOut.get());
        assertEquals("mutationCount", 1L, totals.mutationCount.get());

    }

    public void test_addToSelf() {
        
        final BOpStats stats = new BOpStats();
        
        assertEquals("chunksIn", 0L, stats.chunksIn.get());
        assertEquals("unitsIn", 0L, stats.unitsIn.get());
        assertEquals("unitsOut", 0L, stats.unitsOut.get());
        assertEquals("chunksOut", 0L, stats.chunksOut.get());

        stats.chunksIn.increment();
        stats.unitsIn.increment();
        stats.unitsIn.increment();
        
        assertEquals("chunksIn", 1L, stats.chunksIn.get());
        assertEquals("unitsIn", 2L, stats.unitsIn.get());
        assertEquals("unitsOut", 0L, stats.unitsOut.get());
        assertEquals("chunksOut", 0L, stats.chunksOut.get());

        // add to self.
        stats.add(stats);

        // verify no change.
        assertEquals("chunksIn", 1L, stats.chunksIn.get());
        assertEquals("unitsIn", 2L, stats.unitsIn.get());
        assertEquals("unitsOut", 0L, stats.unitsOut.get());
        assertEquals("chunksOut", 0L, stats.chunksOut.get());

    }

    public void test_serialization() {

        final BOpStats expected = new BOpStats();
        expected.elapsed.set(System.currentTimeMillis());
        expected.opCount.add(12);
        expected.chunksIn.add(1);
        expected.chunksOut.add(3);
        expected.unitsIn.add(4);
        expected.unitsOut.add(6);
        expected.typeErrors.add(8);
        expected.mutationCount.add(7);

        doSerializationTest(expected);
        
    }

    private static void doSerializationTest(final BOpStats expected) {

        final BOpStats actual = (BOpStats) SerializerUtil
                .deserialize(SerializerUtil.serialize(expected));

        assertSameStats(expected, actual);
        
    }

    private static void assertSameStats(final BOpStats expected,
            final BOpStats actual) {

        assertEquals("elapsed", expected.elapsed.get(), actual.elapsed.get());

        assertEquals("opCount", expected.opCount.get(), actual.opCount.get());
        
        assertEquals("chunksIn", expected.chunksIn.get(), actual.chunksIn.get());
        
        assertEquals("chunksOut", expected.chunksOut.get(),
                actual.chunksOut.get());
        
        assertEquals("unitsIn", expected.unitsIn.get(), actual.unitsIn.get());
        
        assertEquals("unitsOut", expected.unitsOut.get(), actual.unitsOut.get());
        
        assertEquals("typeErrors", expected.typeErrors.get(),
                actual.typeErrors.get());

        assertEquals("mutationCount", expected.mutationCount.get(),
                actual.mutationCount.get());

    }

}
