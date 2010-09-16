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
 * Created on Sep 16, 2010
 */

package com.bigdata.bop.engine;

import junit.framework.TestCase2;

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

        stats.chunksIn.increment();
        stats.unitsIn.increment();
        stats.unitsIn.increment();
        
        assertEquals("chunksIn", 1L, stats.chunksIn.get());
        assertEquals("unitsIn", 2L, stats.unitsIn.get());
        assertEquals("unitsOut", 0L, stats.unitsOut.get());
        assertEquals("chunksOut", 0L, stats.chunksOut.get());

        stats.unitsOut.increment();
        stats.chunksOut.increment();
        stats.unitsOut.increment();
        stats.unitsOut.increment();
        stats.chunksOut.increment();
        
        assertEquals("chunksIn", 1L, stats.chunksIn.get());
        assertEquals("unitsIn", 2L, stats.unitsIn.get());
        assertEquals("unitsOut", 3L, stats.unitsOut.get());
        assertEquals("chunksOut", 2L, stats.chunksOut.get());

        totals.add(stats);
        
        assertEquals("chunksIn", 1L, totals.chunksIn.get());
        assertEquals("unitsIn", 2L, totals.unitsIn.get());
        assertEquals("unitsOut", 3L, totals.unitsOut.get());
        assertEquals("chunksOut", 2L, totals.chunksOut.get());

        stats.unitsIn.increment();
        
        totals.add(stats);
        
        assertEquals("chunksIn", 2L, totals.chunksIn.get());
        assertEquals("unitsIn", 5L, totals.unitsIn.get());
        assertEquals("unitsOut", 6L, totals.unitsOut.get());
        assertEquals("chunksOut", 4L, totals.chunksOut.get());

    }
    
}
