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

package com.bigdata.util;

import junit.framework.TestCase;

public class TestPseudoRandom extends TestCase {
	
	public void testSimpleGen() {
		testRange(7);
		testRange(123);
		testRange(25764);
        testRange(58000);
        testRange(2*1048576);
	}
	
    /**
     * Verify that the pseudo random number generator completely fills the
     * half-open range.
     * 
     * @param range
     *            The exclusive upper bound of the half-open range.
     */
    void testRange(final int range) {
        testRange(range, 1/* next */);
    }

    /**
     * Verify that the pseudo random number generator completely fills the
     * half-open range.
     * 
     * @param range
     *            The exclusive upper bound of the half-open range.
     * @param next
     *            The next value to visit.
     */
    void testRange(final int range, final int next) {
	    
	    final byte[] tst = new byte[range];
		
		final PseudoRandom psr = new PseudoRandom(range, next);
		
		for (int i = 0; i < range; i++) {
			// we want to test 0 - (range-1)
			final int nxt = psr.next();
			assertTrue(nxt <= range);
			assertTrue(tst[nxt] == 0);
			
			tst[nxt] = 1;
		}
    }

}
