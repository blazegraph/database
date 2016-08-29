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
 * Created on May 27, 2011
 */

package com.bigdata.rdf.internal;

import junit.framework.TestCase;

/**
 * Test suite for {@link VTE}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestVTE extends TestCase {

    /**
     * 
     */
    public TestVTE() {
    }

    /**
     * @param name
     */
    public TestVTE(String name) {
        super(name);
    }

    /**
     * Unit test for {@link VTE} verifies that the
     * correspondence between the enumerated types and the internal values is
     * correct (self-consistent).
     */
    public void test_VTE_selfConsistent() {
       
        for(VTE e : VTE.values()) {

            assertTrue("expected: " + e + " (v=" + e.v + "), actual="
                    + VTE.valueOf(e.v),
                    e == VTE.valueOf(e.v));

        }
        
    }
    
    /**
     * Unit test for {@link VTE} verifies that all legal byte
     * values decode to an internal value type enum (basically, this checks that
     * we mask the two lower bits).
     */
    public void test_VTE_decodeNoErrors() {

        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            
            assertNotNull(VTE.valueOf((byte) i));
            
        }
        
    }
    
}
