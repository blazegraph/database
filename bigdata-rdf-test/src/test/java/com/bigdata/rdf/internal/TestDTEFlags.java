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
 * Test suite for {@link DTEFlags}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDTEFlags extends TestCase {

    /**
     * 
     */
    public TestDTEFlags() {
    }

    /**
     * @param name
     */
    public TestDTEFlags(String name) {
        super(name);
    }

    /**
     * The {@link DTEFlags} must be distinct bit combinations.
     */
    public void test_DTEFlags() {
        assertNotSame(DTEFlags.NUMERIC, DTEFlags.NOFLAGS);
        assertNotSame(DTEFlags.UNSIGNED_NUMERIC, DTEFlags.NOFLAGS);
        assertNotSame(DTEFlags.UNSIGNED_NUMERIC, DTEFlags.NUMERIC);
    }

    public void test_DTEFlags_NOFLAGS() {
        assertEquals(0, DTEFlags.NOFLAGS);
    }

    public void test_DTEFlags_NUMERIC() {
        assertEquals(1, DTEFlags.NUMERIC);
    }

    public void test_DTEFlags_UNSIGNED() {
        assertEquals(3, DTEFlags.UNSIGNED_NUMERIC);
    }
    
}
