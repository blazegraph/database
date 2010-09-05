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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.aggregation;

import junit.framework.TestCase2;

/**
 * Unit tests for the {@link MemorySortOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMemorySortOp extends TestCase2 {

    /**
     * 
     */
    public TestMemorySortOp() {
    }

    /**
     * @param name
     */
    public TestMemorySortOp(String name) {
        super(name);
    }

    /**
     * @todo unit tests for the in-memory sort operator. These tests should not
     *       focus on SPARQL semantics. Instead, just test the ability to impose
     *       the appropriate {@link ISortOrder}[] on some in-memory binding
     *       sets.
     */
    public void test_something() {

        fail("write tests");
        
    }
    
}
