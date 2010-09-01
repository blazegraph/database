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
 * Created on Sep 1, 2010
 */

package com.bigdata.bop.engine;

import junit.framework.TestCase2;

/**
 * Test suite for DISTINCT, ORDER BY, and GROUP BY operators.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Only test those aspects here which pertain to interactions with the
 *       {@link QueryEngine}, especially distributed evaluation of these
 *       operators.
 */
public class TestQueryEngine2 extends TestCase2 {

    /**
     * 
     */
    public TestQueryEngine2() {
    }

    /**
     * @param name
     */
    public TestQueryEngine2(String name) {
        super(name);
    }

    /**
     * @todo Unit test for DISTINCT solutions (replace the DISTINCT binding sets
     *       with a hash map based operator).
     */
    public void test_query_distinct() {
        
        fail("write test");
        
    }

    /**
     * @todo Unit test for ORDER BY over solutions (in memory merge sort
     *       operator for now, followed by an external merge sort operator
     *       later).
     */
    public void test_query_orderBy() {
        
        fail("write test");
        
    }

    /**
     * @todo Unit test for GROUP BY over solutions (rollup against an in memory
     *       fact table for now).
     */
    public void test_query_groupBy() {
        
        fail("write test");
        
    }

}
