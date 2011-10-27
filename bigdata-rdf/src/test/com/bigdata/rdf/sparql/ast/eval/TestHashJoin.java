/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;


/**
 * Test suite for queries designed to exercise a hash join against an access
 * path.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHashJoin extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestHashJoin() {
    }

    /**
     * @param name
     */
    public TestHashJoin(String name) {
        super(name);
    }

    /**
     * A simple SELECT query with a query hint which should force the choice of
     * a hash join for one of the predicates.
     * <p>
     * Note: This does not verify that a hash join was actually used, but if the
     * query hint is interpreted properly then it should use a hash join.
     */
    public void test_hash_join_1() throws Exception {

        new TestHelper("hash-join-1").runTest();
        
    }
    
}
