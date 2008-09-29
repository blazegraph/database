/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.store;

import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Test suite for {@link IAccessPath}.
 * <p>
 * See also {@link TestTripleStore} which tests some of this stuff.
 * 
 * FIXME write tests for SLICE with non-zero offset and non-zero LIMIT.
 * 
 * FIXME write tests for SLICE where the maximum fully buffered limit is
 * exceeded so we are forced to use the asynchronous iterator on
 * {@link AbstractAccessPath}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAccessPath extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestAccessPath() {
        super();
    }

    /**
     * @param name
     */
    public TestAccessPath(String name) {
        super(name);
    }

    /**
     * There are 8 distinct triple pattern bindings for a triple store that
     * select among 3 distinct access paths.
     * 
     * @todo for a quad store there are 16 distinct binding patterns that select
     *       among 6 distinct access paths.
     */
    public void test_getAccessPath() {
       
        AbstractTripleStore store = getStore();

        try {
            
            final SPORelation r = store.getSPORelation();

            assertEquals(SPOKeyOrder.SPO, r.getAccessPath(NULL, NULL, NULL)
                    .getKeyOrder());

            assertEquals(SPOKeyOrder.SPO, r.getAccessPath(1, NULL, NULL)
                    .getKeyOrder());

            assertEquals(SPOKeyOrder.SPO, r.getAccessPath(1, 1, NULL)
                    .getKeyOrder());

            assertEquals(SPOKeyOrder.SPO, r.getAccessPath(1, 1, 1)
                    .getKeyOrder());

            assertEquals(SPOKeyOrder.POS, r.getAccessPath(NULL, 1, NULL)
                    .getKeyOrder());

            assertEquals(SPOKeyOrder.POS, r.getAccessPath(NULL, 1, 1)
                    .getKeyOrder());

            assertEquals(SPOKeyOrder.OSP, r.getAccessPath(NULL, NULL, 1)
                    .getKeyOrder());

            assertEquals(SPOKeyOrder.OSP, r.getAccessPath(1, NULL, 1)
                    .getKeyOrder());

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * @todo write tests of slice where offset=0, offset>0. test with limit at
     *       fence posts (0,1) and with limit GT the maximum that can be fully
     *       buffered. verify stable result sets by using a slice to page
     *       through the results.
     */
    public void test_slice() {
        
        fail("write tests");
        
    }
    
}
