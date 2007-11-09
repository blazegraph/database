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

import java.util.Arrays;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Test suite for {@link IAccessPath}.
 * <p>
 * See also {@link TestTripleStore} which tests some of this stuff.
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

            assertEquals(KeyOrder.SPO, store.getAccessPath(NULL, NULL, NULL)
                    .getKeyOrder());

            assertEquals(KeyOrder.SPO, store.getAccessPath(1, NULL, NULL)
                    .getKeyOrder());

            assertEquals(KeyOrder.SPO, store.getAccessPath(1, 1, NULL)
                    .getKeyOrder());

            assertEquals(KeyOrder.SPO, store.getAccessPath(1, 1, 1)
                    .getKeyOrder());

            assertEquals(KeyOrder.POS, store.getAccessPath(NULL, 1, NULL)
                    .getKeyOrder());

            assertEquals(KeyOrder.POS, store.getAccessPath(NULL, 1, 1)
                    .getKeyOrder());

            assertEquals(KeyOrder.OSP, store.getAccessPath(NULL, NULL, 1)
                    .getKeyOrder());

            assertEquals(KeyOrder.OSP, store.getAccessPath(1, NULL, 1)
                    .getKeyOrder());

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * Unit test for {@link IAccessPath#distinctTermScan()}
     */
    public void test_getDistinctTermIdentifiers() {

        AbstractTripleStore store = getStore();

        try {

            IStatementBuffer buffer = new StatementBuffer(store,
                    100/* capacity */);

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");
            URI D = new URIImpl("http://www.foo.org/D");
            URI E = new URIImpl("http://www.foo.org/E");

            buffer.add(A, B, C);
            buffer.add(C, B, D);
            buffer.add(A, E, C);

            // flush statements to the store.
            buffer.flush();

            assertTrue(store.hasStatement(A, B, C));
            assertTrue(store.hasStatement(C, B, D));
            assertTrue(store.hasStatement(A, E, C));

            // distinct subject term identifiers.
            {

                Long[] expected = new Long[] {

                store.getTermId(A), store.getTermId(C)

                };

                // term identifers will be in ascending order.
                Arrays.sort(expected);

                assertSameItr(expected, store.getAccessPath(KeyOrder.SPO)
                        .distinctTermScan());

            }

            // distinct predicate term identifiers.
            {

                Long[] expected = new Long[] {

                store.getTermId(B), store.getTermId(E)

                };

                // term identifers will be in ascending order.
                Arrays.sort(expected);

                assertSameItr(expected, store.getAccessPath(KeyOrder.POS)
                        .distinctTermScan());

            }

            // distinct object term identifiers.
            {

                Long[] expected = new Long[] {

                store.getTermId(C), store.getTermId(D)

                };

                // term identifers will be in ascending order.
                Arrays.sort(expected);

                assertSameItr(expected, store.getAccessPath(KeyOrder.OSP)
                        .distinctTermScan());

            }

        } finally {

            store.closeAndDelete();

        }

    }

}
