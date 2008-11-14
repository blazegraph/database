/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 25, 2008
 */

package com.bigdata.rdf.rules;

import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TripleStoreUtility;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Test suite for {@link TripleStoreUtility#modelsEqual(AbstractTripleStore, AbstractTripleStore)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestModelsEqual extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestModelsEqual() {
    }

    /**
     * @param name
     */
    public TestModelsEqual(String name) {
        super(name);
    }

    /**
     * Test compares two stores with the same data, then removes a statement
     * from one store and re-compares the stores to verify that the
     * inconsistency is detected.
     * 
     * @throws Exception
     */
    public void test_modelsEqual() throws Exception {

        final AbstractTripleStore store1 = getStore();
        final AbstractTripleStore store2 = getStore();
        assertTrue(store1 != store2);
        try {

            // write statements on one store.
            {
                final BigdataValueFactory f = store1.getValueFactory();
                
                final BigdataURI A = f.createURI("http://www.bigdata.com/a");
                final BigdataURI B = f.createURI("http://www.bigdata.com/b");
                final BigdataURI C = f.createURI("http://www.bigdata.com/c");
                final BigdataURI D = f.createURI("http://www.bigdata.com/d");
                final BigdataURI SCO = f.asValue(RDFS.SUBCLASSOF);

                StatementBuffer buf = new StatementBuffer(store1, 10);

                buf.add(A, SCO, B);
                buf.add(B, SCO, C);
                buf.add(C, SCO, D);

                buf.flush();
            }

            // write the same statements on the other store.
            {
                final BigdataValueFactory f = store1.getValueFactory();
                
                final BigdataURI A = f.createURI("http://www.bigdata.com/a");
                final BigdataURI B = f.createURI("http://www.bigdata.com/b");
                final BigdataURI C = f.createURI("http://www.bigdata.com/c");
                final BigdataURI D = f.createURI("http://www.bigdata.com/d");
                final BigdataURI SCO = f.asValue(RDFS.SUBCLASSOF);

                StatementBuffer buf = new StatementBuffer(store2, 10);

                buf.add(A, SCO, B);
                buf.add(B, SCO, C);
                buf.add(C, SCO, D);

                buf.flush();

            }

            // verify all in store1 also found in store2.
            {
                final ICloseableIterator<BigdataStatement> notFoundItr = TripleStoreUtility.notFoundInTarget(store1,
                        store2);
                try {
                    assertFalse(notFoundItr.hasNext());
                } finally {
                    notFoundItr.close();
                }
            }

            // verify all in store2 also found in store1.
            {
                final ICloseableIterator<BigdataStatement> notFoundItr = TripleStoreUtility.notFoundInTarget(store2,
                        store1);
                try {
                    assertFalse(notFoundItr.hasNext());
                } finally {
                    notFoundItr.close();
                }
            }
            
            // high-level test.
            assertTrue(TripleStoreUtility.modelsEqual(store1, store2));

            // now remove one statement from store2.
            {
                
                final BigdataValueFactory f = store1.getValueFactory();
                
                final BigdataURI A = f.createURI("http://www.bigdata.com/a");
                final BigdataURI B = f.createURI("http://www.bigdata.com/b");
                final BigdataURI SCO = f.asValue(RDFS.SUBCLASSOF);
                
                assertEquals(1L, store2.removeStatements(A, SCO, B));
                
                assertFalse(store2.hasStatement(A, SCO, B));
                
                log.info("Removed one statement from store2.");
                
            }

            // verify one in store1 NOT FOUND in store2.
            {
                final ICloseableIterator<BigdataStatement> notFoundItr = TripleStoreUtility.notFoundInTarget(
                        store1, store2);
                int nnotFound = 0;
                try {
                    while (notFoundItr.hasNext()) {
                        notFoundItr.next();
                        nnotFound++;
                    }
                } finally {
                    notFoundItr.close();
                }
                assertEquals("#notFound", 1, nnotFound);
            }

            // verify all in store2 found in store1.
            {
                final ICloseableIterator<BigdataStatement> notFoundItr = TripleStoreUtility.notFoundInTarget(
                        store2, store1);
                try {
                    assertFalse(notFoundItr.hasNext());
                } finally {
                    notFoundItr.close();
                }
            }
            
            // high-level test.
            assertFalse(TripleStoreUtility.modelsEqual(store1, store2));

        } finally {
            store1.close();
            store2.close();
        }
    }

}
