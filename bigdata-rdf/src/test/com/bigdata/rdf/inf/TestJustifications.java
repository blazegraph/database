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
 * Created on Nov 3, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rdf.inf.Justification.VisitedSPOSet;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * Test suite for writing, reading, chasing and retracting {@link Justification}s.
 * 
 * FIXME isGroundedJustification - there is one simple test (directly proven)
 * but this needs more testing.
 * 
 * @todo more tests of the {@link JustificationArrayIterator}, including remove and
 *       with more than one justification for more than one statement (it is
 *       used when actually removing justifications by the
 *       {@link AbstractTripleStore}).
 * 
 * @todo test the comparator. it is especially important that that all
 *       justifications for the same entailment are clustered.
 *       <P>
 *       Check for overflow of long.
 *       <p>
 *       Check when this is a longer or shorter array than the other where the
 *       two justifications have the same data in common up to the end of
 *       whichever is the shorter array.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJustifications extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestJustifications() {
        super();
    }

    /**
     * @param name
     */
    public TestJustifications(String name) {
        super(name);
    }

    protected void assertEquals(Justification expected, Justification actual) {
        
        if(!expected.equals(actual)) {
            
            fail("Expecting "+expected+", not "+actual);
            
        }
        
    }
    
    /**
     * Creates a {@link Justification}, writes it on the store using an
     * {@link SPOAssertionBuffer}, verifies that we can read it back from the store, and
     * then retracts the justified statement and verifies that the justification
     * was also retracted.
     */
    public void test_writeReadRetract() {
        
        AbstractTripleStore store = getStore();
        
        try {

            /*
             * the explicit statement that is the support for the rule.
             */
            
            long U = store.addTerm(new URIImpl("http://www.bigdata.com/U"));
            long A = store.addTerm(new URIImpl("http://www.bigdata.com/A"));
            long Y = store.addTerm(new URIImpl("http://www.bigdata.com/Y"));
            
            store.addStatements(new SPO[] { new SPO(U, A, Y,
                    StatementEnum.Explicit) }, 1);
            
            assertTrue(store.hasStatement(U, A, Y));
            assertEquals(1,store.getStatementCount());
            
            InferenceEngine inf = store.getInferenceEngine();
            
            // the rule.
            Rule r = new RuleRdf01(inf);

            // the entailment.
            SPO head = new SPO(A, inf.rdfType.id, inf.rdfProperty.id,
                    StatementEnum.Inferred);
            
            long[] bindings = new long[] {
                    U, A, Y
            };
            
            Justification jst = new Justification(r, head, bindings);

            assertEquals(head,jst.getHead());

            assertEquals(
                    new SPO[]{new SPO(U,A,Y,StatementEnum.Inferred)},
                    jst.getTail()
                    );
            
            SPOAssertionBuffer buf = new SPOAssertionBuffer(store, store, null/* filter */,
                    100/* capacity */, true/* justified */);

            assertTrue(buf.add(head, jst));
            
            // no justifications before hand.
            assertEquals(0,store.getJustificationIndex().rangeCount(null,null));

            // flush the buffer.
            assertEquals(1,buf.flush());
            
            // one justification afterwards.
            assertEquals(1,store.getJustificationIndex().rangeCount(null,null));
            
            /*
             * verify read back from the index.
             */
            {
                
                IEntryIterator itr = store.getJustificationIndex().rangeIterator(null, null);
                
                while(itr.hasNext()) {
                    
                    // index only stores keys, not values.
                    assertNull( itr.next() );
                    
                    // get the key from the index.
                    final byte[] key = itr.getKey();
                    
                    // verify that we are reading back the same key that we should have written.
                    assertEquals(jst.getKey(new KeyBuilder()),key);
                    
                    // de-serialize the justification from the key.
                    Justification tmp = new Justification(key);
                    
                    // verify the same.
                    assertEquals(jst,tmp);
                    
                    // no more justifications in the index.
                    assertFalse(itr.hasNext());
                    
                }
                
            }

            /*
             * test iterator with a single justification. 
             */
            {
             
                SPOJustificationIterator itr = new SPOJustificationIterator(store,head);
                
                assertTrue(itr.hasNext());
                
                Justification tmp = itr.next();
                
                assertEquals(jst,tmp);
                
            }
            
            // an empty focusStore.
            TempTripleStore focusStore = new TempTripleStore(store.getProperties());
            
            /*
             * The inference (A rdf:type rdf:property) is grounded by the
             * explicit statement (U A Y).
             */

            assertTrue(Justification.isGrounded(inf, focusStore, store, head,
                    false/* testHead */, true/* testFocusStore */,
                    new VisitedSPOSet(focusStore.getBackingStore())));

            // add the statement (U A Y) to the focusStore.
            focusStore.addStatements(new SPO[] { new SPO(U, A, Y,
                    StatementEnum.Explicit) }, 1);

            /*
             * The inference is no longer grounded since we have declared that
             * we are also retracting its grounds.
             */
            assertFalse(Justification.isGrounded(inf, focusStore, store, head,
                    false/* testHead */, true/* testFocusStore */,
                    new VisitedSPOSet(focusStore.getBackingStore())));
            
            /*
             * remove the justified statements.
             */
            
            assertEquals(1,store.getAccessPath(head.s, head.p, head.o).removeAll());

            /*
             * verify that the justification for that statement is gone.
             */
            {
                
                IEntryIterator itr = store.getJustificationIndex().rangeIterator(null, null);
                
                assertFalse(itr.hasNext());
                
            }
            
        } finally {

            store.closeAndDelete();

        }

    }

}
