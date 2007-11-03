/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 3, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

/**
 * Test suite for writing, reading, chasing and retracting {@link Justification}s.
 * 
 * @todo test out the {@link JustificationIterator}, including remove and with
 *       more than one justification for more than one statement.
 * 
 * @todo do test for isGroundedJustification() or possibly a proof chain
 *       iterator.
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
 * @todo isGroundedJustification
 * 
 * @todo ProofIterator
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
     * {@link SPOBuffer}, verifies that we can read it back from the store, and
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
            
            InferenceEngine inf = new InferenceEngine(store.getProperties(),
                    store);
            
            // the rule.
            Rule r = new RuleRdf01(inf);

            // the entailment.
            SPO head = new SPO(A, inf.rdfType.id, inf.rdfProperty.id,
                    StatementEnum.Inferred);
            
            long[] bindings = new long[] {
                    U, A, Y
            };
            
            Justification jst = new Justification(r, head, bindings);

            SPOBuffer buf = new SPOBuffer(store, null/* filter */,
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
                    
                    // nothing store in the value under the key.
                    assertNull( itr.next() );
                    
                    // get the key from the index.
                    byte[] key = itr.getKey();
                    
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
             
                JustificationIterator itr = new JustificationIterator(store,head);
                
                assertTrue(itr.hasNext());
                
                Justification tmp = itr.next();
                
                assertEquals(jst,tmp);
                
            }
            
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
