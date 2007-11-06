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
 * Created on Nov 5, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.inf.TMStatementBuffer.BufferEnum;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * Test suite for {@link TMStatementBuffer}.
 * 
 * @todo verify that we can downgrade an explicit statement to an inferred one.
 * 
 * @todo write an explicit test of
 *       {@link TMStatementBuffer#applyExistingStatements(AbstractTripleStore, AbstractTripleStore)}
 *       or the private variant of that method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTMStatementBuffer extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestTMStatementBuffer() {
        super();
    }

    /**
     * @param name
     */
    public TestTMStatementBuffer(String name) {
        super(name);
    }

    /**
     * Test for {@link TMStatementBuffer#applyExistingStatements(AbstractTripleStore, AbstractTripleStore, ISPOFilter filter)}.
     */
    public void test_filter_01() {

        AbstractTripleStore store = getStore();

        try {

            /*
             * Setup the database.
             */
            {
                SPOAssertionBuffer buf = new SPOAssertionBuffer(store,
                        null/* filter */, 100/* capacity */, false/* justified */);

                buf.add(new SPO(1, 2, 3, StatementEnum.Inferred));
                
                buf.add(new SPO(2, 2, 3, StatementEnum.Explicit));

                buf.flush();

                assertTrue(store.hasStatement(1, 2, 3));
                assertTrue(store.getStatement(1, 2, 3).isInferred());
                
                assertTrue(store.hasStatement(2, 2, 3));
                assertTrue(store.getStatement(2, 2, 3).isExplicit());

                assertEquals(2,store.getStatementCount());

            }
            
            /*
             * Setup a temporary store.
             */
            TempTripleStore focusStore = new TempTripleStore(store.getProperties());
            {
            
                SPOAssertionBuffer buf = new SPOAssertionBuffer(focusStore,
                        null/* filter */, 100/* capacity */, false/* justified */);

                // should be applied to the database since already there as inferred.
                buf.add(new SPO(1, 2, 3, StatementEnum.Explicit));
                
                // should be applied to the database since already there as explicit.
                buf.add(new SPO(2, 2, 3, StatementEnum.Explicit));

                // should not be applied to the database since not there at all.
                buf.add(new SPO(3, 2, 3, StatementEnum.Explicit));

                buf.flush();

                assertTrue(focusStore.hasStatement(1, 2, 3));
                assertTrue(focusStore.getStatement(1, 2, 3).isExplicit());
                
                assertTrue(focusStore.hasStatement(2, 2, 3));
                assertTrue(focusStore.getStatement(2, 2, 3).isExplicit());
                
                assertTrue(focusStore.hasStatement(3, 2, 3));
                assertTrue(focusStore.getStatement(3, 2, 3).isExplicit());

                assertEquals(3,focusStore.getStatementCount());

            }

            /*
             * For each (explicit) statement in the focusStore that also exists
             * in the database: (a) if the statement is not explicit in the
             * database then mark it as explicit; and (b) remove the statement
             * from the focusStore.
             */
            
            int nremoved = TMStatementBuffer.applyExistingStatements(focusStore, store, null/*filter*/);

            // statement was pre-existing and was converted from inferred to explicit.
            assertTrue(store.hasStatement(1, 2, 3));
            assertTrue(store.getStatement(1, 2, 3).isExplicit());
            
            // statement was pre-existing as "explicit" so no change.
            assertTrue(store.hasStatement(2, 2, 3));
            assertTrue(store.getStatement(2, 2, 3).isExplicit());

            assertEquals(2,focusStore.getStatementCount());
            
            assertEquals("#removed",1,nremoved);
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * A simple test of {@link TMStatementBuffer} in which some statements are
     * asserted and their closure is computed and aspects of that closure are
     * verified (this is based on rdfs11).
     */
    public void test_assertAll_01() {
        
        AbstractTripleStore store = getStore();
        
        try {
            
            InferenceEngine inf = new InferenceEngine(store);

            TMStatementBuffer assertionBuffer = new TMStatementBuffer(inf,
                    100/* capacity */, BufferEnum.AssertionBuffer);
            
            URI U = new URIImpl("http://www.foo.org/U");
            URI V = new URIImpl("http://www.foo.org/V");
            URI X = new URIImpl("http://www.foo.org/X");

            URI rdfsSubClassOf = URIImpl.RDFS_SUBCLASSOF;

            assertionBuffer.add(U, rdfsSubClassOf, V);
            assertionBuffer.add(V, rdfsSubClassOf, X);

            // perform closure and write on the database.
            assertionBuffer.doClosure();

            // explicit.
            assertTrue(store.hasStatement(U, rdfsSubClassOf, V));
            assertTrue(store.hasStatement(V, rdfsSubClassOf, X));

            // inferred.
            assertTrue(store.hasStatement(U, rdfsSubClassOf, X));
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }


    /**
     * A simple test of {@link TMStatementBuffer} in which some statements are
     * asserted, their closure is computed and aspects of that closure are
     * verified, and then an explicit statement is removed and the closure is
     * updated and we verify that an entailment known to depend on the remove
     * statement has also been removed (this is based on rdfs11).
     * 
     * @todo do a variant test where we remove more than one support at once in
     *       a case where the at least one of the statements entails the other
     *       and verify that both statements are removed (ie, verify that
     *       isGrounded is NOT accepting as grounds any support that is in the
     *       focusStore). This test could actually be done in
     *       {@link TestJustifications}.
     */
    public void test_retractAll_01() {
        
        URI U = new URIImpl("http://www.foo.org/U");
        URI V = new URIImpl("http://www.foo.org/V");
        URI X = new URIImpl("http://www.foo.org/X");

        URI rdfsSubClassOf = URIImpl.RDFS_SUBCLASSOF;

        AbstractTripleStore store = getStore();
        
        try {
            
            InferenceEngine inf = new InferenceEngine(store);

            // add some assertions and verify aspects of their closure.
            {
            
                TMStatementBuffer assertionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.AssertionBuffer);

                assertionBuffer.add(U, rdfsSubClassOf, V);
                assertionBuffer.add(V, rdfsSubClassOf, X);

                // perform closure and write on the database.
                assertionBuffer.doClosure();

                // explicit.
                assertTrue(store.hasStatement(U, rdfsSubClassOf, V));
                assertTrue(store.hasStatement(V, rdfsSubClassOf, X));

                // inferred.
                assertTrue(store.hasStatement(U, rdfsSubClassOf, X));

            }
            
            /*
             * retract one of the explicit statements and update the closure.
             * 
             * then verify that it is retracted statement is gone, that the
             * entailed statement is gone, and that the other explicit statement
             * was not touched.
             */
            {
                
                TMStatementBuffer retractionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.RetractionBuffer);

                retractionBuffer.add(V, rdfsSubClassOf, X);

                // update the closure.
                retractionBuffer.doClosure();
                
                // explicit.
                assertTrue(store.hasStatement(U, rdfsSubClassOf, V));
                assertFalse(store.hasStatement(V, rdfsSubClassOf, X));

                // inferred.
                assertFalse(store.hasStatement(U, rdfsSubClassOf, X));
                
            }

            /*
             * Add the retracted statement back in and verify that we get the
             * entailment back.
             */
            {
                
                TMStatementBuffer assertionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.AssertionBuffer);
                
                assertionBuffer.add(V, rdfsSubClassOf, X);

                // update the closure.
                assertionBuffer.doClosure();

                // explicit.
                assertTrue(store.hasStatement(U, rdfsSubClassOf, V));
                assertTrue(store.hasStatement(V, rdfsSubClassOf, X));

                // inferred.
                assertTrue(store.hasStatement(U, rdfsSubClassOf, X));

            }

            /*
             * Note: You MUST NOT submit a statement that is not an explicit
             * statement in the database to the retraction buffer!
             */
//            /*
//             * Retract the entailment and verify that it is NOT removed from the
//             * database (removing an inference has no effect).
//             */
//            {
//                
//                TMStatementBuffer retractionBuffer = new TMStatementBuffer(inf,
//                        100/* capacity */, BufferEnum.RetractionBuffer);
//
//                retractionBuffer.add(U, rdfsSubClassOf, X);
//
//                // update the closure.
//                retractionBuffer.doClosure();
//
//                // explicit.
//                assertTrue(store.hasStatement(U, rdfsSubClassOf, V));
//                assertTrue(store.hasStatement(V, rdfsSubClassOf, X));
//
//                // inferred.
//                assertTrue(store.hasStatement(U, rdfsSubClassOf, X));
//
//            }
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
}
