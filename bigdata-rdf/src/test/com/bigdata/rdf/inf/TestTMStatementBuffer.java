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
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link TMStatementBuffer}.
 * 
 * @todo verify that we can downgrade an explicit statement to an inferred one.
 * 
 * @todo write an explicit test of
 *       {@link TMStatementBuffer#filterExistingStatements(AbstractTripleStore, AbstractTripleStore)}
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
             * Retract the entailment and verify that it is NOT removed from the
             * database (removing an inference has no effect).
             */
            {
                
                TMStatementBuffer retractionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.RetractionBuffer);

                retractionBuffer.add(V, rdfsSubClassOf, X);

                // update the closure.
                retractionBuffer.doClosure();

                // explicit.
                assertTrue(store.hasStatement(U, rdfsSubClassOf, V));
                assertTrue(store.hasStatement(V, rdfsSubClassOf, X));

                // inferred.
                assertTrue(store.hasStatement(U, rdfsSubClassOf, X));

            }
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
}
