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
 * Created on Nov 9, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.OWL;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.EmptySPOIterator;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link BackchainOwlSameAs2} (backchaining equivilent to
 * {@link RuleOwlSameAs2}) and {@link BackchainOwlSameAs3} (backchaining
 * equivilent to {@link RuleOwlSameAs3}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBackchainOwlSameAs extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestBackchainOwlSameAs() {
    }

    /**
     * @param name
     */
    public TestBackchainOwlSameAs(String name) {
        super(name);
    }

    /**
     * Test where the predicate in the backchain query (y a z) is bound to
     * <code>owl:sameAs</code>
     * 
     * <pre>
     *   owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
     * </pre>
     */
    public void test_owlSameAs_predicateIsOwlSameAs() {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI Z = new URIImpl("http://www.foo.org/Z");
            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, new URIImpl(OWL.SAMEAS), Y);
            buffer.add(X, A, Z);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertEquals(2,store.getStatementCount());

            RDFSHelper vocab = new RDFSHelper(store);

            ISPOIterator itr = BackchainOwlSameAs2.newIterator(//
                    store.getAccessPath(NULL, vocab.owlSameAs.id, NULL).iterator(),//
                    NULL, vocab.owlSameAs.id, NULL,//
                    store, //
                    vocab.owlSameAs.id //
                    );
            
            // should be an empty iterator.
            assertSameSPOsAnyOrder(store, new SPO[]{},itr);

            // should be an instance of this class.
            assertTrue( itr instanceof EmptySPOIterator );

        } finally {

            store.closeAndDelete();

        }
        
    }

    /**
     * Test where the data satisifies the fully unbound query (y a z) exactly
     * once.
     * <pre>
     *   owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
     * </pre>
     * 
     * @todo test when subject is bound to Y -> (X A Z)
     * @todo test when subject is bound to X -> (X A Z)
     * @todo test when subject is bound to A -> {}
     * @todo test when predicate is A
     * @todo test more complex examples.
     */
    public void test_owlSameAs2() {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI Z = new URIImpl("http://www.foo.org/Z");
            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, new URIImpl(OWL.SAMEAS), Y);
            buffer.add(X, A, Z);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertEquals(2,store.getStatementCount());

            RDFSHelper vocab = new RDFSHelper(store);

            ISPOIterator itr = BackchainOwlSameAs2.newIterator(//
                    store.getAccessPath(NULL, NULL, NULL).iterator(),//
                    NULL, NULL, NULL,//
                    store, //
                    vocab.owlSameAs.id //
                    );

            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(store.getTermId(X), vocab.owlSameAs.id, store.getTermId(Y),
                            StatementEnum.Explicit),
                            
                    new SPO(store.getTermId(X), store.getTermId(A), store.getTermId(Z),
                            StatementEnum.Explicit),
                                    
                    new SPO(store.getTermId(Y), store.getTermId(A), store.getTermId(Z),
                            StatementEnum.Inferred),
                            
            },
                    
                    itr);
            
        } finally {

            store.closeAndDelete();

        }
        
    }

}
