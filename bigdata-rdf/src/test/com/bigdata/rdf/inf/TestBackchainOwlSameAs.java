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

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.OWL;

import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBlockingBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link BackchainOwlSameAs_2_3} (backchaining equivilent to
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
     * Test creates and executes a specialization of {@link RuleOwlSameAs2}
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_specializeRule() throws InterruptedException, ExecutionException {

        final AbstractTripleStore store = getStore();

        try {
            
            final RDFSHelper vocab = new RDFSHelper(store);

            final URI A = new URIImpl("http://www.foo.org/A");
            final URI Z = new URIImpl("http://www.foo.org/Z");
            final URI X = new URIImpl("http://www.foo.org/X");
            final URI Y = new URIImpl("http://www.foo.org/Y");

            {
                IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);

                buffer.add(X, new URIImpl(OWL.SAMEAS), Y);
                buffer.add(X, A, Z);

                // write on the store.
                buffer.flush();
            }
            
            // verify statement(s).
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertEquals(2,store.getStatementCount());

            // triple pattern for the query.
            final long s = NULL, p=store.getTermId(A), o=NULL;
            
            // owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
            Rule r = new RuleOwlSameAs2(vocab);

            // specialize the rule by binding the triple pattern.
            final Rule r1 = r.specialize(//
                    s, p, o, //
                    new IConstraint[] {//
                        new NEConstant((Var) r.head.p,vocab.owlSameAs.id)//
                    });
            
            /*
             * Buffer on which the rule will write.
             */
            final SPOBlockingBuffer buffer = new SPOBlockingBuffer(store,
                    null /* filter */, 100/* capacity */);
            
            store.readService.submit(new Runnable() {

                public void run() {

                    // run the rule.
                    r1.apply(false/* justify */, null/* focusStore */,
                            store/* database */, buffer);
                    
                    // close the buffer
                    buffer.close();

                }

            }).get();

            assertSameSPOsAnyOrder(store,
                    new SPO[] {

                            new SPO(store.getTermId(Y), store
                                    .getTermId(A), store.getTermId(Z),
                                    StatementEnum.Inferred),

                    }, buffer.iterator());
    
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    /**
     * Maps a specialization of {@link RuleOwlSameAs2} over each distinct
     * subject visited by an iterator reading on a triple pattern from the
     * database.
     * 
     * @todo filter (x type resource).
     * 
     * @todo test when subject is bound to Y -> (X A Z)
     * @todo test when subject is bound to X -> (X A Z)
     * @todo test when subject is bound to A -> {}
     * @todo test when predicate is A
     * @todo test when predicate is owl:sameAs
     * @todo test more complex examples.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_owlSameAs_01() throws InterruptedException, ExecutionException {

        final AbstractTripleStore store = getStore();

        try {

            final URI A = new URIImpl("http://www.foo.org/A");
            final URI Z = new URIImpl("http://www.foo.org/Z");
            final URI X = new URIImpl("http://www.foo.org/X");
            final URI Y = new URIImpl("http://www.foo.org/Y");

            {
                IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);

                buffer.add(X, new URIImpl(OWL.SAMEAS), Y);
                buffer.add(X, A, Z); // owl:sameAs2 -> (Y A Z)
                buffer.add(Z, A, X); // owl:sameAs3 -> (Z A Y)

                // write on the store.
                buffer.flush();
            }
            
            // verify statement(s).
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertTrue(store.hasStatement(Z, A, X));
            assertEquals(3,store.getStatementCount());

            Properties properties = new Properties(store.getProperties());
            
            // specify forward chaining so that the backchain iterator does not introduce these entailments.
            properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
            
            InferenceEngine inf = new InferenceEngine(properties,store);

            // triple pattern for the query.
            final long s = NULL, p=store.getTermId(A), o=NULL;

            // original iterator reading on the database.
            ISPOIterator src = store.getAccessPath(s, p, o).iterator();
            
            // iterator that also backchains owl:sameAs {2,3}.
            ISPOIterator itr = inf.backchainIterator(src, s, p, o);
            
            assertSameSPOsAnyOrder(store,
                new SPO[] {

                  // Note: ruled out by the triple pattern.
//                new SPO(store.getTermId(X), inf.owlSameAs.id, store.getTermId(Y),
//                        StatementEnum.Explicit),
                        
                new SPO(store.getTermId(X), store.getTermId(A), store.getTermId(Z),
                        StatementEnum.Explicit),
                                    
                new SPO(store.getTermId(Z), store.getTermId(A), store.getTermId(X),
                        StatementEnum.Explicit),
                                            
                new SPO(store.getTermId(Y), store.getTermId(A), store.getTermId(Z),
                        StatementEnum.Inferred),

                new SPO(store.getTermId(Z), store.getTermId(A), store.getTermId(Y),
                        StatementEnum.Inferred),

                }, itr);
                
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

//    /**
//     * Test where the predicate in the backchain query (y a z) is bound to
//     * <code>owl:sameAs</code>
//     * 
//     * <pre>
//     *   owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
//     * </pre>
//     */
//    public void test_owlSameAs_predicateIsOwlSameAs() {
//
//        AbstractTripleStore store = getStore();
//
//        try {
//
//            URI A = new URIImpl("http://www.foo.org/A");
//            URI Z = new URIImpl("http://www.foo.org/Z");
//            URI X = new URIImpl("http://www.foo.org/X");
//            URI Y = new URIImpl("http://www.foo.org/Y");
//
//            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
//            
//            buffer.add(X, new URIImpl(OWL.SAMEAS), Y);
//            buffer.add(X, A, Z);
//
//            // write on the store.
//            buffer.flush();
//
//            // verify statement(s).
//            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
//            assertTrue(store.hasStatement(X, A, Z));
//            assertEquals(2,store.getStatementCount());
//
//            RDFSHelper vocab = new RDFSHelper(store);
//
//            ISPOIterator itr = BackchainOwlSameAs_2_3.newIterator(//
//                    store.getAccessPath(NULL, vocab.owlSameAs.id, NULL).iterator(),//
//                    NULL, vocab.owlSameAs.id, NULL,//
//                    store, //
//                    vocab.owlSameAs.id //
//                    );
//            
//            // should be an empty iterator.
//            assertSameSPOsAnyOrder(store, new SPO[]{},itr);
//
//        } finally {
//
//            store.closeAndDelete();
//
//        }
//        
//    }

    /**
     * Test where the data satisifies the fully unbound query (y a z) exactly
     * once.
     * <pre>
     *   owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
     * </pre>
     * 
     */
    public void test_owlSameAs_02() {

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

            Properties properties = new Properties(store.getProperties());
            
            // specify forward chaining so that the backchain iterator does not introduce these entailments.
            properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
            
            InferenceEngine inf = new InferenceEngine(properties,store);

            ISPOIterator itr = inf.backchainIterator(
                    store.getAccessPath(NULL, NULL, NULL).iterator(),//
                    NULL, NULL, NULL
                    );

            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(store.getTermId(X), inf.owlSameAs.id, store.getTermId(Y),
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
