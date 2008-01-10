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
import org.openrdf.model.vocabulary.OWL;

import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.NoAxiomFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBlockingBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for
 * {@link InferenceEngine#backchainIterator(ISPOIterator, long, long, long)}
 * when configured to generate entailments for {@link RuleOwlSameAs2} and
 * {@link RuleOwlSameAs3}.
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

            final URI A = new URIImpl("http://www.bigdata.com/A");
            final URI Z = new URIImpl("http://www.bigdata.com/Z");
            final URI X = new URIImpl("http://www.bigdata.com/X");
            final URI Y = new URIImpl("http://www.bigdata.com/Y");

            {
                IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);

                buffer.add(X, OWL.SAMEAS, Y);
                buffer.add(X, A, Z);

                // write on the store.
                buffer.flush();
            }
            
            // verify statement(s).
            assertTrue(store.hasStatement(X, OWL.SAMEAS, Y));
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
     * @todo test when subject is bound to Y -> (X A Z)
     * @todo test when subject is bound to X -> (X A Z)
     * @todo test when subject is bound to A -> {}
     * @todo test more complex examples.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_owlSameAs_01() throws InterruptedException, ExecutionException {

        final AbstractTripleStore store = getStore();

        try {

            final URI A = new URIImpl("http://www.bigdata.com/A");
            final URI Z = new URIImpl("http://www.bigdata.com/Z");
            final URI X = new URIImpl("http://www.bigdata.com/X");
            final URI Y = new URIImpl("http://www.bigdata.com/Y");

            {
                IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);

                buffer.add(X, OWL.SAMEAS, Y);
                
//              owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
                
                buffer.add(X, A, Z); // owl:sameAs2 -> (Y A Z)
                
//              owl:sameAs3: (x owl:sameAs y), (z a x) -&gt; (z a y).
                
                buffer.add(Z, A, X); // owl:sameAs3 -> (Z A Y)

                // write on the store.
                buffer.flush();
            }
            
            // verify statement(s).
            assertTrue(store.hasStatement(X, OWL.SAMEAS, Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertTrue(store.hasStatement(Z, A, X));
            assertEquals(3,store.getStatementCount());

            Properties properties = new Properties(store.getProperties());
            
            // specify forward chaining so that the backchain iterator does not introduce these entailments.
            properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
            
            InferenceEngine inf = new InferenceEngine(properties,store);

            // triple pattern for the query.
            final long s = NULL, p=store.getTermId(A), o=NULL;

            /*
             * Obtain an iterator that read on the triple pattern plus also
             * backchains owl:sameAs {2,3}.
             */
            ISPOIterator itr = inf.backchainIterator(s, p, o);
            
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

            URI A = new URIImpl("http://www.bigdata.com/A");
            URI Z = new URIImpl("http://www.bigdata.com/Z");
            URI X = new URIImpl("http://www.bigdata.com/X");
            URI Y = new URIImpl("http://www.bigdata.com/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, OWL.SAMEAS, Y);
            buffer.add(X, A, Z);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, OWL.SAMEAS, Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertEquals(2,store.getStatementCount());

            Properties properties = new Properties(store.getProperties());

            // specify forward chaining so that the backchain iterator does not introduce these entailments.
            properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
            
            InferenceEngine inf = new InferenceEngine(properties,store);

            ISPOIterator itr = inf.backchainIterator(//
                    NULL, inf.owlSameAs.id, NULL//
                    );

            /*
             * Should only contain explicit statements with owl:sameAs in the
             * predicate position.
             */ 
            assertSameSPOsAnyOrder(store, new SPO[]{

                    new SPO(store.getTermId(X), inf.owlSameAs.id, store.getTermId(Y),
                            StatementEnum.Explicit),
                                        
            },itr);

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
     */
    public void test_owlSameAs_02() {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.bigdata.com/A");
            URI Z = new URIImpl("http://www.bigdata.com/Z");
            URI X = new URIImpl("http://www.bigdata.com/X");
            URI Y = new URIImpl("http://www.bigdata.com/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, OWL.SAMEAS, Y);
            buffer.add(X, A, Z);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, OWL.SAMEAS, Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertEquals(2,store.getStatementCount());

            Properties properties = new Properties(store.getProperties());
            
            // specify forward chaining so that the backchain iterator does not introduce these entailments.
            properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
            
            InferenceEngine inf = new InferenceEngine(properties,store);

            ISPOIterator itr = inf.backchainIterator(NULL, NULL, NULL, NoAxiomFilter.INSTANCE);

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

    /**
     * <p>
     * This tests an example developed by MikeP.
     * </p>
     * <p>
     * Here's your forward closed KB.
     * </p>
     * 
     * <pre>
     *     #foo link #bar
     *     #boo link #bingo
     *     #foo owl:sameAs #boo -&gt; #boo owl:sameAs #foo
     *     #bar owl:sameAs #baz -&gt; #baz owl:sameAs #bar
     *     #bingo owl:sameAs #boingo -&gt; #boingo owl:sameAs #bingo
     * </pre>
     * 
     * <p>
     * A query comes in. (#foo link ?) yields one real binding – one distinct
     * subject and object:
     * </p>
     * 
     * <pre>
     *     (#foo link #bar)
     * </pre>
     * 
     * <p>
     * Run owl:sameAs2 first
     * </p>
     * 
     * <pre>
     *   owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
     * </pre>
     * 
     * <p>
     * binding y to the distinct result subjects (#foo), a to the query
     * predicate (link) and z to the query object (unbound):
     * </p>
     * 
     * <pre>
     *     (x same #foo) + (x link z) -&gt; (#foo link #bingo)
     * </pre>
     * 
     * <p>
     * You now have one distinct subject (#foo) and two distinct objects (#bar,
     * #bingo).
     * </p>
     * <p>
     * Then run owl:sameAs3
     * </p>
     * 
     * <pre>
     *  owl:sameAs3: (x owl:sameAs y), (z a x) -&gt; (z a y).
     * </pre>
     * 
     * <p>
     * FIXME binding x to the distinct result objects (#bar, #bingo), z to the
     * distinct result subjects (#foo), and a to the query predicate (link):
     * </p>
     * 
     * <pre>
     *     (#bar same y) + (#foo link #bar) -&gt; (#foo link #baz)
     *     
     *     (#bingo same y) + (#foo link #bingo) -&gt; (#foo link #boingo)
     * </pre>
     * 
     * <p>
     * You get four results:
     * </p>
     * 
     * <pre>
     *     (#foo link #bar)
     *     (#foo link #bingo)
     *     (#foo link #baz)
     *     (#foo link #boingo)
     * </pre>
     * 
     * <p>
     * Which is exactly as it should be.
     * </p>
     * So there are four issues here:
     * <ol>
     * <li>sameAs2 needs distinct subjects from the query results, sameAs3
     * needs distinct objects from the query results + results from sameAs2.</li>
     * <li>the rules need to test for duplicates in the result set.</li>
     * <li>the rules need to be rewritten to accept bindings per magic sets and
     * the above example.</li>
     * <li>the results of the query + sameAs2 + sameAs3 need to be piped into (?
     * type Resource) backchaining.</li>
     * </ol>
     */
    public void test_owlSameAs_mike() {
        
        AbstractTripleStore store = getStore();

        try {

            /*
             *  #foo link #bar
             *  #boo link #bingo
             *  #foo owl:sameAs #boo -&gt; #boo owl:sameAs #foo
             *  #bar owl:sameAs #baz -&gt; #baz owl:sameAs #bar
             *  #bingo owl:sameAs #boingo -&gt; #boingo owl:sameAs #bingo
             */
            URI link = new URIImpl("http://www.bigdata.com/link");
            URI sameAs = OWL.SAMEAS;
            URI foo = new URIImpl("http://www.bigdata.com/foo");
            URI bar = new URIImpl("http://www.bigdata.com/bar");
            URI baz = new URIImpl("http://www.bigdata.com/baz");
            URI boo = new URIImpl("http://www.bigdata.com/boo");
            URI bingo = new URIImpl("http://www.bigdata.com/bingo");
            URI boingo = new URIImpl("http://www.bigdata.com/boingo");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(foo, link, bar);

            buffer.add(boo, link, bingo);
            
            buffer.add(foo, sameAs, boo);
            buffer.add(boo, sameAs, foo); // inferred during forward closure.
            
            buffer.add(bar, sameAs, baz);
            buffer.add(baz, sameAs, bar); // inferred during forward closure.

            buffer.add(bingo, sameAs, boingo);
            buffer.add(boingo, sameAs, bingo); // inf. during forward closure.

            // write on the store.
            buffer.flush();
            assertEquals(8,store.getStatementCount());

            /*
             * setup inference engine.
             */
            
            Properties properties = new Properties(store.getProperties());
            
            /*
             * Specify forward chaining so that the backchain iterator does not
             * introduce entailments for (x type resource).
             * 
             * Note: the entailments are NOT present in the store since we wrote
             * directly on the database without truth maintenance.
             */
            
            properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
            
            InferenceEngine inf = new InferenceEngine(properties,store);

            // triple pattern query.
            long s = store.getTermId(foo);
            long p = store.getTermId(link);
            long o = NULL;

            /*
             * verify query w/o backchaining.
             */
            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(store.getTermId(foo), store.getTermId(link), store.getTermId(bar),
                            StatementEnum.Explicit),

            },
                    store.getAccessPath(s, p, o).iterator());
            
            /*
             * verify query w/ backchaining.
             */
            ISPOIterator itr = inf.backchainIterator(s, p, o);

            /*
             * (#foo link #bar) (#foo link #bingo) (#foo link #baz) (#foo link
             * #boingo)
             */
            assertSameSPOsAnyOrder(store, new SPO[]{
                    
                    new SPO(store.getTermId(foo), store.getTermId(link), store.getTermId(bar),
                            StatementEnum.Explicit),
                            
                    new SPO(store.getTermId(foo), store.getTermId(link), store.getTermId(bingo),
                            StatementEnum.Inferred),
                                                                        
                    new SPO(store.getTermId(foo), store.getTermId(link), store.getTermId(baz),
                            StatementEnum.Inferred),
                                                                
                    new SPO(store.getTermId(foo), store.getTermId(link), store.getTermId(boingo),
                            StatementEnum.Inferred),
                                                                                        
            },
                    
                    itr);
            
        } finally {

            store.closeAndDelete();

        }
        
    }
    
}
