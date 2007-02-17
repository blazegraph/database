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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.inf;

import java.io.IOException;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.TempTripleStore;
import com.bigdata.rdf.TripleStore;

/**
 * Test suite for inference engine and the magic sets implementation.
 * <p>
 * The magic transform of the rule base {rdfs9, rdfs11} for the query
 * <code>triple(?s,rdf:type,A)?</code> is:
 * 
 * <pre>
 *    
 *    // by rule 1 of the magic transform.
 *    
 *    magic(triple(?s,rdf:type,A)). // the magic seed fact.
 *    
 *    // by rule 2 of the magic transform.
 *    
 *    triple(?v,rdf:type,?x) :-
 *       magic(triple(?u,rdfs:subClassOf,?x))
 *       triple(?u,rdfs:subClassOf,?x),
 *       triple(?v,rdf:type,?u). 
 *    
 *    triple(?u,rdfs:subClassOf,?x) :-
 *       magic(triple(?u,rdfs:subClassOf,?x))
 *       triple(?u,rdfs:subClassOf,?v),
 *       triple(?v,rdf:subClassOf,?x). 
 *    
 *    // by rule 3 of the magic transform ???
 *    
 * </pre>
 * 
 * The magic transform is:
 * <ol>
 * <li>add the query as a seed magic fact</li>
 * <li>insert a magic predicate as the first predicate in the body of each
 * rule. this magic predicate is formed by wrapping the head of the rule in a
 * "magic()" predicate.</li>
 * <li><em>it is not clear to me that rule3 applies for our rules</em></li>
 * </ol>
 * <p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMagicSets extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestMagicSets() {
    }

    /**
     * @param name
     */
    public TestMagicSets(String name) {
        super(name);
    }

    public static class MagicRule extends Rule {

        /**
         * The original rule. Execution is delegated to this object if the magic
         * matches.
         */
        public final Rule rule;
        
        /**
         * @param store
         * @param head
         * @param body
         */
        public MagicRule(InferenceEngine store, Rule rule) {
            
            super(store, rule.head, magicRewrite(rule));
            
            this.rule = rule;
            
        }
        
        static Pred[] magicRewrite(Rule rule) {
            
            Pred[] ret = new Pred[rule.body.length+1];
            
            ret[0] = new Magic(rule.head.s,rule.head.p,rule.head.o);

            System.arraycopy(rule.body, 0, ret, 1, rule.body.length);
            
            return ret;
            
        }

        /**
         * Return true iff the {@link Magic} is matched in the kb.
         */
        public boolean match() {

            // FIXME implement magic match.
            return true;
            
        }

        /**
         * Applies the base rule iff the {@link Magic} is matched.
         */
        public Rule.Stats apply( TempTripleStore entailments ) {

            if(match()) { 
            
                return rule.apply( entailments );
            
            }
            
            return null;
            
        }
        
        
    }
    
    /**
     * Test of query answering using magic sets.
     * 
     * @todo work through the magic sets implementation.
     * 
     * @throws IOException
     */
    public void testQueryAnswering01() throws IOException {

        /*
         * setup the database.
         */
        URI x = new URIImpl("http://www.foo.org/x");
        URI y = new URIImpl("http://www.foo.org/y");
        URI z = new URIImpl("http://www.foo.org/z");

        URI A = new URIImpl("http://www.foo.org/A");
        URI B = new URIImpl("http://www.foo.org/B");
        URI C = new URIImpl("http://www.foo.org/C");

        URI rdfType = new URIImpl(RDF.TYPE);

        URI rdfsSubClassOf = new URIImpl(RDFS.SUBCLASSOF);

        store.addStatement(x, rdfType, C);
        store.addStatement(y, rdfType, B);
        store.addStatement(z, rdfType, A);

        store.addStatement(B, rdfsSubClassOf, A);
        store.addStatement(C, rdfsSubClassOf, B);

        store.commit();

        assertEquals("statementCount", 5, store.getStatementCount());
        assertTrue(store.containsStatement(x, rdfType, C));
        assertTrue(store.containsStatement(y, rdfType, B));
        assertTrue(store.containsStatement(z, rdfType, A));
        assertTrue(store.containsStatement(B, rdfsSubClassOf, A));
        assertTrue(store.containsStatement(C, rdfsSubClassOf, B));
        
        /*
         * run the query triple(?s,rdfType,A) using only rdfs9 and rdfs11.
         */

        // query :- triple(?s,rdf:type,A).
        Triple query = new Triple(store.nextVar(), store.rdfType, new Id(store
                .addTerm(new URIImpl("http://www.foo.org/A"))));

        // Run the queryy.
        TripleStore answerSet = store.query(query, new Rule[] { store.rdfs9,
                store.rdfs11 });

        /*
         * @todo verify the answer set: ?s := {x,y,z}.
         */
        assertEquals("statementCount", 3, answerSet.getStatementCount());
        assertTrue(answerSet.containsStatement(x, rdfType, A));
        assertTrue(answerSet.containsStatement(y, rdfType, A));
        assertTrue(answerSet.containsStatement(z, rdfType, A));

    }
    
}
