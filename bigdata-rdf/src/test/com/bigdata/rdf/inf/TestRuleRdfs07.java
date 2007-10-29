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
 * Created on Apr 13, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.inf.Rule.RuleStats;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Note: rdfs 2, 3, 7, and 9 use the same base class.
 * 
 * @see RuleRdfs02
 * @see RuleRdfs03
 * @see RuleRdfs07
 * @see RuleRdfs09
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdfs07 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdfs07() {
    }

    /**
     * @param name
     */
    public TestRuleRdfs07(String name) {
        super(name);
    }

    /**
     * Test of {@link RuleRdfs07} where the data satisifies the rule exactly
     * once.
     * 
     * <pre>
     *         triple(?u,?b,?y) :-
     *            triple(?a,rdfs:subPropertyOf,?b),
     *            triple(?u,?a,?y).
     * </pre>
     */
    public void test_rdfs07_01() {

        AbstractTripleStore store = getStore();
        
        try {
            
            InferenceEngine inf = new InferenceEngine(store);
            
            URI A = new _URI("http://www.foo.org/A");
            URI B = new _URI("http://www.foo.org/B");
            URI U = new _URI("http://www.foo.org/U");
            URI Y = new _URI("http://www.foo.org/Y");
    
            URI rdfsSubPropertyOf = new _URI(RDFS.SUBPROPERTYOF);
    
            store.addStatement(A, rdfsSubPropertyOf, B);
            store.addStatement(U, A, Y);
    
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B));
            assertTrue(store.containsStatement(U, A, Y));
            assertFalse(store.containsStatement(U, B, Y));
            assertEquals(2,store.getStatementCount());

            // apply the rule.
            RuleStats stats = applyRule(inf,inf.rdfs7,1);
    
            assertEquals("#subqueries",1,stats.nsubqueries[0]);
            assertEquals("#subqueries",0,stats.nsubqueries[1]);
            
            /*
             * validate the state of the primary store.
             */
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B));
            assertTrue(store.containsStatement(U, A, Y));
            assertTrue(store.containsStatement(U, B, Y));
        
        } finally {
        
            store.closeAndDelete();
            
        }
        
    }
    
    /**
     * Test of {@link RuleRdfs07} where the data satisifies the rule twice --
     * there are two matches in the subquery for the same binding on "?a". Only
     * one subquery is made since there is only one match for the first triple
     * pattern.
     * 
     * <pre>
     *           triple(?u,?b,?y) :-
     *              triple(?a,rdfs:subPropertyOf,?b),
     *              triple(?u,?a,?y).
     * </pre>
     */
    public void test_rdfs07_02() {

        AbstractTripleStore store = getStore();
        
        try {
        
            InferenceEngine inf = new InferenceEngine(store);
            
            URI A = new _URI("http://www.foo.org/A");
            URI B = new _URI("http://www.foo.org/B");
            URI U1 = new _URI("http://www.foo.org/U1");
            URI Y1 = new _URI("http://www.foo.org/Y1");
            URI U2 = new _URI("http://www.foo.org/U2");
            URI Y2 = new _URI("http://www.foo.org/Y2");
    
            URI rdfsSubPropertyOf = new _URI(RDFS.SUBPROPERTYOF);
    
            store.addStatement(A, rdfsSubPropertyOf, B);
            store.addStatement(U1, A, Y1);
            store.addStatement(U2, A, Y2);
    
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B));
            assertTrue(store.containsStatement(U1, A, Y1));
            assertTrue(store.containsStatement(U2, A, Y2));
            assertFalse(store.containsStatement(U1, B, Y1));
            assertFalse(store.containsStatement(U2, B, Y2));
    
            // apply the rule.
            RuleStats stats = applyRule(inf,inf.rdfs7,2/*numComputed*/);
    
            assertEquals("#subqueries",1,stats.nsubqueries[0]);
            assertEquals("#subqueries",0,stats.nsubqueries[1]);

            /*
             * validate the state of the primary store.
             */
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B));
            assertTrue(store.containsStatement(U1, A, Y1));
            assertTrue(store.containsStatement(U2, A, Y2));
            assertTrue(store.containsStatement(U1, B, Y1));
            assertTrue(store.containsStatement(U2, B, Y2));
        
        } finally {

            store.closeAndDelete();
            
        }

    }
    
    /**
     * Test of {@link RuleRdfs07} where the data satisifies the rule twice --
     * there are two matches on the first triple pattern that have the same
     * subject. However, only one subquery is made since both matches on the
     * first triple pattern have the same subject.
     * <p>
     * Note: This test is used to verify that the JOIN reorders the results from
     * the first triple pattern into SPO order so that fewer subqueries need to
     * be executed (only one subquery in this case).
     * 
     * <pre>
     *              triple(?u,?b,?y) :-
     *                 triple(?a,rdfs:subPropertyOf,?b),
     *                 triple(?u,?a,?y).
     * </pre>
     */
    public void test_rdfs07_03() {

        AbstractTripleStore store = getStore();

        try {
        
            InferenceEngine inf = new InferenceEngine(store);
            
            URI A = new _URI("http://www.foo.org/A");
            URI B1 = new _URI("http://www.foo.org/B1");
            URI B2 = new _URI("http://www.foo.org/B2");
            URI U = new _URI("http://www.foo.org/U");
            URI Y = new _URI("http://www.foo.org/Y");
    
            URI rdfsSubPropertyOf = new _URI(RDFS.SUBPROPERTYOF);
    
            store.addStatement(A, rdfsSubPropertyOf, B1);
            store.addStatement(A, rdfsSubPropertyOf, B2);
            store.addStatement(U, A, Y);
    
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B1));
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B2));
            assertTrue(store.containsStatement(U, A, Y));
            assertEquals(3,store.getStatementCount());
    
            // apply the rule.
            RuleStats stats = applyRule(inf,inf.rdfs7,2/*expectedComputed*/);
    
            // FIXME enable tests when working on subquery elimination.
//            /*
//             * Verify that only one subquery is issued (iff subquery elimination
//             * is turned on for the rule).
//             */
//            assertEquals("#subqueries",(inf.rdfs7.subqueryElimination)?1:2,stats.nsubqueries[0]);
//            assertEquals("#subqueries",0,stats.nsubqueries[1]);
            
            /*
             * validate the state of the primary store.
             */
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B1));
            assertTrue(store.containsStatement(A, rdfsSubPropertyOf, B2));
            assertTrue(store.containsStatement(U, A, Y));
            assertTrue(store.containsStatement(U, B1, Y)); // entailed.
            assertTrue(store.containsStatement(U, B2, Y)); // entailed.
            assertEquals(5,store.getStatementCount());

        } finally {

            store.closeAndDelete();
            
        }
        
    }
    
}
