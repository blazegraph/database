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
 * Created on Jun 19, 2008
 */

package com.bigdata.join;

import junit.framework.TestCase2;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAccessPathFactory extends TestCase2 {

    /**
     * 
     */
    public TestAccessPathFactory() {
    }

    /**
     * @param name
     */
    public TestAccessPathFactory(String name) {
        super(name);
    }
        
//        /**
//         * Test the ability to obtain the access path given the {@link Pred}.
//         */
//        public void test_getAccessPath() {
//            
//            AbstractTripleStore store = getStore();
//            
//            try {
    //
//                Rule r = new MyRulePattern1(new RDFSHelper(store));
    //
//                State s = r.newState(false/* justify */, store, new SPOAssertionBuffer(store, store,
//                        null/* filter */, 100/* capacity */, false/* justify */));
//                
//                // (u rdfs:subClassOf x)
//                assertEquals(KeyOrder.POS,s.getAccessPath(0).getKeyOrder());
    //
//                // (v rdfs:subClassOf u)
//                assertEquals(KeyOrder.POS,s.getAccessPath(1).getKeyOrder());
//                
//            } finally {
//                
//                store.closeAndDelete();
//                
//            }
    //
//        }
//        
////        /**
////         * Test the ability to choose the more selective access path, that the
////         * selected path changes as predicates become bound, and that the resulting
////         * entailment reflects the current variable bindings.
////         */
////        public void test_getMoreSelectiveAccessPath() {
////            
////            AbstractTripleStore store = getStore();
////            
////            try {
//    //
////                // define some vocabulary.
////                RDFSHelper vocab = new RDFSHelper(store);
////                
//////                InferenceEngine inf = new InferenceEngine(store);
//    //
////                URI U1 = new URIImpl("http://www.foo.org/U1");
////                URI U2 = new URIImpl("http://www.foo.org/U2");
////                URI V1 = new URIImpl("http://www.foo.org/V1");
////                URI V2 = new URIImpl("http://www.foo.org/V2");
////                URI X1 = new URIImpl("http://www.foo.org/X1");
//////                URI X2 = new URIImpl("http://www.foo.org/X2");
//    //
////                // body[0]                  body[1]          -> head
////                // (?u,rdfs:subClassOf,?x), (?v,rdf:type,?u) -> (?v,rdf:type,?x)
////                Rule r = new MyRulePattern1(vocab);
//    //
////                // generate justifications for entailments.
////                final boolean justify = true;
////                
////                State s = r.newState(justify, store, new SPOBuffer(store,justify));
////               
////                /*
////                 * Obtain the access paths corresponding to each predicate in the
////                 * body of the rule. Each access path is parameterized by the triple
////                 * pattern described by the corresponding predicate in the body of
////                 * the rule.
////                 * 
////                 * Note: even when using the same access paths the range counts CAN
////                 * differ based on what constants are bound in each predicate and on
////                 * what positions are variables.
////                 * 
////                 * Note: When there are shared variables the range count generally
////                 * will be different after those variable(s) become bound.
////                 */
////                for (int i = 0; i < r.body.length; i++) {
//    //
////                    assertEquals(0, s.getAccessPath(i).rangeCount());
//    //
////                }
////                
////                /*
////                 * Add some data into the store where it is visible to those access
////                 * paths and notice the change in the range count.
////                 */
////                StatementBuffer buffer = new StatementBuffer(store,100/*capacity*/,true/*distinct*/);
//    //
////                // (u rdf:subClassOf x)
////                buffer.add(U1, URIImpl.RDFS_SUBCLASSOF, X1);
////                
////                // (v rdf:type u)
////                buffer.add(V1, URIImpl.RDF_TYPE, U1);
////                buffer.add(V2, URIImpl.RDF_TYPE, U2);
////                
////                buffer.flush();
////                
////                store.dumpStore();
////                
////                assertEquals(3,store.getStatementCount());
////                
////                // (u rdf:subClassOf x)
////                assertEquals(1,s.getAccessPath(0).rangeCount());
//    //
////                // (v rdf:type u)
////                assertEquals(2,s.getAccessPath(1).rangeCount());
//    //
////                /*
////                 * Now use the more selective of the two 1-bound triple patterns to
////                 * query the store.
////                 */
//    //
////                // (u rdf:subClassOf x)
////                assertEquals(0,s.getMostSelectiveAccessPathByRangeCount());
////                
////                /*
////                 * bind variables for (u rdf:subClassOf x) to known values from the
////                 * statement in the database that matches the predicate.
////                 */
////                
////                assertNotSame(NULL,store.getTermId(U1));
////                s.set(Rule.var("u"), store.getTermId(U1));
////                
////                assertNotSame(NULL,store.getTermId(X1));
////                s.set(Rule.var("x"), store.getTermId(X1));
////                
////                assertTrue(s.isFullyBound(0));
////                
////                // (v rdf:type u)
////                assertEquals(1,s.getMostSelectiveAccessPathByRangeCount());
//    //
////                // bind the last variable.
////                assertNotSame(NULL,store.getTermId(V1));
////                s.set(Rule.var("v"), store.getTermId(V1));
//    //
////                assertTrue(s.isFullyBound(1));
//    //
////                // verify no access path is recommended since the rule is fully bound.
////                assertEquals(-1,s.getMostSelectiveAccessPathByRangeCount());
//    //
////                // emit the entailment
////                s.emit();
////                
////                assertEquals(1,s.buffer.size());
////                assertEquals(justify?1:0,s.buffer.getJustificationCount());
////                
////                // verify bindings on the emitted entailment.
////                SPO entailment = s.buffer.get(0);
////                assertEquals(entailment.s,store.getTermId(V1));
////                assertEquals(entailment.p,store.getTermId(URIImpl.RDF_TYPE));
////                assertEquals(entailment.o,store.getTermId(X1));
////                
////                if(justify) {
////                 
////                    // @todo verify the justification
////                    
////                }
////                
////            } finally {
////                
////                store.closeAndDelete();
////                
////            }
//    //
////        }

}
