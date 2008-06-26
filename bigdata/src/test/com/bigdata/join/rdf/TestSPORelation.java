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

package com.bigdata.join.rdf;

import java.io.File;
import java.util.Properties;

import com.bigdata.join.AbstractRuleTestCase;
import com.bigdata.join.IRelationName;
import com.bigdata.join.IRule;
import com.bigdata.join.MockRelationName;
import com.bigdata.join.RuleState;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.LocalDataServiceClient;
import com.bigdata.service.LocalDataServiceClient.Options;

/**
 * Test ability to insert, update, or remove elements from a relation and the
 * ability to select the right access path given a predicate for that relation
 * and query for those elements (we have to test all this stuff together since
 * testing query requires us to have some data in the relation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPORelation extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestSPORelation() {
    }

    /**
     * @param name
     */
    public TestSPORelation(String name) {
        
        super(name);
        
    }

    File dataDir;
    IBigdataClient client;
    TestTripleStore kb;

    protected void setUp() throws Exception {

        super.setUp();
        
        Properties properties = new Properties(getProperties());

        dataDir = File.createTempFile(getName(), ".tmp");
        
        dataDir.delete();
        
        dataDir.mkdirs();
        
        properties.setProperty(Options.DATA_DIR, dataDir.toString());
        
        // use a temporary store.
//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Temporary.toString());

        client = new LocalDataServiceClient(properties);
        
        client.connect();

        kb = new TestTripleStore(client, "test", ITx.UNISOLATED);

        kb.create();
        
    }

    protected void tearDown() throws Exception {

        client.getFederation().destroy();
        
        client.disconnect(true/*immediateShutdown*/);
        
        super.tearDown();
        
    }

    /**
     * Basic test of the ability insert data into a relation and pull back that
     * data using an unbound query. The use of an unbound query lets the
     * relation select whatever index it pleases and should normally select the
     * "clustered" index for that relation.
     */
    public void test_insertQuery() {
        
    }
    
    /**
     * FIXME {@link RuleState} has become an evaluation order and some access
     * path caching. In order to test this class we need to have either a mock
     * access path or some real data.
     */
    public void test_ruleState() {

        final IRelationName relationName = new MockRelationName();
        
//        final IRelation<ISPO> relation = new SPORelation( );
        
        final IRule r = new TestRuleRdfs9(relationName);

        final RuleState state = new RuleState(r, new SPOJoinNexus(
                false/* elementOnly */, new SPORelationLocator(kb)));
        
        fail("write test");

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
