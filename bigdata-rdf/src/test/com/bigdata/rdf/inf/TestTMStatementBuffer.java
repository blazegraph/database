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
 * Created on Nov 5, 2007
 */

package com.bigdata.rdf.inf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.MDC;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.vocabulary.OWL;

import com.bigdata.isolation.Value;
import com.bigdata.rdf.inf.TMStatementBuffer.BufferEnum;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.SesameStatementIterator;
import com.bigdata.rdf.store.StatementWithType;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Test suite for {@link TMStatementBuffer}.
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
            
            InferenceEngine inf = store.getInferenceEngine();

            TMStatementBuffer assertionBuffer = new TMStatementBuffer(inf,
                    100/* capacity */, BufferEnum.AssertionBuffer);
            
            URI U = new URIImpl("http://www.bigdata.com/U");
            URI V = new URIImpl("http://www.bigdata.com/V");
            URI X = new URIImpl("http://www.bigdata.com/X");

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
        
        URI U = new URIImpl("http://www.bigdata.com/U");
        URI V = new URIImpl("http://www.bigdata.com/V");
        URI X = new URIImpl("http://www.bigdata.com/X");

        URI rdfsSubClassOf = URIImpl.RDFS_SUBCLASSOF;

        AbstractTripleStore store = getStore();
        
        try {
            
            InferenceEngine inf = store.getInferenceEngine();

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

//            /*
//             * Note: You MUST NOT submit a statement that is not an explicit
//             * statement in the database to the retraction buffer!
//             */
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

    /**
     * Given three explicit statements:
     * 
     * <pre>
     *  stmt a:  #user #currentGraph #foo
     *  
     *  stmt b: #currentGraph rdfs:range #Graph
     *  
     *  stmt c: #foo rdf:type #Graph
     * </pre>
     * 
     * a+b implies c
     * <p>
     * Delete a and verify that c is NOT gone since it is an explicit statement.
     */
    public void test_retractWhenStatementSupportsExplicitStatement() {
     
        URI user = new URIImpl("http://www.bigdata.com/user");
        URI currentGraph = new URIImpl("http://www.bigdata.com/currentGraph");
        URI foo = new URIImpl("http://www.bigdata.com/foo");
        URI graph = new URIImpl("http://www.bigdata.com/Graph");
        URI rdftype = URIImpl.RDF_TYPE;
        URI rdfsRange = URIImpl.RDFS_RANGE;

        AbstractTripleStore store = getStore();
        
        try {
            
            InferenceEngine inf = store.getInferenceEngine();

            // add some assertions and verify aspects of their closure.
            {
            
                TMStatementBuffer assertionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.AssertionBuffer);

                // stmt a
                assertionBuffer.add(user, currentGraph, foo );
                
                // stmt b
                assertionBuffer.add(currentGraph, rdfsRange, graph );
                
                // stmt c
                assertionBuffer.add(foo, rdftype, graph );
                
                // perform closure and write on the database.
                assertionBuffer.doClosure();

                // dump after closure.
                store.dumpStore(true,true,false);

                // explicit.
                assertTrue(store.hasStatement(user, currentGraph, foo ));
                assertTrue(store.hasStatement(currentGraph, rdfsRange, graph ));
                assertTrue(store.hasStatement(foo, rdftype, graph));

                // verify that stmt c is marked as explicit in the kb.

                StatementWithType stmtC = (StatementWithType) store
                        .getStatement(foo, rdftype, graph);
                
                assertNotNull(stmtC);
                
                assertEquals(StatementEnum.Explicit, stmtC.getStatementType());
                
            }
            
            /*
             * retract stmt A and update the closure.
             * 
             * then verify that it is retracted statement is gone and that the
             * other explicit statements were not touched.
             */
            {
                
                TMStatementBuffer retractionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.RetractionBuffer);

                retractionBuffer.add(user, currentGraph, foo);

                // update the closure.
                retractionBuffer.doClosure();

                // dump after re-closure.
                store.dumpStore(true,true,false);

                // test the kb.
                assertFalse(store.hasStatement(user, currentGraph, foo));
                assertTrue(store.hasStatement(currentGraph, rdfsRange, graph));
                assertTrue(store.hasStatement(foo, rdftype, graph));

                // verify that stmt c is marked as explicit in the kb.

                StatementWithType stmtC = (StatementWithType) store
                        .getStatement(foo, rdftype, graph);
                
                assertNotNull(stmtC);
                
                assertEquals(StatementEnum.Explicit, stmtC.getStatementType());
                
            }
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * This test demonstrates TM incorrectness.  I add three statements into 
     * store A, then remove one of them.  Then I add the two statements that 
     * remain in store A into store B and compare the stores.  They should be 
     * the same, right?  Well, unfortunately they are not the same.  Too many 
     * inferences were deleted from the first store during TM. 
     */
    public void test_closurecorrectness() {
        
        URI a = new URIImpl("http://www.bigdata.com/a");
        URI b = new URIImpl("http://www.bigdata.com/b");
        URI c = new URIImpl("http://www.bigdata.com/c");
        URI d = new URIImpl("http://www.bigdata.com/d");
        URI sco = URIImpl.RDFS_SUBCLASSOF;

        AbstractTripleStore store = getStore();
        
        Properties properties = store.getProperties();
        
        TempTripleStore tempStore = new TempTripleStore(properties);
        
        try {
            
            // add three
            {
            
                TMStatementBuffer assertionBuffer = new TMStatementBuffer(
                        store.getInferenceEngine(),
                        100/* capacity */, BufferEnum.AssertionBuffer);

                assertionBuffer.add(a, sco, b );
                assertionBuffer.add(b, sco, c );
                assertionBuffer.add(c, sco, d );
                
                // perform closure and write on the database.
                assertionBuffer.doClosure();

                // dump after closure.
                store.dumpStore(true,true,false);

            }
            
            // retract one
            {
                
                TMStatementBuffer retractionBuffer = new TMStatementBuffer(
                        store.getInferenceEngine(),
                        100/* capacity */, BufferEnum.RetractionBuffer);

                retractionBuffer.add(b, sco, c);

                // update the closure.
                retractionBuffer.doClosure();

                // dump after re-closure.
                store.dumpStore(true,true,false);
                
            }
            
            // add two to the "control store"
            {
            
                TMStatementBuffer assertionBuffer = new TMStatementBuffer(
                        tempStore.getInferenceEngine(),
                        100/* capacity */, BufferEnum.AssertionBuffer);

                assertionBuffer.add(a, sco, b );
                assertionBuffer.add(c, sco, d );
                
                // perform closure and write on the database.
                assertionBuffer.doClosure();

                // dump after closure.
                tempStore.dumpStore(true,true,false);

            }
            
            assertSameGraphs( tempStore, store );
            
        } finally {
            
            store.closeAndDelete();
            
            tempStore.closeAndDelete();
            
        }

    }
    
    /**
     * This test demonstrates an infinite loop in TM.
     */
    public void test_infiniteloop() {
     
        URI a = new URIImpl("http://www.bigdata.com/a");
        URI b = new URIImpl("http://www.bigdata.com/b");
        //URI x = new URIImpl("http://www.bigdata.com/x");
        //URI y = new URIImpl("http://www.bigdata.com/y");
        URI entity = new URIImpl("http://www.bigdata.com/Entity");
        URI sameAs = new URIImpl( OWL.SAMEAS );
        URI rdfType = URIImpl.RDF_TYPE;

        AbstractTripleStore store = getStore();
        
        try {
            
            InferenceEngine inf = store.getInferenceEngine();

            // add some assertions and verify aspects of their closure.
            {
            
                TMStatementBuffer assertionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.AssertionBuffer);

                // stmt a
                assertionBuffer.add(a, rdfType, entity );
                // assertionBuffer.add(a, x, y );
                
                // stmt b
                assertionBuffer.add(b, rdfType, entity );
                
                // assert the sameas
                assertionBuffer.add(a, sameAs, b );
                
                // perform closure and write on the database.
                assertionBuffer.doClosure();

                // dump after closure.
                store.dumpStore(true,true,false);

            }
            
            /*
             * retract stmt A and update the closure.
             * 
             * then verify that it is retracted statement is gone and that the
             * other explicit statements were not touched.
             */
            {
                
                TMStatementBuffer retractionBuffer = new TMStatementBuffer(inf,
                        100/* capacity */, BufferEnum.RetractionBuffer);

                // retract the sameas
                retractionBuffer.add(a, sameAs, b);

                // update the closure.
                retractionBuffer.doClosure();

                // dump after re-closure.
                store.dumpStore(true,true,false);
                
            }
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * This is a stress test for truth maintenance. It verifies that retraction
     * and assertion are symmetric by randomly retracting and then asserting
     * statements while using truth maintenance and verifying that the initial
     * conditions are always recovered. It does NOT prove the correctness of the
     * entailments, merely that retraction and assertion are symmetric.
     */
    public void test_stress() {

        String[] resource = new String[] {
                "../rdf-data/alibaba_data.rdf",
                "../rdf-data/alibaba_schema.rdf" };

        String[] baseURL = new String[] { "", "" 
                };

        RDFFormat[] format = new RDFFormat[] {
                RDFFormat.RDFXML,
                RDFFormat.RDFXML
                };

        for(String r : resource) {
            
            if(!new File(r).exists()) {
                
                System.err.println("Resource not found: "+r+", test="+getName()+" skipped.");
                
                return;
                
            }
            
        }

        AbstractTripleStore store = getStore();
        
        Properties properties = store.getProperties();

        try {
            
            /*
             * Note: overrides properties to make sure that entailments are
             * not computed on load.
             */

            properties.setProperty(DataLoader.Options.CLOSURE,
                    ClosureEnum.None.toString());

            DataLoader dataLoader = new DataLoader(properties, store);

            // load and close using an incremental approach.
            dataLoader.loadData(resource, baseURL, format);

            /*
             * Compute the closure of the database.
             */
            
            InferenceEngine inf = new InferenceEngine(properties,store);
            
            inf.computeClosure(null/*focusStore*/);

            /*
             * Make a copy of the graph that will serve as ground truth.
             */
            
            TempTripleStore tmp = new TempTripleStore(properties);
            
            store.copyStatements(tmp, null/*filter*/);
            
            /*
             * Start the stress tests.
             */

            doStressTest(tmp, inf, 10/*ntrials*/, 1/*depth*/, 1/*nstmts*/);

            doStressTest(tmp, inf, 10/*ntrials*/, 1/*depth*/, 5/*nstmts*/);

            doStressTest(tmp, inf, 10/*ntrials*/, 5/*depth*/, 1/*nstmts*/);

            doStressTest(tmp, inf, 5/*ntrials*/, 5/*depth*/, 5/*nstmts*/);

//            // very stressful.
//            doStressTest(tmp, inf, 100/*ntrials*/, 10/*depth*/, 20/*nstmts*/);
            
        } catch(IOException ex) {
            
            fail("Not expecting: "+ex, ex);
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * A stress test for truth maintenance using an arbitrary data set. The test
     * scans the statement indices in some order, selecting N explicit statement
     * to retract. It then retracts them, updates the closure, and then
     * re-asserts them and verifies that original closure was restored.
     * <p>
     * Note: this test by itself does not guarentee that any entailments of
     * those explicit statements were removed - we need to write other tests for
     * that. repeat several times on the dataset, potentially doing multiple
     * retractions before we back out of them.
     * 
     * @param tmp
     *            Ground truth (the state that must be recovered in order for
     *            truth maintenance to be symmetric).
     * @param inf
     *            The {@link InferenceEngine} to use in the test. This reads and
     *            writes on the database whose closure is being maintained.
     * @param ntrials
     *            The #of times that we will run the test.
     * @param D
     *            The recursive depth of the retractions. A depth of ONE (1)
     *            means that one set of N statements will be retracted, closure
     *            updated, and the re-asserted and closure updated and compared
     *            against ground truth (the initial conditions). When the depth
     *            is greater than ONE (1) we will recursively retract a set of N
     *            statements D times. The statements will be reasserted as we
     *            back out of the recursion and the graph compared with ground
     *            truth when we return from the top level of the recursion.
     * @param N
     *            The #of explicit statements to be randomly selected and
     *            retracted on each recursive pass.
     */
    public void doStressTest(TempTripleStore tmp, InferenceEngine inf,
            int ntrials, int D, int N) {

        AbstractTripleStore store = inf.database;
        
        /*
         * Verify our initial conditions agree.
         */
        
        assertSameGraphs( tmp, store );

        for (int trial = 0; trial < ntrials; trial++) {

            /*
             * Do recursion.
             */

            MDC.put("trial", "trial="+trial);

            retractAndAssert(inf,store,0/*depth*/,D,N);

            /*
             * Verify that the closure is correct after all that recursive
             * mutation and restoration.
             */

            assertSameGraphs(tmp, store);

            MDC.remove("trial");
            
        }
        
    }

    /**
     * At each level of recursion up to N explicit statements are selected
     * randomly from the database, retracted, and closure is updated. The method
     * then calls itself recursively, thereby building up a series of updates to
     * the graph. When the recursion bottoms out, the retracted statements are
     * asserted and closure is updated. This continues as we back out of the
     * recursion until the graph SHOULD contain the identical RDF model.
     * 
     * @param inf
     *            Used to update the closure.
     * @param db
     *            The database.
     * @param depth
     *            The current depth of recursion (ZERO on the first call).
     * @param D
     *            The maximum depth of recursion (depth will always be strictly
     *            less than D).
     * @param N
     *            The #of explicit statements to randomly select at each level
     *            of recursion for retraction from the database.
     * 
     * @todo update logic to use TMSPOBuffer.
     * 
     * 
     */
    private void retractAndAssert(InferenceEngine inf, AbstractTripleStore db,
            int depth, final int D, final int N) {

        assert depth >= 0;
        assert depth < D;

        /*
         * FIXME Select N explicit statements at random.
         */
        
        SPO[] stmts = selectRandomExplicitStatements(db, N);

        // shows an error in direct byte buffer.
//        SPO[] stmts = new SPO[] {
//                new SPO(
////                        #entity-103  #telephoneNumber-10144  "436-7482"
//                        db.getTermId(new URIImpl("http://localhost/rdf/alibaba_v41.rdf#entity-103")),
//                        db.getTermId(new URIImpl("http://localhost/rdf/alibaba_v41.rdf#telephoneNumber-10144")),
//                        db.getTermId(new LiteralImpl("436-7482")),
//                        StatementEnum.Explicit
//                        )
//        };
        
        log.info("Selected "+stmts.length+" statements at random: depth="+depth);

        /*
         * Retract those statements and update the closure of the database.
         */
        {

            for(SPO tmp : stmts) {
                log.info("Retracting: "+tmp.toString(db));
            }
            
            // FIXME refactor as TMSPOBuffer.
            TMStatementBuffer retractionBuffer = new TMStatementBuffer(inf, N,
                    BufferEnum.RetractionBuffer);

            StatementIterator itr = new SesameStatementIterator(db,
                    new SPOArrayIterator(stmts, stmts.length));

            try {

                while(itr.hasNext()) {

                    Statement stmt = itr.next();

                    retractionBuffer.add(stmt.getSubject(),
                            stmt.getPredicate(), stmt.getObject());

                }
                
            } finally {

                itr.close();
                
            }

            log.info("Retracting: n="+stmts.length+", depth="+depth);

            // note: an upper bound when using isolated indices.
            final long before = db.getStatementCount();
            
            retractionBuffer.doClosure();
            
            final long after = db.getStatementCount();
            
            final long delta = after - before;
            
            log.info("Retraction: before="+before+", after="+after+", delta="+delta);
            
        }
        
        if (depth + 1 < D) {
            
            retractAndAssert(inf, db, depth+1, D, N);
            
        }
        
        /*
         * Assert those statements and update the closure of the database.
         */
        {

            for(SPO tmp : stmts) {
                log.info("Asserting: "+tmp.toString(db));
            }

            // FIXME refactor as TMSPOBuffer.
            TMStatementBuffer assertionBuffer = new TMStatementBuffer(inf, N,
                    BufferEnum.AssertionBuffer);

            StatementIterator itr = new SesameStatementIterator(db,
                    new SPOArrayIterator(stmts, stmts.length));

            try {

                while(itr.hasNext()) {
 
                    Statement stmt = itr.next();

                    assertionBuffer.add(stmt.getSubject(),
                            stmt.getPredicate(), stmt.getObject());

                }
                
            } finally {

                itr.close();
                
            }

            log.info("Asserting: n="+stmts.length+", depth="+depth);

            // note: an upper bound when using isolated indices.
            final long before = db.getStatementCount();
            
            assertionBuffer.doClosure();
            
            final long after = db.getStatementCount();
            
            final long delta = after - before;
            
            log.info("Assertion: before="+before+", after="+after+", delta="+delta);

        }
        
    }
    
    /**
     * Select N explicit statements from the graph at random.
     * 
     * @param db
     *            The graph.
     * @param N
     *            The #of statements to select.
     * 
     * @return Up to N distinct explicit statements selected from the graph.
     */
    public SPO[] selectRandomExplicitStatements(AbstractTripleStore db, int N) {
        
        Random r = new Random();
        
//        RdfKeyBuilder keyBuilder = new RdfKeyBuilder(new KeyBuilder(Bytes.SIZEOF_LONG));

        /*
         * Count the #of distinct subjects in the graph.
         */
        final int nsubjects;
        {

            Iterator<Long> termIds = db.getAccessPath(KeyOrder.SPO).distinctTermScan();

            int n = 0;

            while(termIds.hasNext()) {
                
                termIds.next();
                
                n++;
                
            }
            
            nsubjects = n;

        }

        log.info("There are "+nsubjects+" distinct subjects");

        /*
         * Choose N distinct subjects from the graph at random.
         */

        Set<Long> subjects = new HashSet<Long>(N);

        for (int i = 0; i < nsubjects && subjects.size() < N; i++) {

            Iterator<Long> termIds = db.getAccessPath(KeyOrder.SPO)
                    .distinctTermScan();

            // choose subject at random.
            int index = r.nextInt( nsubjects );
            
            long s = NULL;
            
            for (int j = 0; termIds.hasNext() && j < index; j++) {

                s = termIds.next();

            }

            subjects.add( s );
            
        }

        log.info("Selected "+subjects.size()+" distinct subjects: "+subjects);

        /*
         * Choose one explicit statement at random for each distinct subject.
         * 
         * Note: It is possible that some subjects will not have any explicit
         * statements, in which case we will select fewer than N statements.
         */
        
        List<SPO> stmts = new ArrayList<SPO>(N);
        
        for( long s : subjects ) {
            
            IAccessPath accessPath = db.getAccessPath(s,NULL,NULL);
            
            ISPOIterator itr = accessPath.iterator(//0, 0,
                    ExplicitSPOFilter.INSTANCE);
            
            if(!itr.hasNext()) continue;
            
            // a chunk of explicit statements for that subject.
            SPO[] chunk = itr.nextChunk();
            
            // statement randomly choosen from that chunk.
            SPO tmp = chunk[r.nextInt(chunk.length)];
            
            log.info("Selected at random: "+tmp.toString(db));
            
            stmts.add(tmp);
            
        }
        
        log.info("Selected "+stmts.size()+" distinct statements: "+stmts);
        
        return stmts.toArray(new SPO[stmts.size()]);
        
    }
    
    /**
     * This is a specialized test for equality in the graphs that simply compare
     * scans on the SPO index.
     * <p>
     * Pre-condition: The term identifiers for the graphs MUST be consistently
     * assigned since the statements are not being materialized as RDF
     * {@link Value} objects.
     * 
     * @param expected
     *            A copy of the statements made after the data set was loaded
     *            and its closure computed and before we began to retract and
     *            assert stuff.
     * 
     * @param actual
     *            Note that this is used by both graphs to resolve the term
     *            identifiers.
     */
    protected void assertSameGraphs(TempTripleStore expected,
            AbstractTripleStore actual) {

        // For the truly paranoid.
//        assertStatementIndicesConsistent(expected);
//        
//        assertStatementIndicesConsistent(actual);
        
        if (expected.getStatementCount() != actual.getStatementCount()) {

            log.warn("statementCount: expected=" + expected.getStatementCount()
                    + ", but actual=" + actual.getStatementCount());
                    
        }
        
        ISPOIterator itre = expected.getAccessPath(KeyOrder.SPO).iterator();

        ISPOIterator itra = actual.getAccessPath(KeyOrder.SPO).iterator();

        int i = 0;

        int nerrs = 0;
        
        int maxerrs = 10;
        
        try {

            while (itre.hasNext()) {

                assert itra.hasNext();

                SPO expectedSPO = itre.next();
                
                SPO actualSPO = itra.next();
                
                if (!expectedSPO.equals(actualSPO)) {

//                    // Note: term identifiers are all in [actual].
//                    fail("index=" + i + ", expected="
//                            + expectedSPO.toString(actual) + ", actual="
//                            + actualSPO.toString(actual));

                    while (actualSPO.compareTo(expectedSPO) < 0) {

                        log.warn("Not expecting: " + actualSPO.toString(actual));

                        if(!itra.hasNext()) break;
                        
                        actualSPO = itra.next();
                        
                        if(nerrs++==maxerrs) fail("Too many errors");

                    }

                    while (expectedSPO.compareTo(actualSPO) < 0) {

                        log.warn("Expecting: " + expectedSPO.toString(actual));

                        if(!itre.hasNext()) break;
                        
                        expectedSPO = itre.next();

                        if(nerrs++==maxerrs) fail("Too many errors");

                    }

                }
                
                i++;

            }

            assertFalse(itra.hasNext());

        } finally {

            itre.close();

            itra.close();

        }

        assertEquals("statementCount", expected.getStatementCount(), actual
                .getStatementCount());

    }
    
}
