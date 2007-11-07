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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.StatementIterator;

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
     * This is a stress test for truth maintenance.
     */
    public void test_stress() {

        String[] resource = new String[] {
                "data/alibaba_data.rdf",
                "data/alibaba_schema.rdf" };

        String[] baseURL = new String[] { "", "" 
                };

        RDFFormat[] format = new RDFFormat[] {
                RDFFormat.RDFXML,
                RDFFormat.RDFXML
                };

        doStressTest(resource, baseURL, format, 100/*ntrials*/, 1/*depth*/, 1/*nstmts*/);
        
//        doStressTest(resource, baseURL, format, 10, 4, 20);
        
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
     * @param resource
     * @param baseURL
     * @param format
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
    public void doStressTest(String[] resource, String[] baseURL,
            RDFFormat[] format, int ntrials, int D, int N) {

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
             * Verify our initial conditions agree.
             */
            
            assertSameGraphs( tmp, store );

            for (int trial = 0; trial < ntrials; trial++) {

                /*
                 * Do recursion.
                 */

                retractAndAssert(inf,store,0/*depth*/,D,N);

                /*
                 * Verify that the closure is correct after all that recursive
                 * mutation and restoration.
                 */

                assertSameGraphs(tmp, store);

            }

        } catch(IOException ex) {
            
            fail("Not expecting: "+ex, ex);
            
        } finally {

            store.closeAndDelete();
            
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
     */
    private void retractAndAssert(InferenceEngine inf, AbstractTripleStore db,
            int depth, final int D, final int N) {

        assert depth >= 0;
        assert depth < D;

        /*
         * Select N explicit statements at random.
         */
        
        SPO[] stmts = selectRandomExplicitStatements(db, N);
        
        /*
         * Retract those statements and update the closure of the database.
         */
        {
        
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

            retractionBuffer.doClosure();
            
        }
        
        if (depth + 1 < D) {
            
            retractAndAssert(inf, db, depth+1, D, N);
            
        }
        
        /*
         * Assert those statements and update the closure of the database.
         */
        {

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

            assertionBuffer.doClosure();

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
            
            ISPOIterator itr = accessPath.iterator(0, 0,
                    ExplicitSPOFilter.INSTANCE);
            
            if(!itr.hasNext()) continue;
            
            // a chunk of explicit statements for that subject.
            SPO[] chunk = itr.nextChunk();
            
            // statement randomly choosen from that chunk.
            stmts.add(chunk[r.nextInt(chunk.length)]);
            
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
     * 
     * @param actual
     */
    protected void assertSameGraphs(AbstractTripleStore expected,
            AbstractTripleStore actual) {

        assertEquals("statementCount", expected.getStatementCount(), actual
                .getStatementCount());

        ISPOIterator itre = expected.getAccessPath(KeyOrder.SPO).iterator(0, 0);

        ISPOIterator itra = actual.getAccessPath(KeyOrder.SPO).iterator(0, 0);

        int i = 0;

        try {

            while (itre.hasNext()) {

                assert itra.hasNext();

                assertEquals("index=" + i, itre.next(), itra.next());

                i++;

            }

            assertFalse(itra.hasNext());

        } finally {

            itre.close();

            itra.close();

        }

    }
    
}
