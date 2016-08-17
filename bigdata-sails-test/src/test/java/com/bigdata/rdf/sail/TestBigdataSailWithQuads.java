/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.sparql.TestVerifyAggregates;
import com.bigdata.rdf.sail.tck.BigdataComplexSparqlQueryTest;
import com.bigdata.rdf.sail.tck.BigdataConnectionTest;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateConformanceTest;
import com.bigdata.rdf.sail.tck.BigdataSparqlFullRWTxTest;
import com.bigdata.rdf.sail.tck.BigdataSparqlTest;
import com.bigdata.rdf.sail.tck.BigdataStoreTest;

/**
 * Test suite for the {@link BigdataSail} with quads enabled. The provenance
 * mode is disabled. Inference is disabled. This version of the test suite uses
 * the pipeline join algorithm.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBigdataSailWithQuads extends AbstractBigdataSailTestCase {

    /**
     * 
     */
    public TestBigdataSailWithQuads() {
    }

    public TestBigdataSailWithQuads(final String name) {

        super(name);
        
    }
    
    public static Test suite() {
        
        final TestBigdataSailWithQuads delegate = new TestBigdataSailWithQuads(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate, "SAIL with Quads (pipeline joins)");

        // test rewrite of RDF Value => BigdataValue for binding set and tuple expr.
        suite.addTestSuite(TestBigdataValueReplacer.class);

        // test pruning of variables not required for downstream processing.
        suite.addTestSuite(TestPruneBindingSets.class);

        // misc named graph API stuff.
        suite.addTestSuite(TestQuadsAPI.class);

// Note: Ported to data driven test.
//        // SPARQL named graphs tests.
//        suite.addTestSuite(TestNamedGraphs.class);

        // test suite for optionals handling (left joins).
        suite.addTestSuite(TestOptionals.class);

        // test of the search magic predicate
        suite.addTestSuite(TestSearchQuery.class);

        // test of high-level query on a graph with statements about statements.
        suite.addTestSuite(TestProvenanceQuery.class);

        suite.addTestSuite(TestOrderBy.class);
        
        suite.addTestSuite(TestUnions.class);
        
        suite.addTestSuite(TestMultiGraphs.class);
        
        suite.addTestSuite(TestInlineValues.class);

        // Validation logic for aggregation operators.
        suite.addTestSuite(TestVerifyAggregates.class);
        
        suite.addTestSuite(TestConcurrentKBCreate.TestWithGroupCommit.class);
        suite.addTestSuite(TestConcurrentKBCreate.TestWithoutGroupCommit.class);

        suite.addTestSuite(TestTxCreate.class);
        suite.addTestSuite(TestCnxnCreate.class);

        suite.addTestSuite(TestChangeSets.class);

        // test suite for the history index.
        suite.addTestSuite(TestHistoryIndex.class);

		suite.addTestSuite(com.bigdata.rdf.sail.TestRollbacks.class);
		suite.addTestSuite(com.bigdata.rdf.sail.TestRollbacksTx.class);
		suite.addTestSuite(com.bigdata.rdf.sail.TestMROWTransactionsNoHistory.class);
		suite.addTestSuite(com.bigdata.rdf.sail.TestMROWTransactionsWithHistory.class);
        
		suite.addTestSuite(com.bigdata.rdf.sail.TestMillisecondPrecisionForInlineDateTimes.class);
		
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket275.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket276.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket348.class);
//        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket352.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket353.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket355.class);
//      suite.addTestSuite(com.bigdata.rdf.sail.TestTicket361.class);
	suite.addTestSuite(com.bigdata.rdf.sail.TestTicket422.class);
		suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1747.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1753.class);

        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1755.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1785.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1788.class);

        suite.addTestSuite(com.bigdata.rdf.sail.DavidsTestBOps.class);

        suite.addTestSuite(com.bigdata.rdf.sail.TestLexJoinOps.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestMaterialization.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket632.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket669.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestTicket1889.class);

        suite.addTestSuite(com.bigdata.rdf.sail.TestNoExceptions.class);

        // The Sesame TCK, including the SPARQL test suite.
        {

            final TestSuite tckSuite = new TestSuite("Sesame 2.x TCK");

            // Sesame Sail test.
            tckSuite.addTestSuite(BigdataStoreTest.class);

            // Sesame SailConnection test.
            tckSuite.addTestSuite(BigdataConnectionTest.class);

            try {

                /*
                 * suite() will call suiteLTSWithPipelineJoins() and then
                 * filter out the dataset tests, which we don't need right now
                 */
//                tckSuite.addTest(BigdataSparqlTest.suiteLTSWithPipelineJoins());
                tckSuite.addTest(BigdataSparqlTest.suite()); // w/ unisolated connection.
                tckSuite.addTest(BigdataSparqlFullRWTxTest.suite()); // w/ full read/write tx.

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

            suite.addTest(tckSuite);

            /*
             * SPARQL 1.1 test suite for things which do not fit in with the
             * manifest test design.
             * 
             * FIXME This should be run for full r/w tx, the embedded federation
             * and scale-out, not just quads.
             */
            tckSuite.addTestSuite(BigdataComplexSparqlQueryTest.class);

            /*
             * Note: The SPARQL 1.1 update test suite is run from
             * com.bigdata.rdf.sparql.ast.eval.update.TestAll.
             */
//            tckSuite.addTestSuite(BigdataSPARQLUpdateTest.class);
            try {
                tckSuite.addTest(BigdataSPARQLUpdateConformanceTest.suite());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
        
        return suite;
        
    }
    
    private Properties properties = null;
    
    @Override
    protected void tearDown(final ProxyBigdataSailTestCase testCase) throws Exception {

        super.tearDown(testCase);
        
        properties = null;
        
    }
    
    @Override
    protected BigdataSail getSail(final Properties properties) {

        this.properties = properties;
        
        return new BigdataSail(properties);
        
    }

    @Override
    public Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());
/*
        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");

        properties.setProperty(Options.QUADS, "true");

        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
*/
        properties.setProperty(Options.QUADS_MODE, "true");
        
        properties.setProperty(Options.TRUTH_MAINTENANCE, "false");

//        properties.setProperty(AbstractResource.Options.NESTED_SUBQUERY, "false");

        return properties;
        
    }
    
    @Override
    protected BigdataSail reopenSail(final BigdataSail sail) {

//        final Properties properties = sail.getProperties();

        if (sail.isOpen()) {

            try {

                sail.shutDown();

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        return getSail(properties);
        
    }

}
