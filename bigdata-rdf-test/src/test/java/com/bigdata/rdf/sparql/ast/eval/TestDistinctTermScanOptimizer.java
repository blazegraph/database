/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.join.DistinctTermScanOp;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.optimizers.ASTDistinctTermScanOptimizer;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * SPARQL level test suite for the {@link ASTDistinctTermScanOptimizer} and its
 * physical operator {@link DistinctTermScanOp}.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
 * 
 *      FIXME Write SPARQL layer tests correct rejection tests for DISTINCT ?var
 *      that should not have been rewritten (those that have more than one BPG).
 */
public class TestDistinctTermScanOptimizer extends
		AbstractDataDrivenSPARQLTestCase {

	public TestDistinctTermScanOptimizer() {
	}

	public TestDistinctTermScanOptimizer(String name) {
		super(name);
	}
	
    public static Test suite()
    {

        final TestSuite suite = new TestSuite(TestDistinctTermScanOptimizer.class.getSimpleName());

        suite.addTestSuite(TestQuadsModeAPs.class);

        suite.addTestSuite(TestTriplesModeAPs.class);
        
        return suite;
    }

    /**
     * Triples mode test suite.
     */
	public static class TestTriplesModeAPs extends TestDistinctTermScanOptimizer {

		@Override
		public Properties getProperties() {

			final Properties properties = new Properties(super.getProperties());

			// turn off quads.
			properties.setProperty(AbstractTripleStore.Options.QUADS, "false");

			// turn on triples
			properties.setProperty(AbstractTripleStore.Options.TRIPLES_MODE,
					"true");

			return properties;

		}

		/**
		 * <pre>
		 * SELECT DISTINCT ?s WHERE { ?s ?p ?o . }
		 * </pre>
		 */
		public void test_distinctTermScan_triples_01() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_triples_01", // testURI,
					"distinctTermScan_triples_01.rq",// queryFileURL
					"distinctTermScan_triples_01.ttl",// dataFileURL
					"distinctTermScan_triples_01.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * <pre>
		 * SELECT DISTINCT ?p WHERE { ?s ?p ?o . }
		 * </pre>
		 */
		public void test_distinctTermScan_triples_02() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_triples_02", // testURI,
					"distinctTermScan_triples_02.rq",// queryFileURL
					"distinctTermScan_triples_01.ttl",// dataFileURL
					"distinctTermScan_triples_02.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * <pre>
		 * SELECT DISTINCT ?o WHERE { ?s ?p ?o . }
		 * </pre>
		 */
		public void test_distinctTermScan_triples_03() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_triples_03", // testURI,
					"distinctTermScan_triples_03.rq",// queryFileURL
					"distinctTermScan_triples_01.ttl",// dataFileURL
					"distinctTermScan_triples_03.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * Correct rejection test where a variable in the triple pattern appears
		 * more than once. We have to actually run the key-range scan in order
		 * to handle the correlated variable binding.
		 * 
		 * <pre>
		 * SELECT DISTINCT ?p WHERE { ?s ?p ?s . }
		 * </pre>
		 */
		public void test_distinctTermScan_triples_correctRejection_01() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_triples_correctRejection_01", // testURI,
					"distinctTermScan_triples_correctRejection_01.rq",// queryFileURL
					"distinctTermScan_triples_correctRejection_01.ttl",// dataFileURL
					"distinctTermScan_triples_correctRejection_01.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was NOT used in the query plan.
			assertEquals(
					0,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * <pre>
		 * SELECT ?p {
		 * 
		 *    { SELECT DISTINCT ?p WHERE { ?s ?p ?o . } } .
		 *    
		 *    <http://bigdata.com#s1> ?p ?o .
		 *    
		 * }
		 * </pre>
		 */
		public void test_distinctTermScan_triples_subQuery_01() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_triples_subQuery_01", // testURI,
					"distinctTermScan_triples_subQuery_01.rq",// queryFileURL
					"distinctTermScan_triples_01.ttl",// dataFileURL
					"distinctTermScan_triples_subQuery_01.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

			// Verify that the DISTINCT ?p was lifted out as a named subquery.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getOptimizedAST(),
							NamedSubqueryInclude.class).size());

		}

	}

	/**
	 * Note: For quads we need to test all of the combinations of default and
	 * named graph modes and both with and without the GRAPH {} wrapping the
	 * triple pattern. These tests can be run both with and without the
	 * optimizer if we define a query hint to disable it. Or you can just
	 * disable it temporarily in the DefaultOptimizerList when developing the
	 * test suite in order to validate the correctness of the test suite without
	 * the optimizer in play.
	 * 
	 * Note: The quads mode indices are:
	 * <pre>
	 * SPOC
	 * POCS
	 * OCSP
	 * CSPO
	 * PCSO
	 * SOPC
	 * </pre>
	 * 
	 * FIXME Add explicit tests using FROM and FROM NAMED. The optimizer handles
	 * this, but we should have test coverage for it also.
	 */
	public static class TestQuadsModeAPs extends TestDistinctTermScanOptimizer {

		/**
		 * Default graph query on all named graphs. The distinct-term-scan is
		 * used and the index is Pxxx.
		 * 
		 * <pre>
		 * SELECT DISTINCT ?p WHERE { ?s ?p ?o . }
		 * </pre>
		 */
		public void test_distinctTermScan_quads_01() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_quads_01", // testURI,
					"distinctTermScan_quads_01.rq",// queryFileURL
					"distinctTermScan_quads_01.trig",// dataFileURL
					"distinctTermScan_quads_01.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * Default graph query on all named graphs. The distinct-term-scan is
		 * used and the index is Sxxx.
		 * 
		 * <pre>
		 * SELECT DISTINCT ?s WHERE { ?s ?p ?o . }
		 * </pre>
		 */
		public void test_distinctTermScan_quads_01b() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_quads_01b", // testURI,
					"distinctTermScan_quads_01b.rq",// queryFileURL
					"distinctTermScan_quads_01.trig",// dataFileURL
					"distinctTermScan_quads_01b.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * Default graph query on all named graphs. The distinct-term-scan is
		 * used and the index is Oxxx.
		 * 
		 * <pre>
		 * SELECT DISTINCT ?o WHERE { ?s ?p ?o . }
		 * </pre>
		 */
		public void test_distinctTermScan_quads_01c() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_quads_01c", // testURI,
					"distinctTermScan_quads_01c.rq",// queryFileURL
					"distinctTermScan_quads_01.trig",// dataFileURL
					"distinctTermScan_quads_01c.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * Named graph query on all named graphs. The distinct-term-scan is used
		 * and the index is Pxxx.
		 * 
		 * <pre>
		 * SELECT DISTINCT ?p WHERE { GRAPH ?g { ?s ?p ?o . } }
		 * </pre>
		 */
		public void test_distinctTermScan_quads_02() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_quads_02", // testURI,
					"distinctTermScan_quads_02.rq",// queryFileURL
					"distinctTermScan_quads_01.trig",// dataFileURL
					"distinctTermScan_quads_02.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * Correct rejection test. The named graph is bound. We do not have an
		 * index that allows us to to a distinct-term-scan on <code>:g ?p</code>
		 * (it would require a CPxx index, but our only index starting with C is
		 * CSPO).
		 * 
		 * <pre>
		 * SELECT DISTINCT ?p WHERE { GRAPH <http://bigdata.com#g1> { ?s ?p ?o . } }
		 * </pre>
		 */
		public void test_distinctTermScan_quads_correctRejection_01() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_quads_correctRejection_01", // testURI,
					"distinctTermScan_quads_correctRejection_01.rq",// queryFileURL
					"distinctTermScan_quads_01.trig",// dataFileURL
					"distinctTermScan_quads_correctRejection_01.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was NOT used in the query plan.
			assertEquals(
					0,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

		/**
		 * Test verifying that the query
		 * 
		 * <pre>
		 * SELECT DISTINCT ?p WHERE { GRAPH ?g { <http://bigdata.com#s1> ?p ?o . } }
		 * </pre>
		 * 
		 * is optimized using the {@link SPOKeyOrder#SPOC} index.
		 */
		public void test_distinctTermScan_quads_correctRejection_02() throws Exception {

			final TestHelper h = new TestHelper("distinctTermScan_quads_correctRejection_02", // testURI,
					"distinctTermScan_quads_correctRejection_02.rq",// queryFileURL
					"distinctTermScan_quads_01.trig",// dataFileURL
					"distinctTermScan_quads_correctRejection_02.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());
		}
	}
}
