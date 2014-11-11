/**

Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.join.DistinctTermScanOp;
import com.bigdata.rdf.sparql.ast.optimizers.ASTFastRangeCountOptimizer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
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
					"distinctTermScan_triples_02.ttl",// dataFileURL
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
					"distinctTermScan_triples_03.ttl",// dataFileURL
					"distinctTermScan_triples_03.srx"// resultFileURL
			);
			
			h.runTest();

			// Verify that the DistinctTermScanOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							DistinctTermScanOp.class).size());

		}

	}

	public static class TestQuadsModeAPs extends TestDistinctTermScanOptimizer {

		/**
		 * <pre>
		 * SELECT DISTINCT ?p WHERE { ?s ?p ?o . }
		 * </pre>
		 * 
		 * FIXME For quads we need to test all of the combinations of default
		 * and named graph modes and both with and without the GRAPH {} wrapping
		 * the triple pattern. These tests can be run both with and without the
		 * optimizer if we define a query hint to disable it. Or you can just
		 * disable it temporarily in the DefaultOptimizerList when developing
		 * the test suite in order to validate the correctness of the test suite
		 * without the optimizer in play.
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

	}

}
