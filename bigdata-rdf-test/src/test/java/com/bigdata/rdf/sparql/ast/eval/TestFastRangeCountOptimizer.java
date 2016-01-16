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
import com.bigdata.bop.join.FastRangeCountOp;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1037" > Rewrite SELECT
 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD </a>
 */
public class TestFastRangeCountOptimizer extends
		AbstractDataDrivenSPARQLTestCase {

	public TestFastRangeCountOptimizer() {
	}

	public TestFastRangeCountOptimizer(String name) {
		super(name);
	}

    public static Test suite()
    {

        final TestSuite suite = new TestSuite(TestFastRangeCountOptimizer.class.getSimpleName());

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
		 * SELECT (COUNT(*) as ?count) {?s ?p ?o}
		 * </pre>
		 */
		public void test_fastRangeCount_triples_01() throws Exception {

			final TestHelper h = new TestHelper("fastRangeCount_triples_01", // testURI,
					"fastRangeCount_triples_01.rq",// queryFileURL
					"fastRangeCount_triples_01.ttl",// dataFileURL
					"fastRangeCount_triples_01.srx"// resultFileURL
			);

			h.runTest();

			// Verify that the FastRangeCountOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							FastRangeCountOp.class).size());

		}

		/**
		 * <pre>
		 * SELECT (COUNT(*) as ?count) {?s ?p <http://bigdata.com#o1>}
		 * </pre>
		 */
		public void test_fastRangeCount_triples_02() throws Exception {

			final TestHelper h = new TestHelper("fastRangeCount_triples_02", // testURI,
					"fastRangeCount_triples_02.rq",// queryFileURL
					"fastRangeCount_triples_01.ttl",// dataFileURL
					"fastRangeCount_triples_02.srx"// resultFileURL
			);

			h.runTest();

			// Verify that the FastRangeCountOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							FastRangeCountOp.class).size());

		}

	    /**
	    * The optimization must *not* be applied in the presence of delete
	    * markers. Delete markers are present in the case of isolatable
	    * indices. The query below, which is amenable to optimization in 
	    * principle, cannot be optimized when this mode is used.
	    * 
       * <pre>
       * SELECT (COUNT(*) as ?count) {?s ?p <http://bigdata.com#o1>}
       * </pre>
       */
      public void test_fastRangeCount_delete_markers() throws Exception {

         final TestHelper h = new TestHelper("fastRangeCount_triples_02", // testURI,
               "fastRangeCount_triples_02.rq",// queryFileURL
               "fastRangeCount_triples_01.ttl",// dataFileURL
               "fastRangeCount_triples_02.srx"// resultFileURL
         );
         enableDeleteMarkersInIndes();
         h.runTest();

         // Verify that the FastRangeCountOp was used in the query plan.
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());

      }
		
	}
	
    /**
     * Quads mode test suite.
     * 
	 * FIXME Add explicit tests using FROM and FROM NAMED. 
    */
	public static class TestQuadsModeAPs extends TestFastRangeCountOptimizer {
		
		/**
		 * Default graph query.  Returns the total range count of the index.
		 * <pre>
		 * SELECT (COUNT(*) as ?count) {?s ?p ?o}
		 * </pre>
		 */
		public void test_fastRangeCount_quads_01() throws Exception {

			final TestHelper h = new TestHelper("fastRangeCount_quads_01", // testURI,
					"fastRangeCount_quads_01.rq",// queryFileURL
					"fastRangeCount_quads_01.trig",// dataFileURL
					"fastRangeCount_quads_01.srx"// resultFileURL
			);

			h.runTest();

			// Verify that the FastRangeCountOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							FastRangeCountOp.class).size());

		}

		/**
		 * Default graph query. Returns range count of Oxxx index where O is
		 * bound to a constant.
		 * 
		 * <pre>
		 * SELECT (COUNT(*) as ?count) {?s ?p <http://bigdata.com#o1>}
		 * </pre>
		 */
		public void test_fastRangeCount_quads_02() throws Exception {

			final TestHelper h = new TestHelper("fastRangeCount_quads_02", // testURI,
					"fastRangeCount_quads_02.rq",// queryFileURL
					"fastRangeCount_quads_01.trig",// dataFileURL
					"fastRangeCount_quads_02.srx"// resultFileURL
			);

			h.runTest();

			// Verify that the FastRangeCountOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							FastRangeCountOp.class).size());

		}

		/**
		 * Named graph query where the graph is not constrained. Returns range
		 * count of Oxxx index where O is bound to a constant.
		 * 
		 * <pre>
		 * SELECT (COUNT(*) as ?count) {GRAPH ?g {?s ?p <http://bigdata.com#o1>} }
		 * </pre>
		 */
		public void test_fastRangeCount_quads_03() throws Exception {

			final TestHelper h = new TestHelper("fastRangeCount_quads_03", // testURI,
					"fastRangeCount_quads_03.rq",// queryFileURL
					"fastRangeCount_quads_01.trig",// dataFileURL
					"fastRangeCount_quads_03.srx"// resultFileURL
			);

			h.runTest();

			// Verify that the FastRangeCountOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							FastRangeCountOp.class).size());

		}

		/**
		 * Named graph query where the graph is constrained. Returns range
		 * count of Cxxx index where C is bound to a constant.
		 * 
		 * <pre>
		 * SELECT (COUNT(*) as ?count) {GRAPH <http://bigdata.com#g1> {?s ?p ?o} }
		 * </pre>
		 */
		public void test_fastRangeCount_quads_04() throws Exception {

			final TestHelper h = new TestHelper("fastRangeCount_quads_04", // testURI,
					"fastRangeCount_quads_04.rq",// queryFileURL
					"fastRangeCount_quads_01.trig",// dataFileURL
					"fastRangeCount_quads_04.srx"// resultFileURL
			);

			h.runTest();

			// Verify that the FastRangeCountOp was used in the query plan.
			assertEquals(
					1,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							FastRangeCountOp.class).size());

		}

		/**
		 * Named graph query where the graph is not constrained but the set of
		 * named graphs is constrained by a FROM NAMED clause.
		 * 
		 * <pre>
		 * FROM NAMED <http://bigdata.com#g1>
		 * FROM NAMED <http://bigdata.com#g3>
		 * 
		 * SELECT (COUNT(*) as ?count) {GRAPH ?g {?s ?p ?o} }
		 * </pre>
		 * 
		 * TODO This case CAN be rewritten as the SUM over the range counts of
		 * the individual named-graph APs. This COULD be done as an aggregate or
		 * as a physical operator. However, that has not been done yet so we
		 * MUST NOT rewrite this query.
		 */
		public void test_fastRangeCount_quads_05() throws Exception {

			final TestHelper h = new TestHelper("fastRangeCount_quads_05", // testURI,
					"fastRangeCount_quads_05.rq",// queryFileURL
					"fastRangeCount_quads_01.trig",// dataFileURL
					"fastRangeCount_quads_05.srx"// resultFileURL
			);

			h.runTest();

			// Verify that the FastRangeCountOp was NOT used in the query plan.
			assertEquals(
					0,
					BOpUtility.toList(h.getASTContainer().getQueryPlan(),
							FastRangeCountOp.class).size());

		}
		
	    /**
       * The optimization must *not* be applied in the presence of delete
       * markers. Delete markers are present in the case of isolatable
       * indices. The query below, which is amenable to optimization in 
       * principle, cannot be optimized when this mode is used.
       * 
       * <pre>
       * SELECT (COUNT(*) as ?count) {GRAPH <http://bigdata.com#g1> {?s ?p ?o} }
       * </pre>
       */
      public void test_fastRangeCount_delete_markers() throws Exception {

         final TestHelper h = new TestHelper("fastRangeCount_quads_04", // testURI,
               "fastRangeCount_quads_04.rq",// queryFileURL
               "fastRangeCount_quads_01.trig",// dataFileURL
               "fastRangeCount_quads_04.srx"// resultFileURL
         );
         enableDeleteMarkersInIndes();
         h.runTest();

         // Verify that the FastRangeCountOp was used in the query plan.
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());

      }

	}

}
