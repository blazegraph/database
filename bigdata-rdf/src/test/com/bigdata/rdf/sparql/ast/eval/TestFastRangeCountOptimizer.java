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
import com.bigdata.bop.join.FastRangeCountOp;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1037" > Rewrite SELECT
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
		public void test_fastRangeCount_01() throws Exception {

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

	}
	
	public static class TestQuadsModeAPs extends TestFastRangeCountOptimizer {
		
		/**
		 * <pre>
		 * SELECT (COUNT(*) as ?count) {?s ?p ?o}
		 * </pre>
		 */
		public void test_fastRangeCount_01() throws Exception {

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

	}

}
