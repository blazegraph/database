/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link ASTFastRangeCountOptimizer}. This needs to handle a
 * variety of things related to the following, including where variables are
 * projected into a sub-select (in which case we run the fast range count using
 * the as-bound variables for the triple pattern).
 * 
 * <pre>
 * SELECT COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern}
 * </pre>
 * 
 * @see ASTFastRangeCountOptimizer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestASTFastRangeCountOptimizer extends AbstractOptimizerTestCase {

	public TestASTFastRangeCountOptimizer() {
		super();
	}

	public TestASTFastRangeCountOptimizer(final String name) {
		super(name);
	}

	@Override
	IASTOptimizer newOptimizer() {

		return new ASTOptimizerList(new ASTFastRangeCountOptimizer());
		
	}

    public static Test suite()
    {

        final TestSuite suite = new TestSuite(ASTFastRangeCountOptimizer.class.getSimpleName());

        suite.addTestSuite(TestQuadsModeAPs.class);

        suite.addTestSuite(TestTriplesModeAPs.class);
        
        return suite;
    }

    /**
     * Quads mode specific test suite.
     */
	public static class TestQuadsModeAPs extends TestASTFastRangeCountOptimizer {

		/**
		 * 
		 * <pre>
		 * SELECT (COUNT(*) as ?w) {?s ?p ?o}
		 * </pre>
		 * 
		 * TODO Do a test where the inner triple pattern is OPTIONAL. This does
		 * not effect anything and the fast range count will still be correct so
		 * the rewrite SHOULD take place.
		 */
		public void test_fastRangeCountOptimizer_quads_mode_01() {

			new Helper() {
				{

					given = select(
							projection(bind(
									functionNode(
											FunctionRegistry.COUNT
													.stringValue(),
											new VarNode("*")), varNode(w))),
							where(newStatementPatternNode(new VarNode(s),
									new VarNode(p), new VarNode(o))));

					/**
					 * We need to convert:
					 * 
					 * <pre>
					 * SELECT (COUNT(*) as ?w) {?s ?p ?o}
					 * </pre>
					 * 
					 * into
					 * 
					 * <pre>
					 * SELECT ?w {?s ?p ?o}
					 * </pre>
					 * 
					 * where the triple pattern has been marked with the
					 * FAST-RANGE-COUNT:=?w annotation; and
					 * 
					 * where the ESTIMATED_CARDINALITY of the triple pattern has
					 * been set to ONE (since we will not use a scan).
					 * 
					 * This means that the triple pattern will be directly
					 * interpreted as binding ?w to the ESTCARD of the triple
					 * pattern.
					 */
					// the triple pattern.
					final StatementPatternNode sp1 = newStatementPatternNode(
							new VarNode(s), new VarNode(p), new VarNode(o));
					// annotate with the name of the variable to become bound to
					// the
					// fast range count of that triple pattern.
					sp1.setFastRangeCount(new VarNode(w));
					sp1.setProperty(Annotations.ESTIMATED_CARDINALITY, 1L);

					// the expected AST.
					expected = select(projection(varNode(w)), where(sp1));

				}
			}.test();

		}

		/**
		 * Verify NO rewrite of the following for a quads-mode KB:
		 * 
		 * <pre>
		 * SELECT (COUNT(?s ?p ?o) as ?w) {?s ?p ?o}
		 * </pre>
		 */
		public void test_fastRangeCountOptimizer_quadsMode_correctRejection_1() {

			new Helper() {
				{

					given = select(
							projection(bind(
									functionNode(
											FunctionRegistry.COUNT
													.stringValue(),
											// expression list
											new VarNode(s), new VarNode(p),
											new VarNode(o)//
									), varNode(w) // BIND(COUNT() as ?w)
							)),
							where(newStatementPatternNode(new VarNode(s),
									new VarNode(p), new VarNode(o))));

					// the expected AST.
					expected = select(
							projection(bind(
									functionNode(
											FunctionRegistry.COUNT
													.stringValue(),
											// expression list
											new VarNode(s), new VarNode(p),
											new VarNode(o)//
									), varNode(w) // BIND(COUNT() as ?w)
							)),
							where(newStatementPatternNode(new VarNode(s),
									new VarNode(p), new VarNode(o))));

				}
			}.test();

		}

//		/**
//		 * TODO Do a test to make sure that this optimizer is disabled if the KB
//		 * uses full read/write transactions AND the evaluation context is SPARQL
//		 * UPDATE (vs SPARQL QUERY).
//		 */
//		public void test_fastRangeCountOptimizer_disabledForSPARQLUpdateAndRWTx() {
//			fail("more coverage of different cases");
//		}

	} // class TestQuadsModeAPs

	/**
	 * Triples mode test suite.
	 * 
	 * @author bryan
	 */
	static public class TestTriplesModeAPs extends TestASTFastRangeCountOptimizer {

		public TestTriplesModeAPs() {
			super();
		}

		public TestTriplesModeAPs(final String name) {
			super(name);
		}

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
		 * Verify correct rewrite of
		 * 
		 * <pre>
		 * SELECT (COUNT(?s ?p ?o) as ?w) {?s ?p ?o}
		 * </pre>
		 */
		public void test_fastRangeCountOptimizer_triplesMode_explicitVarNames_01() {

			new Helper() {
				{

					given = select(
							projection(bind(
									functionNode(
											FunctionRegistry.COUNT
													.stringValue(),
											// expression list
											new VarNode(s), new VarNode(p),
											new VarNode(o)//
									), varNode(w) // BIND(COUNT() as ?w)
							)),
							where(newStatementPatternNode(new VarNode(s),
									new VarNode(p), new VarNode(o))));

					/**
					 * We need to convert:
					 * 
					 * <pre>
					 * SELECT (COUNT(?s ?p ?o) as ?w) {?s ?p ?o}
					 * </pre>
					 * 
					 * into
					 * 
					 * <pre>
					 * SELECT ?w {?s ?p ?o}
					 * </pre>
					 * 
					 * where the triple pattern has been marked with the
					 * FAST-RANGE-COUNT:=?w annotation; and
					 * 
					 * where the ESTIMATED_CARDINALITY of the triple pattern has
					 * been set to ONE (since we will not use a scan).
					 * 
					 * This means that the triple pattern will be directly
					 * interpreted as binding ?w to the ESTCARD of the triple
					 * pattern.
					 */
					// the triple pattern.
					final StatementPatternNode sp1 = newStatementPatternNode(
							new VarNode(s), new VarNode(p), new VarNode(o));
					/*
					 * annotate with the name of the variable to become bound to
					 * the fast range count of that triple pattern.
					 */
					sp1.setFastRangeCount(new VarNode(w));
					sp1.setProperty(Annotations.ESTIMATED_CARDINALITY, 1L);

					// the expected AST.
					expected = select(projection(varNode(w)), where(sp1));

				}
			}.test();

		}

		/**
		 * Verify correct rewrite of
		 * 
		 * <pre>
		 * SELECT (COUNT(?p ?p ?s) as ?w) {?s ?p ?o}
		 * </pre>
		 */
		public void test_fastRangeCountOptimizer_triplesMode_explicitVarNames_02() {

			new Helper() {
				{

					given = select(
							projection(bind(
									functionNode(
											FunctionRegistry.COUNT
													.stringValue(),
											// expression list
											new VarNode(o), new VarNode(p),
											new VarNode(s)//
									), varNode(w) // BIND(COUNT() as ?w)
							)),
							where(newStatementPatternNode(new VarNode(s),
									new VarNode(p), new VarNode(o))));

					/**
					 * We need to convert:
					 * 
					 * <pre>
					 * SELECT (COUNT(?s ?p ?o) as ?w) {?s ?p ?o}
					 * </pre>
					 * 
					 * into
					 * 
					 * <pre>
					 * SELECT ?w {?s ?p ?o}
					 * </pre>
					 * 
					 * where the triple pattern has been marked with the
					 * FAST-RANGE-COUNT:=?w annotation; and
					 * 
					 * where the ESTIMATED_CARDINALITY of the triple pattern has
					 * been set to ONE (since we will not use a scan).
					 * 
					 * This means that the triple pattern will be directly
					 * interpreted as binding ?w to the ESTCARD of the triple
					 * pattern.
					 */
					// the triple pattern.
					final StatementPatternNode sp1 = newStatementPatternNode(
							new VarNode(s), new VarNode(p), new VarNode(o));
					// annotate with the name of the variable to become bound to
					// the
					// fast range count of that triple pattern.
					sp1.setFastRangeCount(new VarNode(w));
					sp1.setProperty(Annotations.ESTIMATED_CARDINALITY, 1L);

					// the expected AST.
					expected = select(projection(varNode(w)), where(sp1));

				}
			}.test();

		}

	} // class TestTriplesModeAPs

}
