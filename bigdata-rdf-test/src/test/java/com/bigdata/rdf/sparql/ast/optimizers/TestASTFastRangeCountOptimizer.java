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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.DISTINCT;
import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.OPTIONAL;
import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.REDUCED;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.QueryRoot;
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
		 * Test rewrite of the:
		 * <pre>SELECT (COUNT(*) as ?w) { OPTIONAL { ?s ?p ?o} }</pre>
		 * OPTIONAL does not effect anything and the fast range 
		 * count will still be correct so the rewrite SHOULD take place.
		 */
		public void test_fastRangeCountOptimizer_quadsMode_optional_pattern() {
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(varNode(s), varNode(p), varNode(o), OPTIONAL)
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(varNode(s), varNode(p), varNode(o), OPTIONAL,
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			}.test();
		}
		
		/**
		 * Test rewrite of:
		 * <pre>SELECT COUNT(*) { GRAPH ?g {?s ?p ?o} }</pre>
		 */
		public void test_fastRangeCountOptimizer_quadsMode_simple_case() {
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(varNode(s), varNode(p), varNode(o), (Object[])varNodes(z))
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(varNode(s), varNode(p), varNode(o),varNode(z),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			}.test();
		}
		
		/**
		 * Test rewrite of:
		 * <pre>SELECT COUNT(*) { GRAPH :g {:s ?p ?o} }</pre>
		 */
		public void test_fastRangeCountOptimizer_quadsMode_constrained_case() {
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(constantNode(b), varNode(p), varNode(o), constantNode(a))
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(constantNode(b), varNode(p), varNode(o), constantNode(a),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
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
											FunctionRegistry.COUNT,
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
		 * Verify correct rewrite of basic combinations with identical semantics:
		 * <pre>SELECT (COUNT(DISTINCT *) AS ?w) {?s ?p ?o}</pre>
		 * <pre>SELECT (COUNT(REDUCED *) AS ?w) {?s ?p ?o}</pre>
		 * <pre>SELECT (COUNT(*) AS ?w) {?s ?p ?o}</pre>
		 */
		public void test_fastRangeCountOptimizer_triplesMode_wildcard() {
			// After optimization expected AST is identical for all cases below.
			class WildCardHelper extends Helper {
								
				public WildCardHelper(HelperFlag... flags) {
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(varNode(s), varNode(p), varNode(o))
								), flags);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(
										varNode(s), varNode(p), varNode(o),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			}
						
			// SELECT (COUNT(*) AS ?w) {?s ?p ?o}
			(new WildCardHelper()).test();
			
			// SELECT (COUNT(DISTINCT *) AS ?w) {?s ?p ?o}
			(new WildCardHelper(DISTINCT)).test();
			
			// SELECT (COUNT(REDUCED *) AS ?w) {?s ?p ?o}
			(new WildCardHelper(REDUCED)).test();
		}
				
		/**
		 * Combinations using a constrained range-count.
		 * <pre>SELECT COUNT(*) {:s ?p ?o}</pre>
		 * <pre>SELECT COUNT(*) {?s :p ?o}</pre>
		 * <pre>SELECT COUNT(*) {?s ?p :o}</pre>
		 * <pre>SELECT COUNT(*) {:s ?p :o}</pre>
		 */
		public void test_fastRangeCountOptimizer_triplesMode_wildcard_with_constraint() {
			//SELECT (COUNT(*) AS ?w) {:s ?p ?o}
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(constantNode(a), varNode(p), varNode(o))
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(
										constantNode(a), varNode(p), varNode(o),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			}.test();
			
			//SELECT (COUNT(*) AS ?w) {?s :p ?o}
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(varNode(s), constantNode(a), varNode(o))
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(
										varNode(s), constantNode(a), varNode(o),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			}.test();
			
			//SELECT (COUNT(*) AS ?w) {?s ?p :o}
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(varNode(s), varNode(p), constantNode(a))
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(
										varNode(s), varNode(p), constantNode(a),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			}.test();
			
			//SELECT (COUNT(*) AS ?w) {:s ?p :o}
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										wildcard()
									), varNode(w)
								)),
								where(
									statementPatternNode(constantNode(b), varNode(p), constantNode(a))
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(
										constantNode(b), varNode(p), constantNode(a),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			}.test();
		}
		
		/**
		 * Combinations using a constrained range-count where the triple pattern is
		 * 1-unbound and the COUNT() references the unbound variable.
		 * <pre>SELECT COUNT(?s) {?s :p :o}</pre>
		 * <pre>SELECT COUNT(?p) {:s ?p :o}</pre>
		 * <pre>SELECT COUNT(?o) {:s :p ?o}</pre>
		 */
		public void test_fastRangeCountOptimizer_triplesMode_wildcard_with_constraint_projection() {
			//SELECT (COUNT(?s) AS ?w) {?s :p :o}
			new Helper() {
				{
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										varNode(s)
									), varNode(w)
								)),
								where(
									statementPatternNode(varNode(s), constantNode(a), constantNode(b))
								)
							);
					
					expected = 
							select(
								projection(varNode(w)), 
								where(
									statementPatternNode(
										varNode(s), constantNode(a), constantNode(b),
										property(Annotations.ESTIMATED_CARDINALITY, 1L),
										property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
									)
								)
							);
				}
			};
		}

		/**
         * <pre>SELECT * { { SELECT COUNT(*) {?s ?p ?o} } }</pre> 
		 */
		public void test_fastRangeCountOptimizer_triplesMode_wildcard_subquery_withot_projection01() {
			new Helper() {
				{
					given = 
							select(
								projection(
									wildcard()
								),
								where(
									selectSubQuery(
										projection(bind(
											functionNode(
												FunctionRegistry.COUNT,
												varNode(s)
											), varNode(w)
										)),
										where(
											statementPatternNode(varNode(s), varNode(p), varNode(o))
										)
									)
								)
							);
					
					expected = 
							select(
								projection(
									wildcard()
								),
								where(
									selectSubQuery(
										projection(bind(
											functionNode(
												FunctionRegistry.COUNT,
												varNode(s)
											), varNode(w)
										)),
										where(
											statementPatternNode(varNode(s), varNode(p), varNode(o),
												property(Annotations.ESTIMATED_CARDINALITY, 1L),
												property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
											)
										)
									)
								)
							);
				}
			};
		}
		
		/**
         * <pre>SELECT * { { SELECT COUNT(*) {?s ?p ?o} } :s :p :o .}</pre> 
		 */
		public void test_fastRangeCountOptimizer_triplesMode_wildcard_subquery_without_projection_02() {
			new Helper() {
				{
					given = 
							select(
								projection(
									wildcard()
								),
								where(
									selectSubQuery(
										projection(bind(
											functionNode(
												FunctionRegistry.COUNT,
												varNode(s)
											), varNode(w)
										)),
										where(
											statementPatternNode(varNode(s), varNode(p), varNode(o))
										)
									),
									statementPatternNode(constantNode(a), constantNode(b), constantNode(c))									
								)
							);
					
					expected = 
							select(
								projection(
									wildcard()
								),
								where(
									selectSubQuery(
										projection(bind(
											functionNode(
												FunctionRegistry.COUNT,
												varNode(s)
											), varNode(w)
										)),
										where(
											statementPatternNode(varNode(s), varNode(p), varNode(o),
												property(Annotations.ESTIMATED_CARDINALITY, 1L),
												property(Annotations.FAST_RANGE_COUNT_VAR, varNode(w))
											)
										)
									),
									statementPatternNode(constantNode(a), constantNode(b), constantNode(c))		
								)
							);
				}
			};
		}
		
		/**
		 * Verify correct rejection:
		 * <pre>SELECT COUNT(?p) {:s ?p ?o}</pre>
		 * <pre>SELECT COUNT(DISTINCT ?p) {:s ?p ?o}</pre>
		 * <pre>SELECT COUNT(REDUCED ?p) {:s ?p ?o}</pre>
		 */
		public void test_fastRangeCountOptimizer_triplesMode_wildcard_rejection() {
			class WildCardHelper extends Helper {
								
				public WildCardHelper(HelperFlag... flags) {
					given = 
							select(
								projection(bind(
									functionNode(
										FunctionRegistry.COUNT,
										varNode(p)
									), varNode(w)
								)),
								where(
									statementPatternNode(constantNode(a), varNode(p), varNode(o))
								), flags);
					
					expected = new QueryRoot(given);
				}
			}
						
			// SELECT (COUNT(?p) AS ?w) {:s ?p ?o}
			(new WildCardHelper()).test();
			
			// SELECT (COUNT(DISTINCT ?p) AS ?w) {:s ?p ?o}
			(new WildCardHelper(DISTINCT)).test();
			
			// SELECT (COUNT(REDUCED ?p) AS ?w) {:s ?p ?o}
			(new WildCardHelper(REDUCED)).test();
		}
		
	} // class TestTriplesModeAPs

}
