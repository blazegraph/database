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

import org.junit.Ignore;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.bop.IPredicate;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.DistinctTermAdvancer;

import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.*;


/**
 * Test suite for {@link ASTDistinctTermScanOptimizer}.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
 *      
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestASTDistinctTermScanOptimizer extends AbstractOptimizerTestCase {

	public TestASTDistinctTermScanOptimizer() {
		super();
	}

	public TestASTDistinctTermScanOptimizer(final String name) {
		super(name);
	}

	@Override
	IASTOptimizer newOptimizer() {

		return new ASTOptimizerList(new ASTDistinctTermScanOptimizer());
		
	}

    public static Test suite()
    {

        final TestSuite suite = new TestSuite(ASTFastRangeCountOptimizer.class.getSimpleName());

        suite.addTestSuite(TestQuadsModeAPs.class);

        suite.addTestSuite(TestTriplesModeAPs.class);
        
        return suite;
    }
    
    protected static abstract class AbstractASTDistinctTermScanTest extends TestASTDistinctTermScanOptimizer {
		/**
		 * 
		 * This is translated into a {@link DistinctTermAdvancer} and the
		 * distinct terms are simply bound onto ?s. The DISTINCT or REDUCED
		 * annotations are REQUIRED in the original AST and are NOT present in
		 * the rewritten AST (the distinct term scan always provides distinct
		 * solutions).
		 * 
		 * Note: This is only possible when the values of the other variables
		 * are ignored (rather than being projected out).
		 * 
		 * <pre>
		 * SELECT (DISTINCT|REDUCED) ?s {?s ?p ?o}
		 * </pre>
		 * 
		 * TODO While it is possible to use the {@link DistinctTermAdvancer}
		 * where there are some constants in the statement pattern. E.g., <code>
		 * SELECT DISTINCT ?o {:s ?p ?o}
		 * </code>, this is only advantageous for QUADS when the context
		 * position is bound to a constant and the size of the named graph is
		 * large. In all other cases we are better off just using a pipeline
		 * join (standard join).
		 * 
		 * FIXME Write an explicit test for a default graph access path. The
		 * graph variable is ignored and not projected out. The distinct term
		 * advancer will automatically possess the appropriate RDF Merge
		 * semantics.
		 * 
		 */
		public void test_distinctTermScanOptimizer_01() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{

						final StatementPatternNode sp1 = newStatementPatternNode(
								new VarNode(s), new VarNode(p), new VarNode(o));

						/*
						 * Note: The assumption is that the range counts have
						 * already been attached for the basic triple pattern.
						 */
						sp1.setProperty(Annotations.ESTIMATED_CARDINALITY,
								rangeCount_sp1);

						final ProjectionNode projection = projection(varNode(s));
						projection.setDistinct(true);

						given = select(projection, where(sp1));

					}

					/**
					 * We need to convert:
					 * 
					 * <pre>
					 * SELECT DISTINCT ?s {?s ?p ?o}
					 * </pre>
					 * 
					 * into
					 * 
					 * <pre>
					 * SELECT ?s {?s ?p ?o}
					 * </pre>
					 * 
					 * where the triple pattern has been marked with the
					 * DISTINCT-TERM-SCAN:=[?s] annotation; and
					 * 
					 * where the ESTIMATED_CARDINALITY of the triple pattern has
					 * been set to N/M * range-count(triple-pattern), where N is
					 * the number of prefix components that are projected out of
					 * the distinct term scan and M is the number of components
					 * in the key.
					 * 
					 * Note: The DISTINCT/REDUCED annotation is REMOVED from the
					 * SELECT. The distinct term scan automatically imposes
					 * DISTINCT on the bound variables.
					 * 
					 * Note: This requires coordination with the optimizer that
					 * attaches the range counts. I.e., it should either run
					 * first or not run if we have already attached the
					 * estimates for a given triple pattern.
					 */
					{
						// the triple pattern.
						final StatementPatternNode sp1 = newStatementPatternNode(
								new VarNode(s), new VarNode(p), new VarNode(o));
						/*
						 * Annotate with the name of the variable(s) to become
						 * bound to the fast range count of that triple pattern.
						 */
						final VarNode distinctTermScanVar = new VarNode(s);
						sp1.setDistinctTermScanVar(distinctTermScanVar);
						/*
						 * Estimate the cardinality of the distinct term scan
						 * access path. This is just a linear estimate based on
						 * assuming that we can do a proportional fraction of
						 * the work using the distinct term scan depending on
						 * how many distinct term scan variables there are and
						 * the arity and cardinality of the underlying triple or
						 * quad pattern access path.
						 */
						final long newRangeCount = (long) (1.0 / (store
								.isQuads() ? 4 : 3)) * rangeCount_sp1;
						/*
						 * Update the estimated cardinality on the SP.
						 */
						sp1.setProperty(Annotations.ESTIMATED_CARDINALITY,
								newRangeCount);
						// the optimizer also adds a SOPC key order to be used by the access path
						sp1.setQueryHint(IPredicate.Annotations.KEY_ORDER, "SPOC");

						// Note: DISTINCT|REDUCED are NOT part of the
						// projection.
						final ProjectionNode projection = projection(varNode(s));

						projection.setDistinct(false);
						projection.setReduced(false);

						// the expected AST.
						expected = select(projection, where(sp1));

					}

				}
			}.test();

		}
    	
    	/**
		 * The triple pattern can be OPTIONAL (simple
		 * optional). This just means that we produce no bindings for ?s, which
		 * is exactly what would happen anyway in a SELECT with a single
		 * required triple pattern.
		 * 
		 * <pre>
		 * SELECT (DISTINCT|REDUCED) ?s { OPTIONAL { ?s ?p ?o} }
		 * </pre>
		 */
		public void test_distinctTermScanOptimizer_optional_pattern() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{
						given = 
								select(
									projection(
										varNode(s)
									),
									where(
										statementPatternNode(
											varNode(s), varNode(p), varNode(o), varNode(z), OPTIONAL,
											property(Annotations.ESTIMATED_CARDINALITY, rangeCount_sp1)
										)
									), DISTINCT
								);
						
					}
					
					{
						final long newRangeCount = (long) (1.0 / (store
								.isQuads() ? 4 : 3)) * rangeCount_sp1;
						
						StatementPatternNode sp = 
							statementPatternNode(
								varNode(s), varNode(p), varNode(o), varNode(z), OPTIONAL,
								property(Annotations.ESTIMATED_CARDINALITY, newRangeCount),
								property(Annotations.DISTINCT_TERM_SCAN_VAR, varNode(s)));
						sp.setQueryHint(IPredicate.Annotations.KEY_ORDER, "SPOC");

						expected = 
								select(
									projection(
										varNode(s)
									),
									where(sp), NOT_DISTINCT, NOT_REDUCED
								);
					}

				}
			}.test();
		}
		
		/**
		 * Reject optimization in case of the constant in SP.
		 * <pre>SELECT DISTINCT ?s {?s :p ?o} </pre>
		 */
		public void test_distinctTermScanOptimizer_reject_constant_in_sp() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{
						given = 
								select(
									projection(
										varNode(s)
									),
									where(
										statementPatternNode(
											varNode(s), constantNode(a), varNode(o),
											property(Annotations.ESTIMATED_CARDINALITY, rangeCount_sp1)
										)
									), DISTINCT
								);
						
					}
					
					{
						expected = new QueryRoot(given);
					}

				}
			};
		}
    }

	/**
	 * Quads mode specific test suite.
	 */
	public static class TestQuadsModeAPs extends
		AbstractASTDistinctTermScanTest {
		
		/**
		 * SELECT DISTINCT ?s { graph ?g {?s ?p ?o} }
		 */
		public void test_distinctTermScanOptimizer_variable_context_not_projected() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{
						given = 
								select(
									projection(
										varNode(s)
									),
									where(
										statementPatternNode(
											varNode(s), varNode(p), varNode(o), varNode(z),
											property(Annotations.ESTIMATED_CARDINALITY, rangeCount_sp1)
										)
									), DISTINCT
								);
						
					}
					
					{
						final long newRangeCount = (long) (1.0 / (store
								.isQuads() ? 4 : 3)) * rangeCount_sp1;
						
						StatementPatternNode sp =
							statementPatternNode(
									varNode(s), varNode(p), varNode(o), varNode(z),
									property(Annotations.ESTIMATED_CARDINALITY, newRangeCount),
									property(Annotations.DISTINCT_TERM_SCAN_VAR, varNode(s)));
						sp.setQueryHint(IPredicate.Annotations.KEY_ORDER, "SPOC");
						expected = 
								select(
									projection(
										varNode(s)
									),
									where(sp), NOT_DISTINCT, NOT_REDUCED
								);
					}

				}
			}.test();
		}
		
		/**
		 * SELECT DISTINCT ?g { graph ?g {?s ?p ?o} }
		 */
		public void test_distinctTermScanOptimizer_variable_context_projected() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{
						given = 
								select(
									projection(
										varNode(z)
									),
									where(
										statementPatternNode(
											varNode(s), varNode(p), varNode(o), varNode(z),
											property(Annotations.ESTIMATED_CARDINALITY, rangeCount_sp1)
										)
									), DISTINCT
								);
						
					}
					
					{
						final long newRangeCount = (long) (1.0 / (store
								.isQuads() ? 4 : 3)) * rangeCount_sp1;
						
						StatementPatternNode sp = statementPatternNode(
								varNode(s), varNode(p), varNode(o), varNode(z),
								property(Annotations.ESTIMATED_CARDINALITY, newRangeCount),
								property(Annotations.DISTINCT_TERM_SCAN_VAR, varNode(z))
							);
						sp.setQueryHint(IPredicate.Annotations.KEY_ORDER, "CSPO");
						
						expected = 
								select(
									projection(
										varNode(z)
									),
									where(sp), NOT_DISTINCT, NOT_REDUCED
								);
					}

				}
			}.test();
		}
		
		/**
		 * SELECT DISTINCT ?s { graph :g {?s ?p ?o} }
		 */
		@Ignore("edge case, not implemented yet")
		public void test_distinctTermScanOptimizer_bound_context() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{
						given = 
								select(
									projection(
										varNode(s)
									),
									where(
										statementPatternNode(
											varNode(s), varNode(p), varNode(o), constantNode(a),
											property(Annotations.ESTIMATED_CARDINALITY, rangeCount_sp1)
										)
									), DISTINCT
								);
						
					}
					
					{
						final long newRangeCount = (long) (1.0 / (store
								.isQuads() ? 4 : 3)) * rangeCount_sp1;
						
						expected = 
								select(
									projection(
										varNode(s)
									),
									where(
										statementPatternNode(
											varNode(s), varNode(p), varNode(o), constantNode(a),
											property(Annotations.ESTIMATED_CARDINALITY, newRangeCount),
											property(Annotations.DISTINCT_TERM_SCAN_VAR, varNode(s))
										)
									), NOT_DISTINCT, NOT_REDUCED
								);
					}

				}
			};
		}
		
		/**
		 * Reject optimization in case of the constant in SP.
		 * <pre>SELECT DISTINCT ?s { GPRAPH ?g {?s :p ?o}} </pre>
		 */
		public void test_distinctTermScanOptimizer_reject_quads_constant_in_sp() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{
						given = 
								select(
									projection(
										varNode(s)
									),
									where(
										statementPatternNode(
											varNode(s), constantNode(a), varNode(o), varNode(y),
											property(Annotations.ESTIMATED_CARDINALITY, rangeCount_sp1)
										)
									), DISTINCT
								);
						
					}
					
					{
						expected = new QueryRoot(given);
					}

				}
			};
		}
		
		/**
		 * Reject optimization in case of the constant context.
		 * <pre>SELECT DISTINCT ?s { GPRAPH :g {?s ?p ?o}} </pre>
		 */
		public void test_distinctTermScanOptimizer_reject_constant_context() {

			new Helper() {
				{

					final long rangeCount_sp1 = 1000L;

					{
						given = 
								select(
									projection(
										varNode(s)
									),
									where(
										statementPatternNode(
											varNode(s), varNode(p), varNode(o), constantNode(a),
											property(Annotations.ESTIMATED_CARDINALITY, rangeCount_sp1)
										)
									), DISTINCT
								);
						
					}
					
					{
						expected = new QueryRoot(given);
					}

				}
			};
		}

	} // quads

	/**
	 * Quads mode specific test suite.
	 */
	public static class TestTriplesModeAPs extends
		AbstractASTDistinctTermScanTest {
		
	} // triples

}
