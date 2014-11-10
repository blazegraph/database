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

import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.DistinctTermAdvancer;

/**
 * Test suite for {@link ASTDistinctTermScanOptimizer}.
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

		/*
		 * FIXME Also include the range count optimizer so we have the range
		 * counts on entry and include some data so we can control what those
		 * range counts are. (alternatively, explicitly set the estimated
		 * cardinality before we run this optimizer in the test setup).
		 */
		return new ASTOptimizerList(new ASTDistinctTermScanOptimizer());
		
	}

	/**
	 * FIXME Handle these cases.
	 * 
	 * This case is quite similar structurally to the fast-range-count but it
	 * gets translated into a {@link DistinctTermAdvancer} in a physical
	 * operator which then counts the number of distinct terms that are visited.
	 * 
	 * <pre>
	 * SELECT (DISTINCT|REDUCED) ?s {?s ?p ?o}
	 * </pre>
	 * 
	 * This case does not include the COUNT(). It is translated into a
	 * {@link DistinctTermAdvancer} and the distinct terms are simply bound onto
	 * ?s. The DISTINCT or REDUCED annotations are REQUIRED in the original AST
	 * and are NOT present in the rewritten AST (the distinct term scan always
	 * provides distinct solutions).
	 * 
	 * Note: This is only possible when the values of the other variables are
	 * ignored.
	 * 
	 * FIXME It is possible to use the {@link DistinctTermAdvancer} where there
	 * are some constants in the statement pattern. E.g.,
	 * 
	 * <pre>
	 * SELECT DISTINCT ?o {:s ?p ?o}
	 * </pre>
	 * 
	 * We need to look at whether the pipeline join would be as efficient, in
	 * which case there is no reason to translate such statement patterns into a
	 * distinct term scan.
	 * 
	 * FIXME Write an explicit test for a default graph access path. The graph
	 * variable is ignored and not projected out. The distinct term advancer
	 * will automatically possess the appropriate RDF Merge semantics.
	 * 
	 * FIXME Write an explicit test for a named graph access path. The graph may
	 * be either bound to a constant or be a variable. If it is a variable, then
	 * it may be either projected out or not. If it is not projected out, then
	 * it is simply ignored.
	 * 
	 * TODO Write test. The triple pattern can be OPTIONAL (simple optional).
	 * This just means that we produce no bindings for ?s, which is exactly what
	 * would happen anyway in a SELECT with a single required triple pattern.
	 * 
	 * <pre>
	 * SELECT (DISTINCT|REDUCED) ?s { OPTIONAL { ?s ?p ?o} }
	 * </pre>
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
				 * been set to N/M * range-count(triple-pattern), where N is the
				 * number of prefix components that are projected out of the
				 * distinct term scan and M is the number of components in the
				 * key.
				 * 
				 * Note: The DISTINCT/REDUCED annotation is REMOVED from the
				 * SELECT. The distinct term scan automatically imposes DISTINCT
				 * on the bound variables.
				 * 
				 * Note: This requires coordination with the optimizer that
				 * attaches the range counts. I.e., it should either run first
				 * or not run if we have already attached the estimates for a
				 * given triple pattern.
				 */
				{
					// the triple pattern.
					final StatementPatternNode sp1 = newStatementPatternNode(
							new VarNode(s), new VarNode(p), new VarNode(o));
					/*
					 * Annotate with the name of the variable(s) to become bound to
					 * the fast range count of that triple pattern.
					 */
					final VarNode[] distinctTermScanVars = new VarNode[] { new VarNode(
							s) };
					sp1.setDistinctTermScanVars(distinctTermScanVars);
					/*
					 * Estimate the cardinality of the distinct term scan access
					 * path. This is just a linear estimate based on assuming
					 * that we can do a proportional fraction of the work using
					 * the distinct term scan depending on how many distinct
					 * term scan variables there are and the arity and
					 * cardinality of the underlying triple or quad pattern
					 * access path.
					 */
					final long newRangeCount = (long) (((double) distinctTermScanVars.length) / (store
							.isQuads() ? 4 : 3)) * rangeCount_sp1;
					/*
					 * Update the estimated cardinality on the SP.
					 */
					sp1.setProperty(Annotations.ESTIMATED_CARDINALITY,
							newRangeCount);

					// Note: DISTINCT|REDUCED are NOT part of the projection.
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
	 * Unit test with a projected prefix consisting of 2 out of 4 components of
	 * the key (e.g., ?s and ?p).
	 * 
	 * <pre>
	 * SELECT ?s ?p { graph ?g {?s ?p ?o} }
	 * </pre>
	 * 
	 * Note: There is no advantage to using the {@link DistinctTermAdvancer}
	 * when the triple pattern is all-but one-bound or even all but two-bound.
	 * In these cases the application of the standard pipeline join operator
	 * should be as efficient and might even be more efficient if we wind up
	 * evaluating the triple pattern "as-bound" with a fully bound pattern since
	 * that turns into a point test.
	 */
	public void test_distinctTermScanOptimizer_02() {

		fail("write tests");
    	
    }
    
}
