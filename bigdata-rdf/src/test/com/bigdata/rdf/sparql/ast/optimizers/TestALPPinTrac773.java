/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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

package com.bigdata.rdf.sparql.ast.optimizers;

import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.*;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Trac733 shows some strange behavior, this test case is intended
 * to explore that.
 * 
 * The basic issues concerns the order of execution of arbitrary length propery
 * paths and other bits of the query. The observed behavior was that
 * adding additional braces, changing the grouping had surprising effects,
 * and that the default choice of order was often poor.
 * 
 * With this first commit we capture the incorrect behavior.
 *
 */
public class TestALPPinTrac773 extends AbstractOptimizerTestCase {

	private class NotNestedHelper extends Helper {
		public NotNestedHelper(HelperFlag zero_or_one_to_one_or_more, String sym) {
			String pattern = "c" + sym;

    		given = select( varNode(z), 
    				where (

    						joinGroupNode(propertyPathNode(varNode(x),pattern, constantNode(b))),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						propertyPathNode(varNode(x),pattern, varNode(z)),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    		varCount = 0;
    		// we have to evaluate this one earlier in order to get the anonymous variable numbering
    		// lined up. Really we should compare the result with expected wise to
    		// the unimportance of the name of anonymous variables.
    		ArbitraryLengthPathNode alpp = arbitartyLengthPropertyPath(varNode(x), varNode(z), zero_or_one_to_one_or_more,
					joinGroupNode( 
							statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
							) );
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), zero_or_one_to_one_or_more,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    					    alpp,
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431)
    				) );
    		varCount = 0;
    		
    	}
	}
	private class NestedHelper extends Helper {
		
		public NestedHelper(HelperFlag zero_or_one_to_one_or_more, String sym) {
			String pattern = "c" + sym;

    		given = select( varNode(z), 
    				where (
    						joinGroupNode(propertyPathNode(varNode(x),pattern, constantNode(b))),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						joinGroupNode(propertyPathNode(varNode(x),pattern, varNode(z))),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    		varCount = 0;
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), zero_or_one_to_one_or_more,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    						arbitartyLengthPropertyPath(varNode(x), varNode(z), zero_or_one_to_one_or_more,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
    												) ),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431)
    				) );
    		varCount = 0;
    		
    	}
	}
	public TestALPPinTrac773() {
	}

	public TestALPPinTrac773(String name) {
		super(name);
	}
	@Override
	IASTOptimizer newOptimizer() {
		return new ASTOptimizerList(
				new ASTPropertyPathOptimizer() {
					private int counter = 0;

					@Override
				    protected VarNode anonVar(final String anon) {
				        VarNode v = new VarNode(anon+counter++);
				        v.setAnonymous(true);
				        return v;
				    }
				},
				new ASTRangeCountOptimizer(){
					@Override


					protected void estimateCardinalities(StatementPatternNode sp, final IV<?, ?> s, final IV<?, ?> p,
							final IV<?, ?> o, final IV<?, ?> c, final AbstractTripleStore db) {
						if (o != null)
						    sp.setProperty(Annotations.ESTIMATED_CARDINALITY, 26l);
						else 
							sp.setProperty(Annotations.ESTIMATED_CARDINALITY, 3135l);
					}
					
				},
				new ASTFlattenJoinGroupsOptimizer(), 
				new ASTStaticJoinOptimizer());
	}
	

	public void testSimpleALPP() {

    	new Helper(){{

    		given = select( varNode(x), 
    				where (
    						joinGroupNode( 
    								propertyPathNode(varNode(x),"c*", constantNode(b))
    							)
    				) );
    		
    		
    		expected = select( varNode(x), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) )
    				) );
    		
    	}}.test();
	}
	public void testNestedPartway() {

    	new Helper(){{

    		given = select( varNode(z), 
    				where (
    						joinGroupNode( 
    								arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) )
    										
    										),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						joinGroupNode( 
    								arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
    												) )
    										
    										),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    		varCount = 0;
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
    												) ),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    	}}.test();
	}
	public void testNotNestedPartway() {

    	new Helper(){{

    		given = select( varNode(z), 
    				where (
    						joinGroupNode( 
    								arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) )
    										
    										),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
    												) ),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    		varCount = 0;
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
    												) ),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    	}}.test();
	}
	public void testNestedStar() {

    	new NestedHelper(ZERO_OR_MORE,"*"){{
    		// currently not correctly optimized.
    		// TODO: this expected result is incorrect.
    		
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
    												) ),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    	}}.test();
	}
	public void testNotNestedStar() {
    	new NotNestedHelper(ZERO_OR_MORE,"*"){{
    		// currently not correctly optimized.
    		// TODO: this expected result is incorrect.

    		ArbitraryLengthPathNode alpp = arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_MORE,
					joinGroupNode( 
							statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
							) );
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054),
    			    		alpp
    				) );
    		
    	}}.test();
	}
	public void testNestedPlus() {

    	new NestedHelper(ONE_OR_MORE,"+").test();
	}
	public void testNotNestedPlus() {

    	new NotNestedHelper(ONE_OR_MORE,"+").test();
	}
	public void testNestedQuestionMark() {

    	new NestedHelper(ZERO_OR_ONE,"?"){{
    		// currently not correctly optimized.
    		// TODO: this expected result is incorrect.
    		
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_ONE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_ONE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
    												) ),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    	}}.test();
	}
	public void testNotNestedQuestionMark() {

    	new NotNestedHelper(ZERO_OR_ONE,"?"){{
    		// currently not correctly optimized.
    		// TODO: this expected result is incorrect.

    		ArbitraryLengthPathNode alpp = arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_ONE,
					joinGroupNode( 
							statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135)
							) );
    		expected = select( varNode(z), 
    				where (
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_ONE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26)
    												) ),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054),
    			    		alpp
    				) );
    		
    	}}.test();
	}
}
