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

package com.bigdata.rdf.sparql.ast.optimizers;

import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.ONE_OR_MORE;
import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.ZERO_OR_MORE;
import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.ZERO_OR_ONE;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.GroupMemberNodeBase;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

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
			this(zero_or_one_to_one_or_more, sym, true);
		}
		public NotNestedHelper(HelperFlag zero_or_one_to_one_or_more, String sym, boolean switchOrdering) {
			String pattern = "c" + sym;

			StatementPatternNode spn1 = statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431);
    		StatementPatternNode spn2 = statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054);
			given = select( varNode(z), 
    				where (

    						joinGroupNode(propertyPathNode(varNode(x),pattern, constantNode(b))),
    						spn1,
    						propertyPathNode(varNode(x),pattern, varNode(z)),
    			    		spn2
    				) );
    		
    		varCount = 0;
    		// we have to evaluate this one earlier in order to get the anonymous variable numbering
    		// lined up. Really we should compare the result with expected wise to
    		// the unimportance of the name of anonymous variables.
    		ArbitraryLengthPathNode alpp1;
    		ArbitraryLengthPathNode alpp2;
    		if (switchOrdering) {
    			alpp2 = alpp2(zero_or_one_to_one_or_more);
    			alpp1 = alpp1(zero_or_one_to_one_or_more);
    		} else {
    			alpp1 = alpp1(zero_or_one_to_one_or_more);
    			alpp2 = alpp2(zero_or_one_to_one_or_more);
    		}
    		
    		/**
    		 * Adjusted order due to changes made in history branch. The 
    		 * computation of ArbitraryLengthPathNode.getEstimatedCardinality()
    		 * has changed, causing triple patterns to have precedence over
    		 * unbounded path expressions. See
    		 * https://jira.blazegraph.com/browse/BLZG-858 / trac #733.
    		 */
         final GroupMemberNodeBase<?> gmn[] = (sym.equals("?")) ?
            new GroupMemberNodeBase[]{alpp1, spn1, alpp2, spn2} :
            new GroupMemberNodeBase[]{spn2, alpp2, spn1, alpp1 };
         
			expected = select( varNode(z),  where ( gmn ) );
    		varCount = 0;
    		
    	}
		ArbitraryLengthPathNode alpp1(HelperFlag zero_or_one_to_one_or_more) {
			return arbitartyLengthPropertyPath(varNode(x), constantNode(b), zero_or_one_to_one_or_more,
							joinGroupNode( statementPatternNode(leftVar(), constantNode(c),  rightVar(), 26) ) );
		}
		ArbitraryLengthPathNode alpp2(HelperFlag zero_or_one_to_one_or_more) {
			return arbitartyLengthPropertyPath(varNode(x), varNode(z), zero_or_one_to_one_or_more,
					joinGroupNode( statementPatternNode(leftVar(), constantNode(c),  rightVar(), 3135) ) );
		}
	}
	private class NestedHelper extends Helper {
		
		public NestedHelper(HelperFlag zero_or_one_to_one_or_more, String sym) {
			String pattern = "d" + sym;

			StatementPatternNode spn1 = statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431);
    		StatementPatternNode spn2 = statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054);
			given = select( varNode(z), 
    				where (
    						joinGroupNode(propertyPathNode(varNode(x),pattern, constantNode(b))),
    						spn1,
    						joinGroupNode(propertyPathNode(varNode(x),pattern, varNode(z))),
    			    		spn2
    				) );
    		
    		varCount = 0;
    		ArbitraryLengthPathNode alpp1 = arbitartyLengthPropertyPath(varNode(x), constantNode(b), zero_or_one_to_one_or_more,
							joinGroupNode( 
									statementPatternNode(leftVar(), constantNode(d),  rightVar(), 26)
									) );
			ArbitraryLengthPathNode alpp2 = arbitartyLengthPropertyPath(varNode(x), varNode(z), zero_or_one_to_one_or_more,
							joinGroupNode( 
									statementPatternNode(leftVar(), constantNode(d),  rightVar(), 3135)
									) );

         /**
          * Adjusted order due to changes made in history branch. The 
          * computation of ArbitraryLengthPathNode.getEstimatedCardinality()
          * has changed, causing triple patterns to have precedence over
          * unbounded path expressions. See
          * https://jira.blazegraph.com/browse/BLZG-858 / trac #733.
          */
         final GroupMemberNodeBase<?> gmn[] = (sym.equals("?")) ?
               new GroupMemberNodeBase[]{alpp1, spn1, alpp2, spn2} :
               new GroupMemberNodeBase[]{spn2, alpp2, spn1, alpp1 };
			
			expected = select( varNode(z), where ( gmn ) );
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
				new ASTPropertyPathOptimizerInTest(),
				new ASTRangeCountOptimizer(){
					@Override


					protected void estimateCardinalities(StatementPatternNode sp, final IV<?, ?> s, final IV<?, ?> p,
							final IV<?, ?> o, final IV<?, ?> c, final AST2BOpContext ctx) {
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

    	new NestedHelper(ZERO_OR_MORE,"*"){{

    		given = select( varNode(z), 
    				where (
    						joinGroupNode( 
    								arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(d),  rightVar(), 26)
    												) )
    										
    										),
    						statementPatternNode(varNode(y), constantNode(c),  varNode(x), 15431),
    						joinGroupNode( 
    								arbitartyLengthPropertyPath(varNode(x), varNode(z), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(d),  rightVar(), 3135)
    												) )
    										
    										),
    			    		statementPatternNode(varNode(z), constantNode(a),  varNode(w), 2054)
    				) );
    		
    		
    	}}.test();
	}
	public void testNotNestedPartway() {

		new NotNestedHelper(ZERO_OR_MORE,"*", false){{

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
    		
    	}}.test();
	}
	public void testNestedStar() {

    	new NestedHelper(ZERO_OR_MORE,"*").test();
	}
	public void testNotNestedStar() {
    	new NotNestedHelper(ZERO_OR_MORE,"*").test();
	}
	public void testNestedPlus() {

    	new NestedHelper(ONE_OR_MORE,"+").test();
	}
	public void testNotNestedPlus() {

    	new NotNestedHelper(ONE_OR_MORE,"+").test();
	}
	public void testNestedQuestionMark() {

    	new NestedHelper(ZERO_OR_ONE,"?").test();
	}
	public void testNotNestedQuestionMark() {

    	new NotNestedHelper(ZERO_OR_ONE,"?").test();
	}
}
