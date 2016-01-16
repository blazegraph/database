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
 * Created on May 3, 2014
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;

/**
 * Test suite for {@link ASTUnionFiltersOptimizer}.
 * 
 * @author Jeremy Carroll
 */
public class TestASTExistsAndJoinOrderByTypeOptimizers extends AbstractOptimizerTestCase {

    /**
     * 
     */
    public TestASTExistsAndJoinOrderByTypeOptimizers() {
    }

    /**
     * @param name
     */
    public TestASTExistsAndJoinOrderByTypeOptimizers(String name) {
        super(name);
    }
	@Override
	IASTOptimizer newOptimizer() {
		return new ASTOptimizerList(new ASTExistsOptimizer(), 
				new ASTJoinOrderByTypeOptimizer());
	}
	
	public void testSimpleExists() {
		new Helper(){{
			given = select( varNode(w), 
					where ( joinGroupNode(
							    filter(
							    	exists(varNode(y), joinGroupNode(
							    			statementPatternNode(constantNode(a),constantNode(b),varNode(w))))
							    )
							) ) );
			
			final SubqueryRoot askQuery;
			expected = select( varNode(w), 
					where (joinGroupNode(
								askQuery=ask(varNode(y),
										joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(b),varNode(w))
											 ) ),
								filter(exists(varNode(y), joinGroupNode(
						    			statementPatternNode(constantNode(a),constantNode(b),varNode(w))))
						    ) )
							) );
			askQuery.setFilterExistsMode(QueryHints.DEFAULT_FILTER_EXISTS);
		}}.test();
		
	}
	public void testOrExists() {
		new Helper(){{
			given = select( varNode(w), 
					where ( joinGroupNode(
							    filter(
							    	or (
							    	    exists(varNode(y), joinGroupNode(
							    			statementPatternNode(constantNode(a),constantNode(b),varNode(w)))),
								    	exists(varNode(z), joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(c),varNode(w)))))
							    )
							) ) );
			
			final SubqueryRoot askQuery1, askQuery2;
			expected = select( varNode(w), 
					where (joinGroupNode(
								askQuery1=ask(varNode(y),
										joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(b),varNode(w))
											 ) ),
								askQuery2=ask(varNode(z),
										joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(c),varNode(w))
											) ),
							    filter(
								    	or (
								    	    exists(varNode(y), joinGroupNode(
								    			statementPatternNode(constantNode(a),constantNode(b),varNode(w)))),
									    	exists(varNode(z), joinGroupNode(
												statementPatternNode(constantNode(a),constantNode(c),varNode(w)))))
								    )
						    ) )
							);
            askQuery1.setFilterExistsMode(QueryHints.DEFAULT_FILTER_EXISTS);
            askQuery2.setFilterExistsMode(QueryHints.DEFAULT_FILTER_EXISTS);
		}}.test();
		
	}
	public void testOrWithPropertyPath() {
		new Helper(){{
			given = select( varNode(w), 
					where ( joinGroupNode(
							    filter(
							    	or (
							    	    exists(varNode(y), joinGroupNode(
							    			arbitartyLengthPropertyPath(varNode(w), constantNode(b), HelperFlag.ONE_OR_MORE,
													joinGroupNode( statementPatternNode(leftVar(), constantNode(b),  rightVar()) ) )

							    			)),
								    	exists(varNode(z), joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(c),varNode(w)))))
							    )
							) ) );

    		varCount = 0;
			final ArbitraryLengthPathNode alpp1 = arbitartyLengthPropertyPath(varNode(w), constantNode(b), HelperFlag.ONE_OR_MORE,
					joinGroupNode( statementPatternNode(leftVar(), constantNode(b),  rightVar()) ) );
    		varCount = 0;
			final ArbitraryLengthPathNode alpp2 = arbitartyLengthPropertyPath(varNode(w), constantNode(b), HelperFlag.ONE_OR_MORE,
					joinGroupNode( statementPatternNode(leftVar(), constantNode(b),  rightVar()) ) );
            final SubqueryRoot askQuery1, askQuery2;
			expected = select( varNode(w), 
					where (joinGroupNode(
					            askQuery1=ask(varNode(y),
										joinGroupNode(
							    			alpp1
											 ) ),
								askQuery2=ask(varNode(z),
										joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(c),varNode(w))
											) ),
							    filter(
								    	or (
								    	    exists(varNode(y), joinGroupNode(
									    			alpp2

									    			)),
									    	exists(varNode(z), joinGroupNode(
												statementPatternNode(constantNode(a),constantNode(c),varNode(w)))))
								    )
						    ) )
							);
            askQuery1.setFilterExistsMode(QueryHints.DEFAULT_FILTER_EXISTS);
            askQuery2.setFilterExistsMode(QueryHints.DEFAULT_FILTER_EXISTS);

		}}.test();
	}

}
