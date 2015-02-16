/**

Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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

/**
 * Trac 794 concerns interactions between BIND and SERVICE going to a remote
 * SPARQL end-point. The service call must be done last.
 */
package com.bigdata.rdf.sparql.ast.optimizers;




public class TestASTMassagedServiceNodeOptimizer extends AbstractOptimizerTestCase {

	public TestASTMassagedServiceNodeOptimizer(String name) {
		super(name);
	}

	public TestASTMassagedServiceNodeOptimizer() {
	}
	@Override
	IASTOptimizer newOptimizer() {
		return new ASTJoinOrderByTypeOptimizer();
	}

	public void testLeaveBindBeforeService() {

    	new Helper(){{

    		given = select( varNode(z), 
    				where (
    						joinGroupNode( 
    						    statementPatternNode(varNode(x), constantNode(c),  constantNode(d)),
    						    bind(functionNode("eg:foo", varNode(x)), varNode(y) ),
    						    service( constantNode(a), 
                                    joinGroupNode( statementPatternNode(varNode(z), constantNode(f), varNode(y)) ) )
                            )
    				) );
    		
    		
    		expected = select( varNode(z), 
    				where (
    						joinGroupNode( 
    						    statementPatternNode(varNode(x), constantNode(c),  constantNode(d)),
    						    bind(functionNode("eg:foo", varNode(x)), varNode(y) ),
    						    service( constantNode(a), 
                                    joinGroupNode( statementPatternNode(varNode(z), constantNode(f), varNode(y)) ) )
                            )
    				) );

    		
    	}}.test();
	}
	public void testPutBindBeforeService() {

    	new Helper(){{

    		given = select( varNode(z), 
    				where (
    						joinGroupNode( 
        						    service( constantNode(a), 
                                            joinGroupNode( statementPatternNode(varNode(z), constantNode(f), varNode(y)) ) ),
    						    statementPatternNode(varNode(x), constantNode(c),  constantNode(d)),
    						    bind(functionNode("eg:foo", varNode(x)), varNode(y) )
                            )
    				) );
    		
    		
    		expected = select( varNode(z), 
    				where (
    						joinGroupNode( 
    						    statementPatternNode(varNode(x), constantNode(c),  constantNode(d)),
    						    bind(functionNode("eg:foo", varNode(x)), varNode(y) ),
    						    service( constantNode(a), 
                                    joinGroupNode( statementPatternNode(varNode(z), constantNode(f), varNode(y)) ) )
                            )
    				) );

    		
    	}}.test();
	}

}
