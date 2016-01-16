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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.ZERO_OR_MORE;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTUnionFiltersOptimizer}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id: TestASTEmptyGroupOptimizer.java 5302 2011-10-07 14:28:03Z
 *          thompsonbry $
 */
public class TestASTPropertyPathOptimizer extends AbstractOptimizerTestCase {

    /**
     * 
     */
    public TestASTPropertyPathOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTPropertyPathOptimizer(String name) {
        super(name);
    }

  
	@Override
	IASTOptimizer newOptimizer() {
		return new ASTPropertyPathOptimizerInTest();
	}
	
	/**
	 * This is (nearly) the same as {@link TestALPPinTrac773#testSimpleALPP()
	 */
	public void test_basic_star() {
		new Helper(){{

    		given = select( varNode(x), 
    				where (
    						joinGroupNode( 
    								propertyPathNode(varNode(x),"c*", constantNode(b))
    							)
    				) );
    		
    		
    		expected = select( varNode(x), 
    				where (
    					joinGroupNode( 
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar())
    												) ) )
    				) );
		}}.test();
	}

	public void test_filter_star() {
		new Helper(){{

    		given = select( varNode(x), 
    				where (
					  filter(
						exists(varNode(y),
    						joinGroupNode( 
    								propertyPathNode(varNode(x),"c*", constantNode(b))
    							) ) )
    				) );
    		
    		expected = select( varNode(x), 
    				where (
    				  filter(
    					exists(varNode(y),
    					  joinGroupNode( 
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar())
    												) ) ) ) )
    				) );
		}}.test();
	}
	public void test_filter_or_star() {
		new Helper(){{

    		given = select( varNode(x), 
    				where (
					  filter(
						    	or (
						    			constantNode(a),
						exists(varNode(y),
    						joinGroupNode( 
    								propertyPathNode(varNode(x),"c*", constantNode(b))
    							) ) )
    							)
    				) );
    		
    		expected = select( varNode(x), 
    				where (
    				  filter(
						    	or (
						    			constantNode(a),
    					exists(varNode(y),
    					  joinGroupNode( 
    						arbitartyLengthPropertyPath(varNode(x), constantNode(b), ZERO_OR_MORE,
    										joinGroupNode( 
    												statementPatternNode(leftVar(), constantNode(c),  rightVar())
    												) ) ) ) ) )
    				) );
		}}.test();
	}

}
