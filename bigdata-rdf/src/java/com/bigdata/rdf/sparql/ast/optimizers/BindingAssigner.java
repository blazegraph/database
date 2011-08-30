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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.IASTOptimizer;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Assigns values to variables based on a supplied set of bindings.
 * <p>
 * Based on {@link org.openrdf.query.algebra.evaluation.impl.BindingAssigner}.
 */
public class BindingAssigner implements IASTOptimizer {

	@SuppressWarnings("unchecked")
	@Override
	public IQueryNode optimize(final AST2BOpContext context, 
			final IQueryNode queryNode, final DatasetNode dataset, 
			final IBindingSet[] bindingSets) {
		
//        final String sparql = "describe <http://www.bigdata.com>";
//
//        final QueryRoot expected = new QueryRoot(QueryType.DESCRIBE);
//        {
//
//            final ProjectionNode projection = new ProjectionNode();
//            expected.setProjection(projection);
//            
//            final VarNode anonvar = new VarNode("-iri-1");
//            anonvar.setAnonymous(true);
//            projection.addProjectionExpression(new AssignmentNode(anonvar,
//                    new ConstantNode(makeIV(valueFactory
//                            .createURI("http://www.bigdata.com")))));
//
//        }

//        final String sparql = "construct { ?s ?p ?o } where {?s ?p ?o}";
//
//        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
//        {
//
//            final ConstructNode construct = new ConstructNode();
//            expected.setConstruct(construct);
//            construct.addChild(new StatementPatternNode(new VarNode("s"),
//                    new VarNode("p"), new VarNode("o"), null/* c */,
//                    Scope.DEFAULT_CONTEXTS));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
//                    new VarNode("p"), new VarNode("o"), null/* c */,
//                    Scope.DEFAULT_CONTEXTS));
//        }

		// only works when there is exactly one incoming binding set
		if (bindingSets == null || bindingSets.length != 1) {
			
			return queryNode;
			
		}
		
		final IBindingSet bs = bindingSets[0];
		
		final QueryRoot root = (QueryRoot) queryNode;
		
		if (!root.hasWhereClause()) {
			
			return root;
			
		}
		
		final IGroupNode<IGroupMemberNode> where = root.getWhereClause();
		
		/* 
		 * Visit all variable nodes and set their value if it is available
		 * in the supplied binding set.
		 */
		final Iterator<IVariable<?>> it = 
			BOpUtility.getSpannedVariables((BOp) where);
		
		while (it.hasNext()) {
			
			final IVariable<IV> v = (IVariable<IV>) it.next();
			
			final IConstant<IV> val = bs.get(v);
			
			if (val != null) {
				
//				v.setValue(val);
				
			}
			
		}
		
		return queryNode;
			
	}
	
}
