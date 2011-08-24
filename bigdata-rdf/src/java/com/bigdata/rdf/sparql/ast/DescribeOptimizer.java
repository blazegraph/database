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

package com.bigdata.rdf.sparql.ast;

import java.util.Collection;
import java.util.LinkedHashSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sail.QueryType;

/**
 * Optimizer to turn a describe query into a construct query.
 */
public class DescribeOptimizer implements IASTOptimizer {

	/**
	 * Change this:
	 * 
	 * describe term1 term2 ...
	 * where {
	 *    whereClause .
	 * }
	 * 
	 * Into this:
	 * 
	 * construct {
	 *   term1 ?p1a ?o1 .
	 *   ?s1   ?p1b term1 .
	 *   term2 ?p2a ?o2 .
	 *   ?s2   ?p2b term2 .
	 * }
	 * where {
	 *   whereClause .
	 *   {
	 *     term1 ?p1a ?o1 .
	 *   } union {
	 *     ?s1   ?p1b term1 .
	 *   } union {
	 *     term2 ?p2a ?o2 .
	 *   } union {
	 *     ?s2   ?p2b term2 .
	 *   }
	 */
	@Override
	public IQueryNode optimize(final AST2BOpContext context, 
			final IQueryNode queryNode, final DatasetNode dataset, 
			final IBindingSet[] bindingSet) {
		
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

		final QueryRoot describe = (QueryRoot) queryNode;
		
		if (describe.getQueryType() != QueryType.DESCRIBE) {
			throw new IllegalArgumentException();
		}
		
		final IGroupNode<IGroupMemberNode> where = describe.getWhereClause();
		
		final UnionNode union = new UnionNode();
		
		where.addChild(union);

		final ConstructNode<StatementPatternNode> construct = 
			new ConstructNode<StatementPatternNode>();
		
		final ProjectionNode projection = describe.getProjection();
		
		describe.setProjection(null);
		describe.setConstruct(construct);
		
		final Collection<TermNode> terms = new LinkedHashSet<TermNode>();
		
		if (projection.isWildcard()) {
		
			/* 
			 * Visit all variable nodes in where clause and add them
			 * into the terms collections.
			 */
			throw new RuntimeException("implement me");
			
		} else {
		
			int i = 0;
			for (AssignmentNode n : projection) {
				
				// can only have variables and constants in describe projections
				terms.add((TermNode) n.getValueExpressionNode());
	
			}
				
		}		
		
		int i = 0;
		
		for (TermNode term : terms) {
			
			final int termNum = i++;
			
			{ // <term> ?pN-a ?oN
			
				final StatementPatternNode sp = new StatementPatternNode(
						term, 
						new VarNode("p"+termNum+"a"),
						new VarNode("o"+termNum)
						); 

				construct.addChild(sp);
				
				union.addChild(sp);
				
			}
				
			
			{ // ?sN ?pN-b <term>
			
				final StatementPatternNode sp = new StatementPatternNode(
					new VarNode("s"+termNum),
					new VarNode("p"+termNum+"b"),
					term
					);

				construct.addChild(sp);
				
				union.addChild(sp);
				
			}
			
		}
		
		return describe;
		
	}
	
}
