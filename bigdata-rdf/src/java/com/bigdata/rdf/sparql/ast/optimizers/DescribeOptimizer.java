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

import java.util.Collection;
import java.util.LinkedHashSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Optimizer to turn a describe query into a construct query.
 */
public class DescribeOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger.getLogger(DescribeOptimizer.class); 
    
	/**
	 * Change this:
	 * <pre>
	 * describe term1 term2 ...
	 * where {
	 *    whereClause ...
	 * }
	 * </pre>
	 * Into this:
	 * <pre>
	 * construct {
	 *   term1 ?p1a ?o1 .
	 *   ?s1   ?p1b term1 .
	 *   term2 ?p2a ?o2 .
	 *   ?s2   ?p2b term2 .
	 * }
	 * where {
	 *   whereClause ...
	 *   {
	 *     term1 ?p1a ?o1 .
	 *   } union {
	 *     ?s1   ?p1b term1 .
	 *   } union {
	 *     term2 ?p2a ?o2 .
	 *   } union {
	 *     ?s2   ?p2b term2 .
	 *   }
	 * </pre>
	 */
	@SuppressWarnings("unchecked")
    @Override
	public IQueryNode optimize(final AST2BOpContext context, 
			final IQueryNode queryNode, //final DatasetNode dataset, 
			final IBindingSet[] bindingSet) {
		
		final QueryRoot queryRoot = (QueryRoot) queryNode;
		
		if (queryRoot.getQueryType() != QueryType.DESCRIBE) {
			
		    // Not a query that we will rewrite.
		    return queryRoot;
		    
		}

        // Change the query type. This helps prevent multiple rewrites.
		queryRoot.setQueryType(QueryType.CONSTRUCT);
		
		final GraphPatternGroup<IGroupMemberNode> where;
		
		if (queryRoot.hasWhereClause()) {
			
		    // start with the existing WHERE clause.
			where = queryRoot.getWhereClause();
			
		} else {
			
			// some describe queries don't have where clauses
			queryRoot.setWhereClause(where = new JoinGroupNode());
			
		}
		
		final UnionNode union = new UnionNode();
		
		where.addChild(union); // append UNION to WHERE clause.

        final ConstructNode construct = new ConstructNode();

		final ProjectionNode projection = queryRoot.getProjection();
		
		if(projection == null) {

            throw new RuntimeException("No projection?");
		    
		}
		
		queryRoot.setProjection(null); // remove the projection.
		queryRoot.setConstruct(construct); // add CONSTRUCT node.
		
		final Collection<TermNode> terms = new LinkedHashSet<TermNode>();

		/*
		 * The projection node should have been rewritten first.
		 */
        if (projection.isWildcard())
            throw new AssertionError("Wildcard projection was not rewritten.");
//		if (projection.isWildcard()) {
//		
//			/* 
//			 * Visit all variable nodes in where clause and add them
//			 * into the terms collections.
//			 */
//			final Iterator<IVariable<?>> it = 
//				BOpUtility.getSpannedVariables((BOp) where);
//			
//			while (it.hasNext()) {
//				
//				final IVariable<IV> v = (IVariable<IV>) it.next();
//				
//				final VarNode var = new VarNode(v);
//				
//				terms.add(var);
//				
//			}
//			
//            if (terms.isEmpty())
//                throw new RuntimeException(
//                        "DESCRIBE *, but no variables in this query");
//
//		} else {

        if (projection.isEmpty())
            throw new RuntimeException(
                    "DESCRIBE, but no variables are projected.");
        
			for (AssignmentNode n : projection) {
				
				// can only have variables and constants in describe projections
				terms.add((TermNode) n.getValueExpressionNode());
	
			}
				
//		}		
		
		int i = 0;
		
		for (TermNode term : terms) {
			
			final int termNum = i++;
			
			{ // <term> ?pN-a ?oN
			
                /*
                 * Note: Each statement has to be in a different part of the
                 * UNION. Also, note that we do not allow a bare statement
                 * pattern in a union. The statement pattern has to be embedded
                 * within a group.
                 */
				final StatementPatternNode sp = new StatementPatternNode(
						term, 
						new VarNode("p"+termNum+"a"),
						new VarNode("o"+termNum)
						);

				construct.addChild(sp);
				
				final JoinGroupNode group = new JoinGroupNode();
				group.addChild(sp);
                union.addChild(group);
				
//				union.addChild(sp);
				
			}
				
			
			{ // ?sN ?pN-b <term>
			
				final StatementPatternNode sp = new StatementPatternNode(
					new VarNode("s"+termNum),
					new VarNode("p"+termNum+"b"),
					term
					);

				construct.addChild(sp);

                final JoinGroupNode group = new JoinGroupNode();
                group.addChild(sp);
                union.addChild(group);

//				union.addChild(sp);
				
			}
			
		}
		
		return queryRoot;
		
	}
	
}
