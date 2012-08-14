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
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.cache.IDescribeCache;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * This optimizer rewrites the projection node of a DESCRIBE query into,
 * generating a CONSTRUCT clause and extending the WHERE clause to capture the
 * semantics of the DESCRIBE query. The query type is also changed to CONSTRUCT.
 * <p>
 * For example, the optimizer changes this:
 * 
 * <pre>
 * describe term1 term2 ...
 * where {
 *    whereClause ...
 * }
 * </pre>
 * 
 * Into this:
 * 
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
 * <p>
 * Note: The {@link ASTConstructOptimizer} will add a {@link ProjectionNode}
 * based on the generated CONSTRUCT template.
 * 
 * TODO CBD.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578"> Concise
 *      Bounded Description </a>
 */
public class ASTDescribeOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger.getLogger(DescribeOptimizer.class); 
    
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

        final ConstructNode construct = new ConstructNode(context);

        final ProjectionNode projection = queryRoot.getProjection();

        if (projection == null) {

            throw new RuntimeException("No projection?");

        }

        final IDescribeCache describeCache = context.getDescribeCache();
        
        if (describeCache != null) {

            /*
             * We need to keep the projection so we can correlate the original
             * variables for the resources that are being described with the
             * bindings on those variables in order to figure out what resources
             * were described when we are maintaining a DESCRIBE cache.
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
             * DESCRIBE CACHE </a>
             */

            projection.setReduced(true);

        } else {
         
            // remove the projection.
            queryRoot.setProjection(null);
            
        }

        queryRoot.setConstruct(construct); // add CONSTRUCT node.

        final Collection<TermNode> terms = new LinkedHashSet<TermNode>();

        if (projection.isWildcard()) {
         
            /*
             * The projection node should have been rewritten first.
             */
            
            throw new AssertionError("Wildcard projection was not rewritten.");
            
        }

        if (projection.isEmpty()) {

            throw new RuntimeException(
                    "DESCRIBE, but no variables are projected.");

        }

        /*
         * Note: A DESCRIBE may have only variables and constants in the
         * projection. Generalized value expressions are not allowed.
         */

        for (AssignmentNode n : projection) {

            terms.add((TermNode) n.getValueExpressionNode());

        }

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
///*
// * If the DESCRIBE cache is to be maintained, then we must also add
// * in the original DESCRIBE variable. We need to project them in
// * order to recognize which resources are being described.
// * 
// * FIXME Unit test to verify this code path.
// * 
// * TODO Cast to interface is not compatible with remote or
// * distributed cache.
// */
//final DescribeCache describeCache = ((SparqlCache) context.sparqlCache)
//        .getDescribeCache(context.getAbstractTripleStore());
//
//if (describeCache != null) {
//
//    /*
//     * The set of variables that were in the original DESCRIBE
//     * projection. This can include both variables explicitly given
//     * in the query (DESCRIBE ?foo WHERE {...}) and variables bound
//     * to constants by an AssignmentNode (DESCRIBE uri).
//     */
//    
//    final Set<IVariable<?>> describeVars = context.astContainer
//            .getOriginalAST().getProjectedVars(
//                    new LinkedHashSet<IVariable<?>>());
//
//    for (IVariable<?> var : describeVars) {
//
//        // Add into the new projection node.
//        projection.addProjectionVar(new VarNode(var.getName()));
//
//    }
//
//}
