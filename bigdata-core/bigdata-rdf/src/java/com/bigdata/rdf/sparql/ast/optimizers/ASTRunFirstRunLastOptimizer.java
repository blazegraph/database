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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;

/**
 * This optimizer simply puts each type of {@link IGroupMemberNode} within a
 * {@link JoinGroupNode} in the right order w.r.t. to the other types.
 * <p>
 * Basically the ASTRunFirstRunLastOptimizer will look for IJoinNodes that have
 * a query hint of QueryHints.RUN_FIRST=true or RUN_LAST=true. If it finds more
 * than one "run first" or "run last" it will throw an exception. If it finds an
 * optional marked as "run first" it will throw an exception. It will then scan
 * the group and identify the first and last indices for IJoinNodes, and place
 * the run first and run last IJoinNodes at those indices. The static optimizer
 * will also look for a "run first" IJoinNode in a group and make sure that it
 * gets run first in the group (of the statement patterns). 
 */
public class ASTRunFirstRunLastOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger
//            .getLogger(ASTRunFirstRunLastOptimizer.class);

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

        // Main WHERE clause
        {

            @SuppressWarnings("unchecked")
			final GraphPatternGroup<IGroupMemberNode> whereClause = 
            	(GraphPatternGroup<IGroupMemberNode>) queryRoot.getWhereClause();

            if (whereClause != null) {

                optimize(context, sa, whereClause);
                
            }

        }

        // Named subqueries
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                @SuppressWarnings("unchecked")
				final GraphPatternGroup<IGroupMemberNode> whereClause = 
                	(GraphPatternGroup<IGroupMemberNode>) namedSubquery.getWhereClause();

                if (whereClause != null) {

                    optimize(context, sa, whereClause);

                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

	/**
	 * 1. Look for multiple run first or run last joins and throw an exception.
	 * <p>
	 * 2. Find the run first join if it exists. Make sure it is not optional.
	 * Put it first.
	 * <p> 
	 * 3. Find the run last optimizer if it exists. Put it last.
	 */
    private void optimize(final IEvaluationContext ctx, final StaticAnalysis sa,
    		final GraphPatternGroup<?> op) {

    	if (op instanceof JoinGroupNode) {
    		
    		final JoinGroupNode joinGroup = (JoinGroupNode) op;
    	
    		IGroupMemberNode first = null;
    		IGroupMemberNode last = null;
    		
            for (IGroupMemberNode child : joinGroup) {
            
            	if (child instanceof IBindingProducerNode) {
            		
            		final ASTBase join = (ASTBase) child;
            		
            		if (join.getProperty(QueryHints.RUN_FIRST, false)) {
            			
            			if (first != null) {
            				
            				throw new RuntimeException(
            						"there can be only one \"run first\" join in any group");
            				
            			}
            			
            			if (((IJoinNode) join).isOptional()) {
            				
            				throw new RuntimeException(
            						"\"run first\" cannot be attached to optional joins");
            				
            			}
            			
            			first = child;
            			
            		}
            		
            		if (join.getProperty(QueryHints.RUN_LAST, false)) {
            			
            			if (last != null) {
            				
            				throw new RuntimeException(
            						"there can be only one \"run last\" join in any group");
            				
            			}
            			
            			last = child;
            			
            		}
            		
            	}
            	
            }
        
            if (first != null) {
            	
                int firstJoinIndex = 0;
                for (int i = 0; i < joinGroup.arity(); i++) {
                	if (joinGroup.get(i) instanceof IBindingProducerNode) {
                		firstJoinIndex = i;
                		break;
                	}
                }
                
		        joinGroup.removeChild(first);
		        
		        joinGroup.addArg(firstJoinIndex, (BOp) first);
		        
            }
            
            if (last != null) {
            	
                int lastJoinIndex = 0;
                for (int i = joinGroup.size()-1; i >= 0; i--) {
                	if (joinGroup.get(i) instanceof IBindingProducerNode) {
                		lastJoinIndex = i;
                		break;
                	}
                }
                
		        joinGroup.removeChild(last);
		        
		        joinGroup.addArg(lastJoinIndex, (BOp) last);
            	
            }
    		
    	}
    	
        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GraphPatternGroup<?>) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                optimize(ctx, sa, childGroup);
                
            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
                        .getWhereClause();

                optimize(ctx, sa, childGroup);

            }
            
        }

    }
    
}
