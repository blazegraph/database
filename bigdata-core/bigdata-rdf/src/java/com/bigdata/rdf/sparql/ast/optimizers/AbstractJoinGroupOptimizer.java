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

import java.util.Iterator;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * This makes it easier to sit down and write an optimizer that operates on
 * a group.
 */
public abstract class AbstractJoinGroupOptimizer implements IASTOptimizer {

	private final boolean childFirst;
	
	private final boolean optimizeServiceNodes;
	
	public AbstractJoinGroupOptimizer() {
		this(false, false);
	}
	
	protected AbstractJoinGroupOptimizer(final boolean childFirst, final boolean optimizeServiceNodes) {
		this.childFirst = childFirst;
		this.optimizeServiceNodes = optimizeServiceNodes;
	}
	
	/**
	 * Top-level optimize method.  Will locate the relevant top-level 
	 * {@link GraphPatternGroup} nodes (where clause, named subqueries) and 
	 * delegate to the 
	 * {@link #optimize(AST2BOpContext, StaticAnalysis, GraphPatternGroup)} method.
	 */
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

                optimize(context, sa, bindingSets, whereClause);
                
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

                    optimize(context, sa, bindingSets, whereClause);

                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    /**
     * Optimize a particular {@link GraphPatternGroup}.  If the group happens
     * to be a {@link JoinGroupNode}, this method will delegate to the
     * {@link #optimize(AST2BOpContext, StaticAnalysis, JoinGroupNode)}
     * method, which is the method that subclasses should override to do the
     * work of actually optimizing a particular join group.  After optimizing
     * the group, this method will descend into the children and recursively 
     * optimize any child groups as well.
     * <p>
     * I've made this method final, but I could perhaps see cases where
     * subclasses might want to override.  Maybe revisit.  -mp
     */
    private void optimize(final AST2BOpContext ctx, 
    		final StaticAnalysis sa, final IBindingSet[] bSets, 
    		final GraphPatternGroup<?> op) {

    	if (!childFirst) {
    	
	    	if (op instanceof JoinGroupNode) {
	    		
	    		final JoinGroupNode joinGroup = (JoinGroupNode) op;
	
	    		optimizeJoinGroup(ctx, sa, bSets, joinGroup);
	    		
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

                optimize(ctx, sa, bSets, childGroup);
                
            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
                        .getWhereClause();

                optimize(ctx, sa, bSets, childGroup);

            } else if (child instanceof FilterNode) {
            	
            	final FilterNode filter = (FilterNode) child;
            	
            	final IValueExpressionNode ve = filter.getValueExpressionNode();

            	optimize(ctx, sa, bSets, ve);
            	
            } else if (child instanceof ArbitraryLengthPathNode) {
            	
            	final ArbitraryLengthPathNode alpNode = (ArbitraryLengthPathNode) child;
            	
            	final IBindingSet bs = new ListBindingSet();
            	
            	if (alpNode.left().isConstant()) {
            		
            		bs.set(alpNode.tVarLeft().getValueExpression(), 
            				((ConstantNode) alpNode.left()).getValueExpression());
            		
            	}
            	
            	if (alpNode.right().isConstant()) {
            		
            		bs.set(alpNode.tVarRight().getValueExpression(), 
            				((ConstantNode) alpNode.right()).getValueExpression());
            		
            	}
            	
            	/*
            	 * The transitivity vars are unique to the alpNode, so we don't
            	 * need to merge, simple addition will do fine.
            	 */
            	
            	final IBindingSet[] bSets2;
            	
            	if (bSets == null || bSets.length == 0 || 
            			(bSets.length == 1 && bSets[0].isEmpty())) {
            		
            		bSets2 = new IBindingSet[] { bs };
            		
            	} else {
            		
            		bSets2 = new IBindingSet[bSets.length+1];
            		
            		bSets2[0] = bs;
            		
            		System.arraycopy(bSets, 0, bSets2, 1, bSets.length);
            		
            	}
            	
            	optimize(ctx, sa, bSets2, alpNode.subgroup());
            	
            } else if (child instanceof ServiceNode && optimizeServiceNodes) {
            	
            	final ServiceNode serviceNode = (ServiceNode) child;

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) serviceNode
                		.getGraphPattern();

                optimize(ctx, sa, bSets, childGroup);
            	
            } else if (child instanceof AssignmentNode) {
                // This optimize execution is needed to process expression value,
                // for example, property paths inside EXISTS
                // Ref: Test_Ticket_T173243
                final AssignmentNode assignmentNode = (AssignmentNode) child;

                final IValueExpressionNode valueExpressionNode = assignmentNode.getValueExpressionNode();

                optimize(ctx, sa, bSets, valueExpressionNode);

            }
            
        }
        
    	if (childFirst) {
        	
	    	if (op instanceof JoinGroupNode) {
	    		
	    		final JoinGroupNode joinGroup = (JoinGroupNode) op;
	
	    		optimizeJoinGroup(ctx, sa, bSets, joinGroup);
	    		
	    	}
    	
    	}

    }

	private void optimize(final AST2BOpContext ctx, final StaticAnalysis sa,
			final IBindingSet[] bSets, final IValueExpressionNode ve) {
		if (ve instanceof SubqueryFunctionNodeBase) {

		    final SubqueryFunctionNodeBase subqueryFunction = (SubqueryFunctionNodeBase) ve;

		    final GraphPatternGroup<IGroupMemberNode> graphPattern = subqueryFunction
		            .getGraphPattern();

		    if (graphPattern != null) {

		    	optimize(ctx, sa, bSets, graphPattern);
		    	
		    }
		    
		} else {
			Iterator<BOp> it = ((BOp)ve).argIterator();
			while (it.hasNext()) {
				
				BOp b = it.next();
				if (b instanceof IValueExpressionNode) {
					
					optimize(ctx, sa, bSets, (IValueExpressionNode)b);
					
				}
			}
		}
	}
    
    /**
     * Subclasses can do the work of optimizing a join group here.
     */
    protected abstract void optimizeJoinGroup(
    		final AST2BOpContext ctx, final StaticAnalysis sa, 
    		final IBindingSet[] bSets, final JoinGroupNode op);
    
}
