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

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Pruning rules for unknown IVs in statement patterns:
 * 
 * If an optional join is known to fail, then remove the optional group in which
 * it appears from the group (which could be an optional group, a join group, or
 * a union).
 * 
 * If a statement pattern contains an unknown term, a with this statement
 * pattern will certainly fail. Thus the group in which the statement pattern
 * appears (the parent) will also fail. Continue recursively up the parent
 * hierarchy until we hit a UNION or an OPTIONAL parent. If we reach the root 
 * of the where clause for a subquery, then continue up the groups in which the 
 * subquery appears.
 * 
 * If the parent is a UNION, then remove the child from the UNION.
 * 
 * If a UNION has one child, then replace the UNION with the child.
 * 
 * If a UNION is empty, then fail the group in which it fails (unions are not
 * optional).
 * 
 * These rules should be triggered if a join is known to fail, which includes
 * the case of an unknown IV in a statement pattern as well
 * <code>GRAPH uri {}</code> where uri is not a named graph.
 * 
 * <pre>
 * 
 * TODO From BigdataEvaluationStrategyImpl3#945
 * 
 * Prunes the sop tree of optional join groups containing values
 * not in the lexicon.
 * 
 *         sopTree = stb.pruneGroups(sopTree, groupsToPrune);
 * 
 * 
 * If after pruning groups with unrecognized values we end up with a
 * UNION with no subqueries, we can safely just return an empty
 * iteration.
 * 
 *         if (SOp2BOpUtility.isEmptyUnion(sopTree.getRoot())) {
 *             return new EmptyIteration<BindingSet, QueryEvaluationException>();
 *         }
 * </pre>
 * 
 * and also if we encounter a value not in the lexicon, we can still continue
 * with the query if the value is in either an optional tail or an optional join
 * group (i.e. if it appears on the right side of a LeftJoin). We can also
 * continue if the value is in a UNION. Otherwise we can stop evaluating right
 * now.
 * 
 * <pre>
 *                 } catch (UnrecognizedValueException ex) {
 *                     if (sop.getGroup() == SOpTreeBuilder.ROOT_GROUP_ID) {
 *                         throw new UnrecognizedValueException(ex);
 *                     } else {
 *                         groupsToPrune.add(sopTree.getGroup(sop.getGroup()));
 *                     }
 *                 }
 * </pre>
 * 
 * 
 * ASTPruneUnknownTerms : If an unknown terms appears in a StatementPatternNode
 * then we get to either fail the query or prune that part of the query. If it
 * appears in an optional, then prune the optional. if it appears in union, the
 * prune that part of the union. if it appears at the top-level then there are
 * no solutions for that query. This is part of what
 * BigdataEvaluationStrategyImpl3#toPredicate(final StatementPattern
 * stmtPattern) is doing. Note that toVE() as called from that method will throw
 * an UnknownValueException if the term is not known to the database.
 * 
 * FIXME Isolate pruning logic since we need to use it in more than one place.
 */
public class ASTUnknownTermOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTUnknownTermOptimizer.class);

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        // Main WHERE clause
        {

            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                eliminateGroupsWithUnknownTerms(queryRoot, whereClause);
                
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
            for (int i = 0; i < namedSubqueries.size(); i++) {

                final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
                        .get(i);

                final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause != null) {

                    eliminateGroupsWithUnknownTerms(queryRoot, whereClause);
                    
                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    /**
     * If the group has an unknown term, simply prune it out from its parent.
     * If the group happens to be the top-level of the where, simply replace
     * the where with an empty join group.
     * 
     * @param op
     */
    private static void eliminateGroupsWithUnknownTerms(final QueryRoot queryRoot,
            final GroupNodeBase<IGroupMemberNode> op) {

    	/*
    	 * Check the statement patterns.
    	 */
    	for (int i = 0; i < op.arity(); i++) {
    		
    		final BOp sp = op.get(i);
    		
    		if (!(sp instanceof StatementPatternNode)) {
    			
    			continue;
    			
    		}
    		
    		for (int j = 0; j < sp.arity(); j++) {
    			
    			final BOp term = sp.get(j);
    			
    			if (term instanceof ConstantNode) {
    				
    				final IV iv = ((ConstantNode) term).getValue().getIV();
    				
    				if (iv == null || iv.isNullIV()) {
    					
    					pruneGroup(queryRoot, op);
    					
    				}
    				
    			}
    			
    		}
    		
    	}
    	
        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GroupNodeBase<?>) {

                @SuppressWarnings("unchecked")
                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) child;

                eliminateGroupsWithUnknownTerms(queryRoot, childGroup);

            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) subquery
                        .getWhereClause();

                eliminateGroupsWithUnknownTerms(queryRoot, childGroup);

            }

        }

    }
    
    private static void pruneGroup(final QueryRoot queryRoot,
            final GroupNodeBase<IGroupMemberNode> op) {

    	final IGroupNode parent = op.getParent();
    	
    	if (op.getParent() == null) {
    		
    		/*
    		 * We've reached the end of the line. Prune out the Where clause
    		 * from the QueryRoot.
    		 */
    		
    		queryRoot.setWhereClause(new JoinGroupNode());
    		
        } else if (parent instanceof UnionNode
                || ((op instanceof IJoinNode) && ((IJoinNode) op).isOptional())) {

    		/*
    		 * We've reached a stopping point - we can prune an optional group
    		 * and we can prune the child of a UNION.
    		 */
    		parent.removeChild(op);
    		
    	} else {
    		
    		/*
    		 * Continue onwards and upwards.
    		 */
    		pruneGroup(queryRoot, (GroupNodeBase) parent);
    		
    	}
    	
    }

    /**
     * Remove any empty (non-GRAPH) groups (normal groups and UNIONs, but not
     * GRAPH {}).
     */
    static private void removeEmptyChildGroups(final GraphPatternGroup<?> op) {

        int n = op.arity();

        for (int i = 0; i < n; i++) {

            final BOp child = op.get(i);

            if (!(child instanceof GroupNodeBase<?>))
                continue;

            if (((GroupNodeBase<?>) child).getContext() != null) {
                /*
                 * Do not prune GRAPH ?g {} or GRAPH uri {}. Those constructions
                 * have special semantics.
                 */
                continue;
            }

            if (child.arity() == 0) {

                // remove an empty child group.
                op.removeArg(child);

                // one less child to visit.
                n--;

            }

        }

    }

}
