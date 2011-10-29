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
 * Created on Oct 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Optimizer attaches {@link FilterNode}s which will run as "join filters" to
 * {@link StatementPatternNode}s. The joins must already be in the order in
 * which they will be evaluated.
 *
 * TODO Can this also handle RangeBOp attachments? See the bottom of
 * AST2BOpUtility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTAttachJoinFiltersOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                    attachJoinFilters(context, sa, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        attachJoinFilters(context, sa, queryRoot.getWhereClause());

        return queryRoot;
        
    }

    /**
     * Recursively process groups.
     * 
     * @param context
     * @param sa
     * @param group
     */
    @SuppressWarnings("unchecked")
    private void attachJoinFilters(final AST2BOpContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        /*
         * Recursion.
         */
        for(IGroupMemberNode child : group) {

            if (child instanceof GraphPatternGroup<?>) {
            
                attachJoinFilters(context, sa,
                        (GraphPatternGroup<IGroupMemberNode>) group);
                
            } else if (child instanceof SubqueryRoot) {
                
                attachJoinFilters(context, sa,
                        ((SubqueryRoot) child).getWhereClause());
                
            }
            
        }
        
        if (group instanceof JoinGroupNode) {
            
            /*
             * Filter attachment for this join group.
             */
            
            attachJoinFilters2(context, sa, (JoinGroupNode) group);
            
        }
        
    }
    
    /**
     * Figure out which filters will be attached to which statement patterns.
     * This only inspects the required statement patterns. Simple optionals are
     * handled by {@link ASTSimpleOptionalOptimizer}.
     * <p>
     * Note: This handles re-attach by collecting previously attached FILTERS
     * from required joins.
     * 
     * FIXME We must be able to attach a FILTER to any {@link IJoinNode}. Those
     * filters need to be run with (or after) that join. This includes INCLUDE,
     * subgroup, and subquery joins NOT just statement pattern nodes.
     */
    private void attachJoinFilters2(final AST2BOpContext context,
            final StaticAnalysis sa, final JoinGroupNode group) {

        /*
         * Collect all required joins and all join filters.
         */

        // The join path (required joins only).
        final List<IJoinNode> requiredJoins = new LinkedList<IJoinNode>();

        // The join filters.
        final List<FilterNode> joinFilters = new LinkedList<FilterNode>(
                sa.getJoinFilters(group));

        for (IGroupMemberNode child : group) {
            
            if (!(child instanceof IJoinNode)) {
                continue;
            }
            
            final IJoinNode aJoinNode = (IJoinNode) child;
            
            if (aJoinNode.isOptional()) {
                continue;
            }
            
            requiredJoins.add(aJoinNode);
            
            final List<FilterNode> ownJoinFilters = aJoinNode
                    .getAttachedJoinFilters();
            
            if (ownJoinFilters != null) {
            
                // Pick up any join filters already attached to this join node.
                
                joinFilters.addAll(ownJoinFilters);
                
                aJoinNode.setAttachedJoinFilters(null);
                
            }

        }

        if (requiredJoins.isEmpty()) {

            // Nothing to do.
            return;
            
        }

        final int requiredJoinCount = requiredJoins.size();

        final IJoinNode[] path = new IJoinNode[requiredJoinCount];

        final Set<IVariable<?>> knownBound = sa.getDefinitelyIncomingBindings(
                group, new LinkedHashSet<IVariable<?>>());

        /*
         * Figure out which filters are attached to which joins.
         */

        final FilterNode[][] assignedConstraints = sa.getJoinGraphConstraints(
                path, joinFilters.toArray(new FilterNode[joinFilters.size()]),
                knownBound, true/* pathIsComplete */);
        /*
         * Attach the join filters.
         */
        for (int i = 0; i < requiredJoinCount; i++) {

            final IJoinNode tmp = path[i];

            tmp.setAttachedJoinFilters(Arrays.asList(assignedConstraints[i]));

        }

    }

}
