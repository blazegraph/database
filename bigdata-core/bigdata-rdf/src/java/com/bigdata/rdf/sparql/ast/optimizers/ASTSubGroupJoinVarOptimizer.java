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
 * Created on Oct 25, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Optimizer assigns join variables to sub-groups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTSubGroupJoinVarOptimizer implements IASTOptimizer {

    @SuppressWarnings("unchecked")
    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                    assignJoinVars(sa, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        assignJoinVars(sa, queryRoot.getWhereClause());

        return new QueryNodeWithBindingSet(queryRoot, bindingSets);

    }
    
    /**
     * Assign the join variable(s) to the group. The join variables are those
     * variables which are definitely bound by the time the group runs, so this
     * depends on the order of the nodes in the parent group. There may be zero
     * or more join variables. Child groups are processed recursively.
     * 
     * @param sa
     * @param group
     */
    private void assignJoinVars(final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        if (group.getParentGraphPatternGroup() != null) {

            /*
             * The variables which will be definitely bound based on an analysis
             * of the group.
             */
            final Set<IVariable<?>> boundByGroup = sa
                 .getDefinitelyProducedBindings(group,
                      new LinkedHashSet<IVariable<?>>(), true/* recursive */);

            /*
             * Find the set of variables which will be definitely bound by the
             * time the group is evaluated.
             */
            final Set<IVariable<?>> incomingBindings = sa
                .getDefinitelyIncomingBindings(
                        (GraphPatternGroup<?>) group,
                        new LinkedHashSet<IVariable<?>>());

            /*
             * This is only those variables which are bound on entry into the group
             * in which the SERVICE join appears *and* which are "must" bound
             * variables projected by the SERVICE.
             */
            boundByGroup.retainAll(incomingBindings);

            @SuppressWarnings("rawtypes")
            final IVariable[] joinVars = boundByGroup.toArray(new IVariable[0]);

            group.setJoinVars(joinVars);
            
            /*
             * The variables that will definitely be bound inside the subquery.
             */
            final Set<IVariable<?>> definitelyBoundInGroup =
                sa.getDefinitelyProducedBindings(
                    group, new LinkedHashSet<IVariable<?>>(), true);
            
            

            /*
             * Find the set of variables which have appeared in the query and
             * may be bound by the time the group is evaluated.
             */
            final Set<IVariable<?>> maybeIncomingBindings = sa
                .getMaybeIncomingBindings(
                    (GraphPatternGroup<?>) group, 
                    new LinkedHashSet<IVariable<?>>());

            
            /**
             * Add the variables that are used inside filters in the OPTIONAL,
             * since the SPARQL 1.1 semantics lifts these filters to the
             * upper level in case the join succeeds.
             * 
             * However, note that this may not be valid in the general case,
             * i.e. it is unclear how to deal with ill designed patterns, where
             * the inner subgroup contains an optional join reusing the 
             * variables. 
             */
            if (group instanceof JoinGroupNode) {
               
               final JoinGroupNode jgn = (JoinGroupNode)group;
               if (jgn.isOptional()) {
                  final Set<FilterNode> filters = new LinkedHashSet<FilterNode>();
                  
                  for (BOp node : jgn.args()) {

                     final Iterator<BOp> it = BOpUtility.preOrderIterator(node);
                     while (it.hasNext()) {
                        final BOp bop = it.next();
                        if (bop instanceof FilterNode) {
                           filters.add((FilterNode)bop);                           
                        } else if (node instanceof StatementPatternNode) {
                           final StatementPatternNode nodeAsSP = 
                              (StatementPatternNode)node;
                           final List<FilterNode> attachedFilters = 
                              nodeAsSP.getAttachedJoinFilters();
                           for (final FilterNode filter : attachedFilters) {
                              filters.add(filter);
                           }
                        }
                     }
                  }
                  
                  for (FilterNode fn : filters) {
                     definitelyBoundInGroup.addAll(sa.getSpannedVariables(fn,
                           true/* filters */, new LinkedHashSet<IVariable<?>>()));
                  }
               }
            }
            
            /*
             * Retain the defintely bound variables that have already
             * appeared previously in the query up to this point. 
             */
            definitelyBoundInGroup.retainAll(maybeIncomingBindings);

            
            @SuppressWarnings("rawtypes")
            final IVariable[] projectInVars = 
                  definitelyBoundInGroup.toArray(new IVariable[0]);

            group.setProjectInVars(projectInVars);
            
        }

        /*
         * Recursion.
         */
        for (IGroupMemberNode child : group) {

            if (child instanceof GraphPatternGroup<?>) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> subGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                assignJoinVars(sa, subGroup);

            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> subGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
                        .getWhereClause();

                assignJoinVars(sa, subGroup);

            }

        }

    }

}
