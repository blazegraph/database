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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinSetUtil;
import com.bigdata.rdf.sparql.ast.JoinSetUtil.VertexJoinSet;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Rewrites join groups having one or more joins which would involve a full
 * cross product as hash joins of sub-groups where there is a constraint imposed
 * indirectly via a FILTER operating across the variables bound by the joins.
 * This handles queries such as BSBM Q5.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/253
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTHashJoinOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTHashJoinOptimizer.class);
    
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

                    optimizeJoinGroups(context, sa, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        optimizeJoinGroups(context, sa, queryRoot.getWhereClause());

        return new QueryNodeWithBindingSet(queryRoot, bindingSets);
        
    }

    /**
     * Identify sets of joins which share variables only indirectly through a
     * constraint (FILTER). Such joins are pushed down into a sub-group along
     * with the constraint. The sub-group can be efficiently joined back to the
     * parent group (using a hash join) as long as there is a shared variable
     * between the sub-group and the parent (this condition is satisified if one
     * of the joins shares a variable with the parent group).
     */
    private void optimizeJoinGroups(final IEvaluationContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        final int arity = group.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = (BOp) group.get(i);
 
            if (child instanceof GraphPatternGroup<?>) {

                /*
                 * Note: Do recursion *before* we do the rewrite.
                 */
                optimizeJoinGroups(context, sa,
                        ((GraphPatternGroup<IGroupMemberNode>) child));

            } else if (child instanceof ServiceNode) {
            
                // Do not rewrite things inside of a SERVICE node.
                continue;
                
            }

        }
        
        /*
         * Analyze the joins in the group.
         */
        final JoinSetUtil joinSets = new JoinSetUtil(sa, null/* knownBound */,
                group);

        if (joinSets.joinFilters.isEmpty()) {
            /*
             * All of the required joins in this group can be made using
             * directly shared variables.
             */
            return;
        }

        /*
         * There is more than one join set. Join sets, by definition, are sets
         * of vertices with disjoint sets of variables (no variables are shared
         * between different join sets).
         * 
         * This means that we will have a full cross product between those join
         * sets unless there are some filters which indirectly share variables
         * and hence constraint the joins between those join sets.
         * 
         * FIXME Identify joins which depend on indirectly shared variables and
         * push them down into a sub-group. The largest join set stays in this
         * group. The other join sets get pushed down into subgroups. Any filter
         * which is fully bound in the subgroup moves into the subgroup. This
         * needs to be done with some awareness of a good join order (e.g.,
         * after the static optimizer) since we need to know which joins will
         * run before each subgroup that we push down in order to know what
         * variables are known bound on entry.
         * 
         * TODO Should we always push down a sub-group for a disjoint join set?
         * A hash join may very well be more efficient than a pipelined join as
         * the sub-group can run once (actually, it could be lifted out into a
         * named subquery) and the hash join will wind up doing less work than a
         * pipelined join.
         * 
         * TODO This process could use a recursive expansion in case we can
         * connect things transitively. It is really just like computing the
         * direct join sets. The inner loop would scan everything not yet "used"
         * and recurse if we are able to build up a larger join set.
         * 
         * TODO A DISTINCT projection into the sub-group would benefit BSBM Q5,
         * but we need to recognize that the DISTINCT projection is Ok based on
         * both the top-level projection and the fact that the join to pick up
         * the other variable (productLabel) would occur against the distinct
         * variable (product).
         */
        final int directJoinSetCount = joinSets.directJoinSets.size();

        if (directJoinSetCount > 1) {

            final VertexJoinSet[] a = joinSets.directJoinSets
                    .toArray(new VertexJoinSet[directJoinSetCount]);

            /*
             * Sort into order by decreasing join set size (#of vertices). This
             * let's us handle the join groups with the most vertices first.
             */
            Arrays.sort(a, new VertexJoinSetComparator());

            for (int i = 0; i < a.length; i++) {

                for (int j = i + 1; j < a.length; j++) {

                    final Set<IVariable<?>> set1 = a[i].joinvars;
                    final Set<IVariable<?>> set2 = a[j].joinvars;

                    final Set<IVariable<?>> joinvars = new LinkedHashSet<IVariable<?>>();
                    joinvars.addAll(set1);
                    joinvars.addAll(set2);

                    for (FilterNode f : joinSets.joinFilters) {

                        if (sa.isFullyBound(f, set1)
                                || sa.isFullyBound(f, set2)) {
                            // filter runs with one of the join sets.
                            continue;
                        }

                        if (sa.isFullyBound(f, joinvars)) {
                            /*
                             * This join filter does not run with either of
                             * those join sets which considered by themselves,
                             * but it can run when we consider those join sets
                             * together. Thus the filter implicitly shares some
                             * variables across the join sets and provides a
                             * join which is at least somewhat constrained.
                             */
                            log.error("indirect join: joinSet1=" + a[i]
                                    + ",joinSet2=" + a[j] + " on filter=" + f);
                        }

                    }

                }

            }

        }

    }
    
    /**
     * Place {@link VertexJoinSet}s into decreasing order by the #of vertices.
     */
    private static class VertexJoinSetComparator implements
            Comparator<VertexJoinSet> {

        @Override
        public int compare(VertexJoinSet o1, VertexJoinSet o2) {

            return o2.vertices.size() - o1.vertices.size();
            
        }
        
    }

}
