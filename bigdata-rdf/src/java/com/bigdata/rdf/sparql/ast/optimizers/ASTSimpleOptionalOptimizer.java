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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collection;
import java.util.LinkedList;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.store.BD;

/**
 * A "simple optional" is an optional sub-group that contains only one statement
 * pattern, no sub-groups of its own, and no filters that require materialized
 * variables. We can lift these "simple optionals" into the parent group without
 * incurring the costs of launching a {@link SubqueryOp}.
 * 
 * 
 * FIXME : Simple optional: must lift filter onto the statement pattern node.
 * 
 * <pre>
 * where {
 *  ?x type Foo . // adds binding for ?x
 *  optional {
 *    ?x p ?y . // adds bindings for ?y if ?x != Bar
 *    filter (?x != Bar) .
 *  }
 * }
 * </pre
 * 
 * The filter(s) can be lifted, but they must be attached to the statement
 * pattern node such that toPredicate() puts them onto the predicate since they
 * must run *with* the join for that predicate. (The problem is that ?x != Bar
 * is filtering the optional join, not ?x).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSimpleOptionalOptimizer.java 5197 2011-09-15 19:10:44Z
 *          thompsonbry $
 */
public class ASTSimpleOptionalOptimizer implements IASTOptimizer {

    @SuppressWarnings("unchecked")
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {        

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        /*
         * Collect optional groups.
         * 
         * Note: We can not transform graph patterns inside of SERVICE calls so
         * this explicitly visits the interesting parts of the tree.
         */

        final Collection<JoinGroupNode> optionalGroups = new LinkedList<JoinGroupNode>();

        {

            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

                    collectOptionalGroups(namedSubquery.getWhereClause(),
                            optionalGroups);

                }

            }

            collectOptionalGroups(queryRoot.getWhereClause(), optionalGroups);

        }

        /*
         * For each optional group, if it qualifies as a simple optional then
         * lift the statement pattern node and and filters into the parent group
         * and mark the statement pattern node as "optional".
         */

        for(JoinGroupNode group : optionalGroups) {
            
            liftOptionalGroup(group);
            
        }
        
        return queryNode;
        
    }

    /**
     * Collect the optional groups.
     * <p>
     * Note: This will NOT visit stuff inside of SERVICE calls. If those graph
     * patterns get rewritten it has to be by the SERVICE, not us.
     * <p>
     * Note: Do not bother to collect an "optional" unless it has a parent join
     * group node (they all should).
     */
    @SuppressWarnings("unchecked")
    private void collectOptionalGroups(
            final IGroupNode<IGroupMemberNode> group,
            final Collection<JoinGroupNode> optionalGroups) {

        if (group instanceof JoinGroupNode && group.isOptional()
                && group.getParent() != null) {
            
            optionalGroups.add((JoinGroupNode) group);
            
        }
        
        for(IGroupMemberNode child : group) {

            if (child instanceof ServiceNode) {

                final ServiceNode serviceNode = ((ServiceNode) child);
                
                final URI serviceUri = serviceNode.getServiceURI();

                if (!BD.SEARCH.equals(serviceUri)) {
                    /*
                     * Do NOT translate SERVICE nodes (unless they are a bigdata
                     * service).
                     */

                    continue;

                }

                collectOptionalGroups(serviceNode.getGroupNode(),
                        optionalGroups);
                
            }
            
            if (!(child instanceof IGroupNode<?>))
                continue;

            collectOptionalGroups((IGroupNode<IGroupMemberNode>) child,
                    optionalGroups);
           
        }

    }
    
    /**
     * If the {@link JoinGroupNode} qualifies as a simple optional then lift the
     * statement pattern node and and filters into the parent group and mark the
     * statement pattern node as "optional".
     */
    private void liftOptionalGroup(final JoinGroupNode group) {
       
        if(!isSimpleOptional(group)) {
        
            // Not a simple optional.
            return;
            
        }

        // The immediately parent SHOULD be a join group.
        final JoinGroupNode p = (JoinGroupNode) group.getParent();

        if (p == null)
            throw new AssertionError();
        
        for(IGroupMemberNode child : group) {
            
            if(child instanceof StatementPatternNode) {

                final StatementPatternNode sp = (StatementPatternNode) child;

                /*
                 * Set the flag so we know to do an OPTIONAL join for this
                 * statement pattern.
                 */

                sp.setSimpleOptional(true);

                p.replaceWith((BOp) group, (BOp) child);

            } else if (child instanceof FilterNode) {

                /*
                 * We can lift a filter as long as its materialization
                 * requirements would be satisfied in the parent.
                 */

                p.replaceWith((BOp) group, (BOp) child);

            } else {

                /*
                 * This would indicate an error in the logic to identify which
                 * join groups qualify as "simple" optionals.
                 */
                
                throw new AssertionError(
                        "Unexpected child for simple optional: group=" + group
                                + ", child=" + child);
                
            }
            
        }
       
    }
    
    /**
     * Return <code>true</code> iff the join group is a "simple optional".
     * 
     * @param group
     *            Some {@link JoinGroupNode}.
     */
    public static boolean isSimpleOptional(
            final JoinGroupNode group) {

        if (!group.isOptional()) {

            // first, the whole group must be optional
            return false;

        }

        /*
         * Second, make sure we have only one statement pattern, no sub-queries,
         * and no filters that require materialization.
         */
        StatementPatternNode sp = null;

        for (IQueryNode node : group) {

            if (node instanceof StatementPatternNode) {

                if (sp != null) {
                    /*
                     * We already have one statement pattern so this is not a
                     * simple optional.
                     */
                    return false;
                }

                sp = (StatementPatternNode) node;

            } else if (node instanceof FilterNode) {

                final FilterNode filter = (FilterNode) node;

                final INeedsMaterialization req = filter
                        .getMaterializationRequirement();

                if (req.getRequirement() != INeedsMaterialization.Requirement.NEVER) {

                    /*
                     * There are materialization requirements for this join.
                     * 
                     * FIXME We can lift a filter which only depends on
                     * variables which are "incoming bound" into the parent.
                     * 
                     * FIXME If a filter depends on variables which are bound by
                     * the sole statement pattern in the optional join group
                     * (and which are not incoming bound) then we can still lift
                     * the filter IFF it does not have any materialization
                     * requirements for the variables bound by the optional's
                     * statement pattern node.
                     */

                    return false;

                }

            } else {

                /*
                 * Anything else will queer the deal.
                 */

                return false;

            }

        }

        /*
         * If we found one and only one statement pattern and have not tripped
         * any of the other conditions then this is a "simple" optional.
         */

        return sp != null;

    }

}
