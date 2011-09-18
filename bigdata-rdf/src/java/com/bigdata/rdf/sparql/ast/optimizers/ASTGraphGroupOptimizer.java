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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;

/**
 * Handles a variety of special constructions related to graph graph groups.
 * 
 * <dl>
 * <dt>GRAPH ?foo</dt>
 * <dd>
 * Anything nested (even if a subquery) is constrained to be from ?foo.
 * <p>
 * All nested statement patterns must have ?foo as their context, even if they
 * occur within a subquery. (This is not true for a named subquery which just
 * projects its solutions but does not inherit the parent's graph context.
 * However, if we lifted the named subquery out, e.g., for bottom up evaluation
 * semantics, then we should probably impose the GRAPH constraint on the named
 * subquery.)</dd>
 * <dt>GRAPH ?foo { GRAPH ?bar } }</dt>
 * <dd>The easy way to enforce this constraint when there are nested graph
 * patterns is with a <code>SameTerm(?foo,?bar)</code> constraint inside of the
 * nested graph pattern.
 * <p>
 * The problem with this is that it does not enforce the constraint as soon as
 * possible under some conditions. A rewrite of the variable would have that
 * effect but the rewrite needs to be aware of variable scope rules so we do not
 * rewrite the variable within a subquery if it is not projected by that
 * subquery. We would also have to add a BIND(?foo AS ?bar) to make ?bar visible
 * in the scope of parent groups.
 * <p>
 * However, there is an INCLUDE problem too. That could be handled by moving the
 * INCLUDE into a subgroup with a BIND to renamed the variable or by adding a
 * "projection" to the INCLUDE so we could rename the variable there.
 * <p>
 * Since this construction of nested graph patterns is rare, and since it is
 * complicated to make it more efficient, we are going with the SameTerm()
 * constraint for now.</dd>
 * <dt>GRAPH uri</dt>
 * <dd>
 * This is only allowed if the uri is in the named data set (or if no data set
 * was given). Translation time error.</dd>
 * <dt>GRAPH uri { ... GRAPH uri2 ... }</dt>
 * <dd>It is an query error if a <code>GRAPH uri</code> is nested within another
 * <code>GRAPH uri</code> for distinct IRIs.</dd>
 * <dt>GRAPH ?foo { ... GRAPH uri ... }</dt>
 * <dd>If a constant is nested within a <i>non-optional</i>
 * <code>GRAPH uri</code> then that constant could be lifted up and bound using
 * Constant/2 on the outer graph pattern. Again, this is an optimization which
 * may not contribute much value except in very rare cases. We do not need to do
 * anything additional to make this case correct.</dd>
 * <dt>GRAPH ?g {}</dt>
 * <dd>This matches the distinct named graphs in the named graph portion of the
 * data set (special case). There are several variations on this which need to
 * be handled:
 * <ul>
 * <li>If ?g might be bound or is not bound:
 * <ul>
 * <li>If there is no data set, then this should be translated into
 * sp(_,_,_,?g)[filter=distinct] that should be recognized and evaluated using a
 * distinct term advancer on CSPO.</li>
 * <li>If the named graphs are listed explicitly, then just visit that list
 * [e.g., pump them into an htree].</li>
 * </ul>
 * Either way, if there is a filter then apply the filter to the scan/list.</li>
 * <li>If <code>?g</code> is bound coming into <code>graph ?g {}</code> then we
 * want to test for the existence of at least one statement on the CSPO index
 * for <code>?g</code>. This is a CSPO iterator with C bound and a limit of one.
 * </li>
 * </ul>
 * </dd>
 * <dt>GRAPH <uri> {}</dt>
 * <dd>This is an existence test for the graph. This is a CSPO iterator with C
 * bound and a limit of one. However, lift this into a named subquery since we
 * only want to run it once (or precompute the result).</dd>
 * </dl>
 * Note: This optimizer MUST run before optimizers which lift out named
 * subqueries in order to correctly impose the GRAPH constraints on the named
 * subquery.
 * 
 * @see ASTEmptyGroupOptimizer, which handles <code>{}</code> for non-GRAPH
 *      groups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTEmptyGroupOptimizer.java 5177 2011-09-12 17:49:44Z
 *          thompsonbry $
 */
public class ASTGraphGroupOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTGraphGroupOptimizer.class);

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;
        
        /*
         * Collect GRAPH groups.
         * 
         * Note: We can not transform graph patterns inside of SERVICE calls so
         * this explicitly visits the interesting parts of the tree.
         */

        final Collection<JoinGroupNode> graphGroups = new LinkedList<JoinGroupNode>();

        {

            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

                    collectGraphGroups(namedSubquery.getWhereClause(),
                            null/* context */, graphGroups);

                }

            }

            collectGraphGroups(queryRoot.getWhereClause(), null/* context */,
                    graphGroups);

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        for(JoinGroupNode group : graphGroups) {
            
//            liftOptionalGroup(sa, group);
            
        }
        
        return queryNode;
        
    }

    /**
     * Collect the GRAPH groups.
     * <p>
     * Note: This will NOT visit stuff inside of SERVICE calls. If those graph
     * patterns get rewritten it has to be by the SERVICE, not us.
     * @param group
     * @param context
     * @param graphGroups
     */
    @SuppressWarnings("unchecked")
    private void collectGraphGroups(final IGroupNode<IGroupMemberNode> group,
            TermNode context,
            final Collection<JoinGroupNode> graphGroups) {

        if (group instanceof JoinGroupNode && group.getContext() != null) {

            final TermNode innerContext = group.getContext();
            
            if (context == null)
                context = innerContext;
            
            graphGroups.add((JoinGroupNode) group);

        }

        for (IGroupMemberNode child : group) {

            if (child instanceof ServiceNode) {

                /*
                 * Do NOT translate SERVICE nodes (unless they are a bigdata
                 * service).
                 */

                continue;

            }

            if (!(child instanceof IGroupNode<?>))
                continue;

            collectGraphGroups((IGroupNode<IGroupMemberNode>) child, context,
                    graphGroups);

        }

    }

}
