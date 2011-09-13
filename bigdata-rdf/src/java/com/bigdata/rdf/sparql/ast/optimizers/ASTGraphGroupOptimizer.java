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

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.IQueryNode;

/**
 * Handles a variety of special constructions related to graph graph groups.
 * 
 * <pre>
 * - GRAPH ?foo means that anything nested (even if a subquery) is
 *   contrained to be from ?foo.  
 * 
 *    - All nested statement patterns must have ?foo as their context,
 *      even if they occur within a subquery. (This is not true for a
 *      named subquery which just projects its solutions but does not
 *      inherit the parent's graph context.)
 *    
 *    - GRAPH ?foo { GRAPH ?bar } }.  The easy way to enforce this
 *      constraint when there are nested graph patterns is with a
 *      SameTerm(?foo,?bar) constraint inside of the nested graph
 *      pattern.
 * 
 *      The problem with this is that it does not enforce the constraint
 *      as soon as possible under some conditions. A rewrite of the
 *      variable would have that effect but the rewrite needs to be aware
 *      of variable scope rules so we do not rewrite the variable within
 *      a subquery if it is not projected by that subquery.  We would
 *      also have to add a BIND(?foo AS ?bar) to make ?bar visible in the
 *      scope of parent groups.  
 * 
 *      However, there is an INCLUDE problem too.  That could be handled
 *      by moving the INCLUDE into a subgroup with a BIND to renamed the
 *      variable or by adding a "projection" to the INCLUDE so we could
 *      rename the variable there.
 * 
 *      Since this construction of nexted graph patterns is rare, and
 *      since it is complicated to make it more efficient, we are going
 *      with the SameTerm() constraint for now.
 * 
 *    - If a constant is nested within a non-optional GRAPH <uri> then
 *      that constant could be lifted up and bound using Constant/2 on
 *      the outer graph pattern.  Again, this is an optimization which
 *      may not contribute much value except in very rare cases.  We do
 *      not need to do anything additional to make this case correct.
 * 
 *    - It is an query error if a GRAPH <uri> is nested within another
 *      GRAPH <uri> for distinct IRIs.
 * 
 * - GRAPH uri is only allowed if the uri is in the named data set (or if
 *   no data set was given).  Translation time error.
 * 
 * - An empty {} matches a single empty solution. Since we always push in
 *   an empty solution and the join of anything with an empty solution is
 *   that source solution, this is the same as not running the group, so
 *   we just eliminate the empty group.
 *          
 * - GRAPH ?g {} matches the distinct named graphs in the named graph
 *   portion of the data set (special case). 
 * 
 *   - If ?g might be bound or is not bound:
 * 
 *      - If there is no data set, then this should be translated into
 *        sp(_,_,_,?g)[filter=distinct] that should be recognized and
 *        evaluated using a distinct term advancer on CSPO.
 * 
 *      - If the named graphs are listed explicitly, then just visit that
 *        list [e.g., pump them into an htree].
 * 
 *     Either way, if there is a filter then apply the filter to the
 *     scan/list.
 * 
 *   - If ?g is bound coming into the join group then we want to test for
 *     the existence of at least one statement on the CSPO index for ?g.
 *     This is a CSPO iterator with C bound and a limit of one.
 *          
 * - GRAPH <uri> {} is an existence test for the graph. This is a CSPO
 *   iterator with C bound and a limit of one.  However, lift this into a
 *   named subquery since we only want to run it once (or precompute the
 *   result).
 * 
 * </pre>
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

//        if (!(queryNode instanceof QueryRoot))
//            return queryNode;
//
//        final QueryRoot queryRoot = (QueryRoot) queryNode;
//        
//        /*
//         * Lift any ServiceNode out of the main WHERE clause (including any
//         * embedded subqueries). We can not have any service invocations run
//         * from the main WHERE clause because they will be invoked once for each
//         * solution pushed into the query, even if the ServiceNode is the first
//         * operator in the query plan. Each such ServiceNode is replaced by a
//         * named subquery root and a named subquery include.
//         */
//
//        {
//
//            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryRoot
//                    .getWhereClause();
//
//            if (whereClause != null) {
//
//                eliminateEmptyGroups(whereClause);
//
//            }
//
//        }
//
//        /*
//         * Examine each named subquery. If there is more than one ServiceNode,
//         * or if a ServiceNode is embedded in a subquery, then lift it out into
//         * its own named subquery root, replacing it with a named subquery
//         * include.
//         */
//        if (queryRoot.getNamedSubqueries() != null) {
//
//            final NamedSubqueriesNode namedSubqueries = queryRoot
//                    .getNamedSubqueries();
//
//            /*
//             * Note: This loop uses the current size() and get(i) to avoid
//             * problems with concurrent modification during visitation.
//             */
//            for (int i = 0; i < namedSubqueries.size(); i++) {
//
//                final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
//                        .get(i);
//
//                final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) namedSubquery
//                        .getWhereClause();
//
//                if (whereClause != null) {
//
//                    eliminateEmptyGroups(whereClause);
//
//                }
//
//            }
//
//        }
//
////        log.error("\nafter rewrite:\n" + queryNode);

        return queryNode;

    }

}
