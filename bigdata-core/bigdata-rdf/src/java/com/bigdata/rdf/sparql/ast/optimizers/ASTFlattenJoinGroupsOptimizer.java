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
 * Created on Oct 7, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedList;
import java.util.List;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Flatten nested (non-optional,non-minus) JoinGroupNodes whenever possible.
 * 
 *   JoinGroupNode {
 *     JoinGroupNode [context=VarNode(sid)] {
 *       StatementPatternNode(VarNode(a), VarNode(b), VarNode(c), VarNode(sid)) [scope=NAMED_CONTEXTS]
 *     }
 *     StatementPatternNode(VarNode(sid), ConstantNode(TermId(6U)[http://example.com/source]), VarNode(src)) [scope=DEFAULT_CONTEXTS]
 *   }
 *   
 *   ==>
 *   
 *   JoinGroupNode {
 *     StatementPatternNode(VarNode(a), VarNode(b), VarNode(c), VarNode(sid)) [scope=NAMED_CONTEXTS]
 *     StatementPatternNode(VarNode(sid), ConstantNode(TermId(6U)[http://example.com/source]), VarNode(src)) [scope=DEFAULT_CONTEXTS]
 *   }
 */
public class ASTFlattenJoinGroupsOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger
//            .getLogger(ASTFlattenUnionsOptimizer.class);
//    
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

            final GroupNodeBase<?> whereClause = (GroupNodeBase<?>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                flattenGroups(whereClause);
                
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

                final GroupNodeBase<?> whereClause = (GroupNodeBase<?>) namedSubquery
                        .getWhereClause();

                if (whereClause != null) {

                    flattenGroups(whereClause);
                    
                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    /**
     * 
     * 
     * @param op
     */
    private static void flattenGroups(final GroupNodeBase<?> op) {

        /*
         * Recursion first, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GroupNodeBase<?>) {

                final GroupNodeBase<?> childGroup = (GroupNodeBase<?>) child;

                flattenGroups(childGroup);
                
            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) subquery
                        .getWhereClause();

                flattenGroups(childGroup);

            }

        }

        final IGroupNode<?> parent = op.getParent();
        
        if (op instanceof JoinGroupNode && 
        		!((JoinGroupNode) op).isOptional() &&
        		!((JoinGroupNode) op).isMinus() &&
        		parent != null && parent instanceof JoinGroupNode) {
            
        	final JoinGroupNode thisJoinGroup = (JoinGroupNode) op;
        	
            final JoinGroupNode parentJoinGroup = (JoinGroupNode) parent;
            
            int pos = parentJoinGroup.indexOf(thisJoinGroup);

            final List<IGroupMemberNode> children = 
            		new LinkedList<IGroupMemberNode>(thisJoinGroup.getChildren());

            for (IGroupMemberNode child : children) {
            	
            	thisJoinGroup.removeChild(child);
            	
            	parentJoinGroup.addArg(pos++, child);
            	
            }
            
            parentJoinGroup.removeChild(thisJoinGroup);
            
        }

    }

}
