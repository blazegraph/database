/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.ASTOptimizerResult;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.ConstructNode.Annotations;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Generates the {@link ProjectionNode} for a CONSTRUCT or DESCRIBE query. It is
 * populated with each variable which appears in the {@link ConstructNode}. The
 * {@link ASTDescribeOptimizer} MUST be run first for a DESCRIBE query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTConstructOptimizer implements IASTOptimizer {

//    private static final Logger log = Logger
//            .getLogger(ConstructOptimizer.class);
    
    public ASTConstructOptimizer() {
    }

    @Override
    public ASTOptimizerResult optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
           return new ASTOptimizerResult(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        switch (queryRoot.getQueryType()) {
        case CONSTRUCT:
            break;
        default:
           	if (context.nativeDistinctSPO) {
           	    queryRoot.setProperty(Annotations.NATIVE_DISTINCT, true);
           	}
           	return new ASTOptimizerResult(queryRoot, bindingSets);
        }

        final ConstructNode constructNode = queryRoot.getConstruct();

        if (constructNode == null) {

            throw new RuntimeException("No CONSTRUCT clause?");
            
        }

        final ProjectionNode projection;
        if (queryRoot.getProjection() == null) {

            /*
             * Set a new projection on the query.
             * 
             * Note: This handles both a CONSTRUCT query and a DESCRIBE query
             * when we are NOT maintaining a DESCRIBE cache.
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
             * DESCRIBE CACHE </a>
             */
            queryRoot.setProjection(projection = new ProjectionNode());

            projection.setReduced(true);

        } else {
            
            projection = queryRoot.getProjection();
            
        }

        // Add projected variables based on the CONSTRUCT template.
        {

            // Visit the distinct variables in the CONSTRUCT clause.
            final Iterator<IVariable<?>> itr = BOpUtility
                    .getSpannedVariables(constructNode);

            while (itr.hasNext()) {

                // Add each variable to the projection.
                projection.addProjectionVar(new VarNode(itr.next().getName()));

            }

        }

        if (context.nativeDistinctSPO) {

            /**
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/579">
             *      CONSTRUCT should apply DISTINCT (s,p,o) filter </a>
             */
            
            constructNode.setNativeDistinct(true);

        }

        final SliceNode slice = queryRoot.getSlice();
        final OrderByNode orderBy = queryRoot.getOrderBy();

        if (slice == null) {

            if (orderBy != null) {
                
                /**
                 * Clear the ORDER BY clause if unless a SLICE is also given.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/577"
                 *      > DESCRIBE with OFFSET/LIMIT must use sub-SELECT </a>
                 */

                queryRoot.setOrderBy(null);
                
            }

        } else if (slice != null) {

            /**
             * Push the WHERE clause into a sub-SELECT. The SLICE and the ORDER
             * BY (if present) are moved to the sub-SELECT. The sub-SELECT has
             * the same projection as the top-level query.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/577"
             *      > DESCRIBE with OFFSET/LIMIT must use sub-SELECT </a>
             */
         
            final SubqueryRoot subqueryRoot = new SubqueryRoot(QueryType.SELECT);
            
            // Make a copy of the top-level projection.
            subqueryRoot.setProjection((ProjectionNode) projection.clone());

            // Steal the WHERE clause from the top-level query.
            subqueryRoot.setWhereClause(queryRoot.getWhereClause());
            
            // Setup the new WHERE clause for the top-level query.
            queryRoot.setWhereClause(new JoinGroupNode(subqueryRoot));
            
            // Move the OFFSET/LIMIT onto the sub-SELECT.
            subqueryRoot.setSlice(slice);
            queryRoot.setSlice(null);

            if (orderBy != null) {

                // Move the ORDER BY clause (if present) onto the sub-SELECT.
                subqueryRoot.setOrderBy(orderBy);
                queryRoot.setOrderBy(null);
                
            }
            
        }

        return new ASTOptimizerResult(queryRoot, bindingSets);

    }

}
