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
 * Created on Nov 27, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.FilterExistsModeEnum;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Used to specify the query plan for FILTER (NOT) EXISTS. There are two basic
 * plans: vectored sub-plan and subquery with LIMIT ONE. Each plan has its
 * advantages.
 * 
 * @see FilterExistsModeEnum
 * @see <a href="http://trac.blazegraph.com/ticket/988"> bad performance for FILTER
 *      EXISTS </a>
 */
final class FilterExistsHint extends AbstractQueryHint<FilterExistsModeEnum> {

    protected FilterExistsHint() {
        super(QueryHints.FILTER_EXISTS,
                QueryHints.DEFAULT_FILTER_EXISTS);
    }

    @Override
    public void handle(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op,
            final FilterExistsModeEnum value) {

        if (op instanceof JoinGroupNode
                && ((JoinGroupNode) op).getParent() == null) {
            /*
             * This is the top-level join group inside of the FILTER. It does
             * not have a direct parent. We resolve the parent ExistsNode or
             * NotExistsNode by searching from the top-level query root.
             */

            final JoinGroupNode filterGroup = (JoinGroupNode) op;

            final IQueryNode p = StaticAnalysis.findParent(queryRoot,
                    filterGroup);

            if (p instanceof FilterNode) {

                final IValueExpressionNode n = ((FilterNode) p)
                        .getValueExpressionNode();

                if (n instanceof SubqueryFunctionNodeBase) {

                    ((SubqueryFunctionNodeBase) n).setFilterExistsMode(value);

                }

            }
            
//            _setAnnotation(context, scope, op, getName(), value);

        }

    }

    @Override
    public FilterExistsModeEnum validate(final String value) {
        
        return FilterExistsModeEnum.valueOf(value);
        
    }

}
