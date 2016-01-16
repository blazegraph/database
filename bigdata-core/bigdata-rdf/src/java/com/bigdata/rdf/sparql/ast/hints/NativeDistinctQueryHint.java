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

import com.bigdata.bop.solutions.HTreeDistinctBindingSetsOp;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Query hint for turning the {@link HTreeDistinctBindingSetsOp} on/off.
 * 
 * TODO This can only be used enable/disable the native distinct solutions both
 * on a query wide basis (via {@link AST2BOpContext}). However, it should be
 * possible to enable/disable this on a (sub-)select basis (by interpreting the
 * AST annotation when we are generating the query plan. When supporting this
 * more selective application, write a unit test which verifies that the native
 * distinct annotation is correctly interpreted when it appears on a
 * {@link QueryBase}.
 * 
 * TODO The same is true of {@link QueryHints#NATIVE_DISTINCT_SPO} and several
 * other similar "analytic" hints. However, this is not possible (or is more
 * difficult) when the native versus JVM semantics of the operation span
 * non-local operators (i.e., named subqueries with their non-local includes).
 */
final class NativeDistinctQueryHint extends AbstractBooleanQueryHint {

    protected NativeDistinctQueryHint() {
        super(QueryHints.NATIVE_DISTINCT_SOLUTIONS,
                QueryHints.DEFAULT_NATIVE_DISTINCT_SOLUTIONS);
    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {

        if (scope == QueryHintScope.Query) {

            context.nativeDistinctSolutions = value;

            return;

        }

        // if (op instanceof QueryBase) {
        //
        // super.attach(context, scope, op, value);
        //
        // }

        throw new QueryHintException(scope, op, getName(), value);

    }

}
