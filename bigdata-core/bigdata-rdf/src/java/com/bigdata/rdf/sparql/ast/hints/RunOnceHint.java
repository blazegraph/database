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
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSparql11SubqueryOptimizer;

/**
 * Query hint indicating whether or not a Sub-Select should be transformed into
 * a named subquery, lifting its evaluation out of the main body of the query
 * and replacing the subquery with an INCLUDE. When <code>true</code>, the
 * subquery will be lifted out. When <code>false</code>, the subquery will not
 * be lifted unless other semantics require that it be lifted out regardless.
 * This hint must be used with {@link QueryHintScope#SubQuery}.
 * <p>
 * For example, the following may be used to lift out the sub-select in which it
 * appears into a {@link NamedSubqueryRoot}. The lifted expression will be
 * executed exactly once.
 * 
 * <pre>
 * hint:SubQuery hint:runOnce "true" .
 * </pre>
 * <p>
 * Note: This sets the {@link SubqueryRoot.Annotations#RUN_ONCE} AST annotation
 * which is then interpreted by the {@link ASTSparql11SubqueryOptimizer}.
 */
final class RunOnceHint extends AbstractBooleanQueryHint {

    protected RunOnceHint() {

        super(QueryHints.RUN_ONCE, null/* default */);

    }

    @Override
    public void handle(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {

        if (scope != QueryHintScope.SubQuery) {
        
            throw new QueryHintException(scope, op,
                    getName(), value);

        }
        
        if (op instanceof SubqueryRoot) {

            _setAnnotation(context, scope, op,
                    SubqueryRoot.Annotations.RUN_ONCE, value);

        }

    }

}
