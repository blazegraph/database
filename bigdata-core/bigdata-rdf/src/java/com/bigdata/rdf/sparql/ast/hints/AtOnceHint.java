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

import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Query hint marks the operator as requiring "atOnce" evaluation. All solutions
 * will be buffered by the {@link QueryEngine} before the operator is evaluated.
 * When it is evaluated, it will receive all solutions in a single invocation of
 * that operator. However, the solutions MAY appear in multiple chunks since the
 * {@link QueryEngine} does not guarantee that the chunk will be merged before
 * the operator is invoked. This query hint is allowed in any scope. The hint is
 * transferred as an annotation onto all query plan operators generated from the
 * annotated scope.
 * <p>
 * Note: The "atOnce" hint is basically turned into <code>NOT(PIPELINED)</code>.
 * 
 * @see PipelineOp.Annotations#PIPELINED
 */
final class AtOnceHint extends AbstractBooleanQueryHint {

    protected AtOnceHint() {
        
        super(QueryHints.AT_ONCE, !PipelineOp.Annotations.DEFAULT_PIPELINED);

    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {

        if (op instanceof IQueryNode) {

            /*
             * Note: This is set on the queryHint Properties object and then
             * transferred to the pipeline operator when it is generated.
             */

            _setQueryHint(context, scope, op, PipelineOp.Annotations.PIPELINED,
                    !value);

        }

//        if (QueryHintScope.Query.equals(scope)) {
//            
//            /*
//             * Also stuff the query hint on the global context for things
//             * which look there.
//             */
//
//            conditionalSetGlobalProperty(context,
//                    PipelineOp.Annotations.PIPELINED, !value);
//
//        }

    }

}
