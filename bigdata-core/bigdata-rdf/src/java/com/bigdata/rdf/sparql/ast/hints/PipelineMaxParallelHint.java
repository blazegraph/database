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
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Sets the maximum #of operator evaluation tasks which can execute
 * concurrently.
 * <p>
 * Note: "maxParallel" is a general property of the query engine. This query
 * hint does not change the structure of the query plan, but simply serves as a
 * directive to the query engine that it should not allow more than the
 * indicated number of parallel instances of the operator to execute
 * concurrently. This query hint is allowed in any scope. The hint is
 * transferred as an annotation onto all query plan operators generated from the
 * annotated scope.
 * 
 * @see PipelineOp.Annotations#MAX_PARALLEL
 */
final class PipelineMaxParallelHint extends AbstractIntQueryHint {

    protected PipelineMaxParallelHint() {
        super(QueryHints.MAX_PARALLEL,
                PipelineOp.Annotations.DEFAULT_MAX_PARALLEL);
    }

    @Override
    public void handle(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Integer value) {

        if (op instanceof IJoinNode) {

            /*
             * Note: This is set on the queryHint Properties object and then
             * transferred to the pipeline operator when it is generated.
             */

            _setQueryHint(context, scope, op,
                    PipelineOp.Annotations.MAX_PARALLEL, value);

        }

//        if (QueryHintScope.Query.equals(scope)) {
//
//            /*
//             * Also stuff the query hint on the global context for things which
//             * look there.
//             */
//
//            conditionalSetGlobalProperty(context,
//                    PipelineOp.Annotations.MAX_PARALLEL, value);
//
//        }

    }

}
