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

import com.bigdata.bop.engine.NativeHeapStandloneChunkHandler;
import com.bigdata.bop.join.HTreeHashJoinUtility;
import com.bigdata.bop.join.IHashJoinUtility;
import com.bigdata.bop.join.JVMHashJoinUtility;
import com.bigdata.bop.join.SolutionSetHashJoinOp;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Query hint for turning analytic query on/off.
 * <p>
 * TODO Allow this to be specified for each hash index build rather than just
 * globally for the query execution context. The primary consumer of hash
 * indices is the {@link SolutionSetHashJoinOp}. That operator implementation
 * identical for both JVM and {@link HTree} based hash joins. Therefore, we
 * could make the decision about whether to use the {@link JVMHashJoinUtility}
 * or the {@link HTreeHashJoinUtility} when building the hash index by
 * annotating that operator and then let the {@link SolutionSetHashJoinOp}
 * handle the hash join by delegating to the appropriate
 * {@link IHashJoinUtility} implementation.
 */
final class AnalyticQueryHint extends AbstractBooleanQueryHint {

    protected AnalyticQueryHint() {
        super(QueryHints.ANALYTIC, QueryHints.DEFAULT_ANALYTIC);
    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Boolean value) {

        switch (scope) {
        case Query:
            context.nativeHashJoins = value;
            context.nativeDistinctSolutions = value;
            context.nativeDistinctSPO = value;
            context.queryEngineChunkHandler = NativeHeapStandloneChunkHandler.NATIVE_HEAP_INSTANCE;
            return;
        }

        throw new QueryHintException(scope, op, getName(), value);

    }

}
