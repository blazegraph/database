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

import com.bigdata.bop.engine.IChunkHandler;
import com.bigdata.bop.engine.NativeHeapStandloneChunkHandler;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Query hint deciding on the {@link IChunkHandler} to be used.
 * 
 * @see BLZG-533 (Vector query engine on the native heap)
 */
final class QueryEngineChunkHandlerQueryHint extends AbstractQueryHint<QueryEngineChunkHandlerEnum> {

    protected QueryEngineChunkHandlerQueryHint() {
        super(QueryHints.QUERY_ENGINE_CHUNK_HANDLER,
                QueryEngineChunkHandlerEnum.valueOf(QueryHints.DEFAULT_QUERY_ENGINE_CHUNK_HANDLER.getClass()));
    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final QueryEngineChunkHandlerEnum value) {

        switch (scope) {
        case Query:
            switch(value) {
            case Managed:
                context.queryEngineChunkHandler = NativeHeapStandloneChunkHandler.MANAGED_HEAP_INSTANCE;
                break;
            case Native:
                context.queryEngineChunkHandler = NativeHeapStandloneChunkHandler.NATIVE_HEAP_INSTANCE;
                break;
            default:
                throw new UnsupportedOperationException();
            }
            return;
        }

        throw new QueryHintException(scope, op, getName(), value);

    }

    @Override
    public QueryEngineChunkHandlerEnum validate(final String value) {
        
        return QueryEngineChunkHandlerEnum.valueOf(value);
        
    }

}
