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

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * The maximum #of chunks that can be buffered before an the producer would
 * block (default {@value BufferAnnotations#DEFAULT_CHUNK_OF_CHUNKS_CAPACITY}).
 * Note that partial chunks may be combined into full chunks whose nominal
 * capacity is specified by {@link BufferAnnotations#CHUNK_CAPACITY}.
 * 
 * @see BufferAnnotations#CHUNK_OF_CHUNKS_CAPACITY
 */
final class BufferChunkOfChunksCapacityHint extends AbstractIntQueryHint {

    protected BufferChunkOfChunksCapacityHint() {
        super(BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                BufferAnnotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);
    }

    @Override
    public void handle(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase op, final Integer value) {

        if (op instanceof IQueryNode) {

            /*
             * Note: This is set on the queryHint Properties object and then
             * transferred to the pipeline operator when it is generated.
             */

            _setQueryHint(context, scope, op, getName(), value);

        }

//        if (QueryHintScope.Query.equals(scope)) {
//            
//            /*
//             * Also stuff the query hint on the global context for things which
//             * look there.
//             */
//
//            conditionalSetGlobalProperty(context,
//                    BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY, value);
//
//        }

    }

}
