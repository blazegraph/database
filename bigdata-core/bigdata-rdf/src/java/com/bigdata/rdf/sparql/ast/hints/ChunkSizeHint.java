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
import com.bigdata.rdf.sparql.ast.QueryHints;

/**
 * This is identical to the {@link BufferChunkCapacityHint}, but it is accessed
 * through the well known name {@link QueryHints#CHUNK_SIZE}.
 * <p>
 * Sets the capacity of the output buffer that used to accumulate chunks of
 * solutions (default {@value BufferAnnotations#DEFAULT_CHUNK_CAPACITY}).
 * Partial chunks may be automatically combined into full chunks.
 * <p>
 * Note: The "chunkSize" is a general property of the query engine. This query
 * hint does not change the structure of the query plan, but simply serves as a
 * directive to the query engine that it should allocate an output buffer for
 * the operator that will emit chunks of the indicated target capacity. This
 * query hint is allowed in any scope, but is generally used to effect the
 * behavior of a join group, a subquery, or the entire query. The hint is
 * transferred as an annotation onto all query plan operators generated from the
 * annotated scope.
 * 
 * @see BufferAnnotations#CHUNK_CAPACITY
 */
final class ChunkSizeHint extends AbstractChunkSizeHint {

    protected ChunkSizeHint() {
        super(QueryHints.CHUNK_SIZE,
                BufferAnnotations.DEFAULT_CHUNK_CAPACITY);
    }

}
