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
 * Created on Oct 22, 2010
 */

package com.bigdata.bop.engine;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.solutions.SolutionSetStream;

/**
 * Implementation supports a standalone database. Depending on the specific
 * instance used, either the generated chunk is left on the Java heap or it is
 * migrated onto the native heap using a {@link SolutionSetStream}. Either way,
 * it is then handed off synchronously using
 * {@link QueryEngine#acceptChunk(IChunkMessage)}. That method will queue the
 * chunk for asynchronous processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class StandaloneChunkHandler implements IChunkHandler {

    /**
     * Instance puts all chunks onto the native heap.
     * 
     * @see BLZG-533 Vector query engine on native heap.
     */
    public static final IChunkHandler NATIVE_HEAP_INSTANCE = new NativeHeapStandloneChunkHandler();
    
    /**
     * Instance puts all chunks onto the managed object heap.
     */
    public static final IChunkHandler MANAGED_HEAP_INSTANCE = new ManagedHeapStandloneChunkHandler();

    /**
     * This instance is explicitly used for several unit tests in the query
     * engine package that are not written to the RDF data model (the native
     * heap version assumes an RDF data model).
     */
    public static final IChunkHandler TEST_INSTANCE = new ManagedHeapStandloneChunkHandler();

    private final boolean nativeHeap;

    protected StandaloneChunkHandler(final boolean nativeHeap) {

        this.nativeHeap = nativeHeap;
        
    }
    
    @Override
    public int handleChunk(final IRunningQuery query, final int bopId,
            final int sinkId, final IBindingSet[] chunk) {

        if (query == null)
            throw new IllegalArgumentException();

        if (chunk == null)
            throw new IllegalArgumentException();

        if (chunk.length == 0)
            return 0;

        final IChunkMessage<IBindingSet> msg;
        
        if (nativeHeap) {

            // See BLZG-533: Vector the query engine on the native heap.
            msg = new LocalNativeChunkMessage(//
                    query.getQueryController(),//
                    query.getQueryId(),//
                    sinkId, // bopId
                    -1, // partitionId
                    query, // runningQuery,
                    chunk
                    );

        } else {

            // Store the chunk on the managed object heap.
            msg = new LocalChunkMessage(//
                query.getQueryController(), //
                query.getQueryId(),// 
                sinkId,// bopId
                -1, // partitionId
                chunk);

        }
        
        final QueryEngine queryEngine = query.getQueryEngine();

        queryEngine.acceptChunk(msg);

        return 1;

    }
    
}
