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

/**
 * Implementation supports a standalone database. The generated chunk is left on
 * the Java heap and handed off synchronously using
 * {@link QueryEngine#acceptChunk(IChunkMessage)}. That method will queue the
 * chunk for asynchronous processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StandaloneChunkHandler implements IChunkHandler {

    public static final IChunkHandler INSTANCE = new StandaloneChunkHandler();
    
    public int handleChunk(final IRunningQuery query, final int bopId,
            final int sinkId, final IBindingSet[] chunk) {

        if (query == null)
            throw new IllegalArgumentException();

        if (chunk == null)
            throw new IllegalArgumentException();

        if (chunk.length == 0)
            return 0;
        
        final LocalChunkMessage msg = new LocalChunkMessage(
                query.getQueryController(), //
                query.getQueryId(),// 
                sinkId,// bopId
                -1, // partitionId
                chunk);

        final QueryEngine queryEngine = query.getQueryEngine();

        queryEngine.acceptChunk(msg);

        return 1;

    }

}
