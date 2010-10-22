/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;

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
    
    public int handleChunk(final RunningQuery query, final int bopId,
            final int sinkId, final IBindingSet[] chunk) {

        if (query == null)
            throw new IllegalArgumentException();

        if (chunk == null)
            throw new IllegalArgumentException();

        if (chunk.length == 0)
            return 0;
        
        final LocalChunkMessage<IBindingSet> msg = new LocalChunkMessage<IBindingSet>(
                query.getQueryController(), query.getQueryId(), sinkId,
                -1/* partitionId */,
                new ThickAsynchronousIterator<IBindingSet[]>(
                        new IBindingSet[][] { chunk }));

        query.getQueryEngine().acceptChunk(msg);

        return 1;

    }

}
