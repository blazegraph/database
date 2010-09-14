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
 * Created on Sep 13, 2010
 */

package com.bigdata.bop.engine;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * API providing a variety of ways to access chunks of data (data are typically
 * elements or binding sets).
 * 
 * @todo Expose an {@link IChunkedIterator}, which handles both element at a
 *       time and chunk at a time.
 * 
 * @todo Expose a mechanism to visit the direct {@link ByteBuffer} slices in
 *       which the data are stored. For an operator which executes on a GPU, we
 *       want to transfer the data from the direct {@link ByteBuffer} in which
 *       it was received into a direct {@link ByteBuffer} which is a slice onto
 *       its VRAM. (And obviously we need to do the reverse with the outputs of
 *       a GPU operator).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IChunkAccessor<E> {

    /**
     * Visit the binding sets in the chunk.
     * 
     * @deprecated We do not need to use {@link IAsynchronousIterator} any more.
     *             This could be much more flexible and should be harmonized to
     *             support high volume operators, GPU operators, etc. probably
     *             the right thing to do is introduce another interface here
     *             with a getChunk():IChunk where IChunk let's you access the
     *             chunks data in different ways (and chunks can be both
     *             {@link IBindingSet}[]s and element[]s so we might need to
     *             raise that into the interfaces and/or generics as well).
     * 
     * @todo It is likely that we can convert to the use of
     *       {@link BlockingQueue} instead of {@link BlockingBuffer} in the
     *       operators and then handle the logic for combining chunks inside of
     *       the {@link QueryEngine}. E.g., by scanning this list for chunks for
     *       the same bopId and combining them logically into a single chunk.
     *       <p>
     *       For scale-out, chunk combination will naturally occur when the node
     *       on which the operator will run requests the {@link ByteBuffer}s
     *       from the source nodes. Those will get wrapped up logically into a
     *       source for processing. For selective operators, those chunks can be
     *       combined before we execute the operator. For unselective operators,
     *       we are going to run over all the data anyway.
     */
    IAsynchronousIterator<E[]> iterator();

//    /**
//     * Chunked iterator pattern. The iterator may be used for element at a time
//     * processing, but the underlying iterator operators in chunks. The size of
//     * the chunks depends originally on the data producer, but smaller chunks
//     * may be automatically combined into larger chunks both during production
//     * and when data are buffered, whether to get them off of the heap or to
//     * transfer them among nodes.
//     * 
//     * @return
//     */
//    IChunkedIterator<E> chunkedIterator();
    
}
