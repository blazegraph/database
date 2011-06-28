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

import java.util.concurrent.TimeUnit;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Extended to use the {@link BufferAnnotations} to provision the
 * {@link BlockingBuffer} and to track the {@link BOpStats} as chunks are added
 * to the buffer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BlockingBufferWithStats.java 3838 2010-10-22 19:45:33Z
 *          thompsonbry $
 * 
 * @todo replace with {@link OutputStatsBuffer}? (It is still used by the
 *       {@link ChunkedRunningQuery} and by the query output buffer.)
 */
public class BlockingBufferWithStats<E> extends BlockingBuffer<E> {

    private final BOpStats stats;
    
    public BlockingBufferWithStats(final PipelineOp op, final BOpStats stats) {

        super(op.getChunkOfChunksCapacity(), op.getChunkCapacity(), op
                .getChunkTimeout(), BufferAnnotations.chunkTimeoutUnit);

        this.stats = stats;

    }

    /**
     * Overridden to track {@link BOpStats#unitsOut} and
     * {@link BOpStats#chunksOut}.
     * <p>
     * Note: {@link BOpStats#chunksOut} will report the #of chunks added to this
     * buffer. However, the buffer MAY combine chunks either on add() or when
     * drained by the iterator so the actual #of chunks read back from the
     * iterator MAY differ.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public boolean add(final E e, final long timeout, final TimeUnit unit)
            throws InterruptedException {

        final boolean ret = super.add(e, timeout, unit);

        if (e.getClass().getComponentType() != null) {

            stats.unitsOut.add(((Object[]) e).length);

        } else {

            stats.unitsOut.increment();

        }

        stats.chunksOut.increment();

        return ret;

    }

}
