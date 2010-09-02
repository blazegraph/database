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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Abstract base class for pipelined operators regardless of the type of data
 * moving along the pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractPipelineOp<E> extends BOpBase implements
        PipelineOp<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {
        
    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    protected AbstractPipelineOp(final AbstractPipelineOp<E> op) {

        super(op);

    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    protected AbstractPipelineOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public int getChunkCapacity() {
        
        return getProperty(Annotations.CHUNK_CAPACITY,
                Annotations.DEFAULT_CHUNK_CAPACITY);

    }

    public int getChunkOfChunksCapacity() {

        return getProperty(Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

    }

    public long getChunkTimeout() {
        
        return getProperty(Annotations.CHUNK_TIMEOUT,
                Annotations.DEFAULT_CHUNK_TIMEOUT);
        
    }

    /**
     * The {@link TimeUnit}s in which the {@link #chunkTimeout} is measured.
     */
    protected static transient final TimeUnit chunkTimeoutUnit = TimeUnit.MILLISECONDS;

    public BOpStats newStats() {

        return new BOpStats();

    }

    public IBlockingBuffer<E[]> newBuffer() {

        return new BlockingBuffer<E[]>(getChunkOfChunksCapacity(),
                getChunkCapacity(), getChunkTimeout(), chunkTimeoutUnit);

    }

}
