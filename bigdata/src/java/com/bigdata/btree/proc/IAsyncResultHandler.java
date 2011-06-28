/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 30, 2009
 */

package com.bigdata.btree.proc;

import com.bigdata.btree.keys.KVO;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.IAsynchronousWriteBufferFactory;
import com.bigdata.service.ndx.IScaleOutClientIndex;
import com.bigdata.service.ndx.pipeline.IndexPartitionWriteTask;

/**
 * Interface for chunk-at-a-time result processing for asynchronous index
 * writes.
 * <p>
 * Note: For backward compatibility both
 * {@link #aggregate(KVO[], Object, Split)} and
 * {@link IResultHandler#aggregate(Object, Split)} will be invoked by the
 * {@link IndexPartitionWriteTask}. In general, one of those methods should be
 * a NOP.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This interface was hacked in after the fact to support asynchronous
 *       writes.
 * 
 * @see KVO
 * @see IAsynchronousWriteBufferFactory
 */
public interface IAsyncResultHandler<R extends Object, A extends Object, O extends Object, X extends KVO<O>>
        extends IResultHandler<R, A> {

    /**
     * Method is invoked for each result and is responsible for combining the
     * results in whatever manner is meaningful for the procedure.
     * Implementations of this method MUST be <strong>thread-safe</strong>
     * since the procedure MAY be applied in parallel when it spans more than
     * one index partition.
     * 
     * @param chunk
     *            The {@link KVO}[] chunk for which the result was obtained.
     * @param result
     *            The result from applying the procedure to a single index
     *            partition.
     * @param split
     *            The {@link Split} that generated that result.
     */
    public void aggregateAsync(X[] chunk, R result, Split split);
    
}
