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
 * Created on Sep 3, 2010
 */

package com.bigdata.bop;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * An pipeline operator reads from a source and writes on a sink.
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPipelineOp<E> extends BOp {

    /**
     * Return a new object which can be used to collect statistics on the
     * operator evaluation (this may be overridden to return a more specific
     * class depending on the operator).
     */
    BOpStats newStats();

    /**
     * Instantiate a buffer suitable as a sink for this operator. The buffer
     * will be provisioned based on the operator annotations.
     * <p>
     * Note: if the operation swallows binding sets from the pipeline (such as
     * operators which write on the database) then the operator MAY return an
     * immutable empty buffer.
     * 
     * @param stats
     *            The statistics on this object will automatically be updated as
     *            elements and chunks are output onto the returned buffer.
     * 
     * @return The buffer.
     */
    IBlockingBuffer<E[]> newBuffer(BOpStats stats);

    /**
     * Return a {@link FutureTask} which computes the operator against the
     * evaluation context. The caller is responsible for executing the
     * {@link FutureTask} (this gives them the ability to hook the completion of
     * the computation).
     * 
     * @param context
     *            The evaluation context.
     * 
     * @return The {@link FutureTask} which will compute the operator's
     *         evaluation.
     * 
     * @todo Modify to return a {@link Callable}s for now since we must run each
     *       task in its own thread until Java7 gives us fork/join pools and
     *       asynchronous file I/O.  For the fork/join model we will probably
     *       return the ForkJoinTask.
     */
    FutureTask<Void> eval(BOpContext<E> context);
    
}
