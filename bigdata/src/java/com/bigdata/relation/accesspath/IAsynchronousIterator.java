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
 * Created on Aug 27, 2008
 */

package com.bigdata.relation.accesspath;

import java.util.concurrent.TimeUnit;

import com.bigdata.relation.rule.IQueryOptions;
import com.bigdata.relation.rule.IRule;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Interface for iterators that are running asynchronously. Such iterators are
 * the consumer side of a producer - consumer pattern. The producer writes on an
 * {@link IBlockingBuffer} which is drained by the {@link IAsynchronousIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BlockingBuffer
 */
public interface IAsynchronousIterator<E> extends ICloseableIterator<E> {

//    /**
//     * Return <code>true</code> if the {@link Future} that is writing on the
//     * {@link IBlockingBuffer} has been set and is done. This indicates that all
//     * results are materialized on the queue and will be visited without waiting
//     * on the {@link Future}. It does NOT indicate whether or not the
//     * {@link Future} completely successfully. This method may be used to decide
//     * whether or not {@link Iterator#hasNext()} would block. When this method
//     * returns <code>true</code> {@link Iterator#hasNext()} should not block
//     * because no more elements will be added to the {@link BlockingBuffer}.
//     * 
//     * @return <code>true</code> iff the producer is done.
//     */
//    public boolean isFutureDone();

    /**
     * Return <code>true</code> iff this iterator will not visit any more
     * elements (non-blocking).
     * 
     * @return <code>true</code> iff the iterator is known to be exhausted
     *         without blocking. A <code>false</code> return does NOT imply
     *         that the iterator will visit more elements, only that it MIGHT
     *         visit more elements.
     */
    public boolean isExhausted();

    /**
     * Return <code>true</code> iff there is at least one element that can be
     * visited. If the buffer is empty then this will block until: (a) an
     * element appears in the buffer; (b) the buffer is
     * {@link BlockingBuffer#close()}ed; or (c) the timeout expires.
     * <p>
     * Note that a <code>false</code> return DOES NOT signify that the iterator
     * is exhausted. However, if you specify an infinite timeout using
     * {@link Long#MAX_VALUE} {@link TimeUnit#SECONDS} then you MAY safely
     * interpret a <code>false</code> return as an indication that the iterator
     * is exhausted.
     * 
     * @param timeout
     *            The length of time that the method may block awaiting an
     *            element to appear.
     * @param unit
     *            The units in which the <i>timeout</i> is expressed.
     * 
     * @return <code>true</code> iff there is an element available.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted while waiting another
     *             element.
     */
    public boolean hasNext(final long timeout, final TimeUnit unit)
            throws InterruptedException;
    
    /**
     * Waits up to the <i>timeout</i> to return the next element. When the
     * generic type of the elements in the buffer is an array type and there is
     * more than one element available within the specified time, then multiple
     * elements in the buffer MAY be combined into a single element and that
     * element returned to the caller.
     * 
     * @param timeout
     *            The timeout (overrides the <i>chunkTimeout</i> specified to
     *            the {@link BlockingBuffer} ctor).
     * @param unit
     *            The unit in which the timeout is expressed.
     * 
     * @return The element -or- <code>null</code> iff the timeout was exceeded
     *         before {@link #hasNext(long, TimeUnit)} returned
     *         <code>true</code>.
     *         <p>
     *         In order to have a definitive indication that the iterator is
     *         exhausted you need to invoke {@link #hasNext()} without a timeout
     *         (blocking) or {@link #isExhausted()} (non-blocking).
     *         
     * @throws InterruptedException
     *             if the current thread is interrupted while waiting for
     *             another element.
     */
    public E next(long timeout, TimeUnit unit) throws InterruptedException;
    
    /**
     * Notes that the iterator is closed and hence may no longer be read. It is
     * safe to invoke this method even if the iterator is closed. However, if
     * the producer is still running then it will be <em>canceled</em>
     * (interrupted) in order to prevent the {@link IBlockingBuffer} from
     * filling up and deadlocking. For this reason, {@link #close()} has
     * consequences NOT entailed by {@link ICloseableIterator}.
     * <p>
     * Note: Depending on the semantics of the producer, it MAY choose to treat
     * an interrupt() as normal (but eager) termination. For example, rule
     * execution treats an interrupt() as normal (but eager) termination with
     * the consequence that queries may be safely interrupted once some limit
     * has been satisfied. However, the preferred way to treat LIMIT is using
     * {@link IRule} with an {@link IQueryOptions} that specifies a LIMIT.
     */
    public void close();
    
}
