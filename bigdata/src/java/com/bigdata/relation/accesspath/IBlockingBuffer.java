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
 * Created on Jun 26, 2008
 */

package com.bigdata.relation.accesspath;

import java.util.concurrent.Future;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.striterator.IChunkedIterator;

/**
 * <p>
 * Interface provides an iterator to drain chunks from an {@link IBuffer}.
 * </p>
 * <h2>CONOPS</h2>
 * <p>
 * This interface is useful where one (or more) processes will write
 * asynchronously on the {@link IBuffer} while another drains it via the
 * {@link #iterator()}. For better performance in a multi-threaded environment,
 * each thread is given an {@link UnsynchronizedArrayBuffer} of some capacity.
 * The threads populate their {@link UnsynchronizedArrayBuffer}s in parallel
 * using non-blocking operations. The {@link UnsynchronizedArrayBuffer}s in
 * turn are configured to flush <em>chunks</em> of elements onto an either an
 * {@link IBuffer} whose generic type is <code>E[]</code>. Each element in
 * the target {@link IBuffer} is therefore a chunk of elements from one of the
 * source {@link UnsynchronizedArrayBuffer}s.
 * </p>
 * <p>
 * There are two families of synchronized {@link IBuffer}s
 * </p>
 * <ol>
 * <li>An {@link IBuffer} that targets a mutable {@link IRelation};</li>
 * and
 * <li>An {@link IBlockingBuffer} that exposes an {@link IAsynchronousIterator}
 * for reading chunks of elements.</li>
 * </ol>
 * <p>
 * This design means that blocking operations are restricted to chunk-at-a-time
 * operations, primarily when an {@link UnsynchronizedArrayBuffer}<code>&lt;E&gt;</code>
 * overflows onto an {@link IBuffer}<<code>&lt;E[]&gt;</code> and when the
 * {@link IBuffer}<<code>&lt;E[]&gt;</code> either is flushed onto an
 * {@link IMutableRelation} or drained by an {@link IChunkedIterator}.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements in the chunks.
 */
public interface IBlockingBuffer<E> extends IBuffer<E> {

    /**
     * Return an iterator reading from the buffer. It is NOT safe for concurrent
     * processes to consume the iterator. The iterator will visit elements in
     * the order in which they were written on the buffer, but note that the
     * elements may be written onto the {@link IBlockingBuffer} by concurrent
     * processes in which case the order is not predictable without additional
     * synchronization.
     * 
     * @return The iterator.
     */
    public IAsynchronousIterator<E> iterator();

    /**
     * Add an element to the buffer.
     * <p>
     * Note: This method is constrained to throw the specified exception if the
     * buffer has been {@link #close()}d.
     * 
     * @param e
     *            The element
     * 
     * @throws BufferClosedException
     *             if the buffer has been {@link #close()}d.
     */
    public void add(E e);
    
    /**
     * Return <code>true</code> if the buffer is open.
     */
    public boolean isOpen();
    
    /**
     * Signal that no more data will be written on this buffer (this is required
     * in order for the {@link #iterator()} to know when no more data will be
     * made available).
     */
    public void close();

    /**
     * Signal abnormal termination of the process writing on the buffer. The
     * buffer will be closed. The {@link #iterator()} will report the <i>cause</i>
     * via a wrapped exception the next time any method on its interface is
     * invoked. The internal queue may be cleared once this method is invoked.
     * 
     * @param cause
     *            The exception thrown by the processing writing on the buffer.
     */
    public void abort(Throwable cause);

    /**
     * This is a NOP since the {@link #iterator()} is the only way to consume
     * data written on the buffer.
     * 
     * @return ZERO (0L)
     */
    public long flush();
    
    /**
     * Set the {@link Future} for the source processing writing on the
     * {@link IBlockingBuffer}.
     * 
     * @param future
     *            The {@link Future}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the future has already been set.
     */
    public void setFuture(Future future);
    
}
