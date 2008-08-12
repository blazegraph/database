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

import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * An interface that provides an iterator to drain the {@link IBuffer}. This
 * interface is useful where one (or more) processes will write asynchronously
 * on the {@link IBuffer} while another drains it via the {@link #iterator()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBlockingBuffer<E> extends IBuffer<E> {

    /**
     * Return an iterator reading from the buffer. The elements will be visited
     * in the order in which they were written on the buffer.
     * 
     * @return The iterator.
     */
    public IChunkedOrderedIterator<E> iterator();
    
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
