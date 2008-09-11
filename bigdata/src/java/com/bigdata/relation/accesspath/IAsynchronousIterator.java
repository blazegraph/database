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

import java.util.Iterator;
import java.util.concurrent.Future;

import com.bigdata.striterator.ICloseableIterator;

/**
 * Interface for iterators that are running asynchronously.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAsynchronousIterator<E> extends ICloseableIterator<E> {

    /**
     * True if the {@link Future} that is writing on the {@link IBlockingBuffer}
     * has been set and is done. This indicates that all results are
     * materialized on the queue and will be visited without waiting on the
     * {@link Future}. It does NOT indicate whether or not the {@link Future}
     * completely successfully. This method may be used to decide whether or not
     * {@link Iterator#hasNext()} would block. When this method returns
     * <code>true</code> {@link Iterator#hasNext()} should not block because
     * no more elements will be added to the {@link BlockingBuffer}.
     * 
     * @return <code>true</code> iff the producer is done.
     */
    public boolean isFutureDone();
    
//    /**
//     * Wait up to timeout units for the next element.
//     * 
//     * @param timeout
//     *            The timeout.
//     * @param unit
//     *            The unit for that timeout.
//     * 
//     * @return The next chunk.
//     * 
//     * @throws NoSuchElementException
//     *             if there are no more elements available.
//     */
//    public E next(long timeout, TimeUnit unit);

}
