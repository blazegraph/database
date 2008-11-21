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

package com.bigdata.service.proxy;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Remote;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.pipeline.JoinTask;
import com.bigdata.striterator.ICloseableIterator;

/**
 * {@link Remote} interface declaring the API of {@link IAsynchronousIterator}
 * but also declaring that each methods throws {@link IOException} in order to
 * be compatible with {@link Remote} and {@link Exporter}. Of course, this
 * means that this interface can not extend {@link IAsynchronousIterator}!
 * <p>
 * Note: In practice, {@link IAsynchronousIterator}s are declared with an array
 * type. There are two main uses: transferring {@link IBindingSet}[]s between
 * {@link JoinTask}s and transferring {@link ISolution}[]s from the last join
 * dimension back to the client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements visited by the source iterator.
 */
public interface RemoteAsynchronousIterator<E> extends Remote {

    /**
     * @see Iterator#hasNext()
     */
    boolean hasNext() throws IOException;

    /**
     * @see Iterator#next()
     */
    E next() throws IOException;

    /**
     * Method applies a custom serializer to the value returned by
     * {@link #next()} and returns a {@link RemoteElement} that can be
     * transported by RMI and which knows how to de-serialize the element on the
     * other end.
     * 
     * @return 
     */
    RemoteElement<E> nextElement() throws IOException;
    
    /**
     * @see Iterator#remove()
     */
    void remove() throws IOException;

    /**
     * @see ICloseableIterator#close()
     */
    void close() throws IOException;

    /**
     * @see IAsynchronousIterator#isExhausted()
     */
    boolean isExhausted() throws IOException;

    /**
     * @see IAsynchronousIterator#hasNext(long, TimeUnit)
     */
    boolean hasNext(final long timeout, final TimeUnit unit)
            throws IOException;

    /**
     * @see IAsynchronousIterator#next(long, TimeUnit)
     */
    E next(long timeout, TimeUnit unit) throws IOException;
    
    /**
     * Used to send an serialized element together with the object that knows
     * how to de-serialize the element.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    interface RemoteElement<E> extends Serializable {
    
        /**
         * Return the de-serialized element.
         * 
         * @return The de-serialized element.
         */
        public E get();
        
    }
    
}
