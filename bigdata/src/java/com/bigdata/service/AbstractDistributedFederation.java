/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.Future;

import com.bigdata.io.IStreamSerializer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;

/**
 * Abstract base class for {@link IBigdataFederation} implementations where the
 * services are distributed using RMI and are running, at least in principle,
 * across more than one host/JVM.
 * 
 * @todo Explore a variety of cached and uncached strategies for the metadata
 *       index. An uncached strategy is currently used. However, caching may be
 *       necessary for some kinds of application profiles, especially as the #of
 *       index partitions grows. If an application performs only unisolated and
 *       read-committed operations, then a single metadata index cache can be
 *       shared by the client for all operations against a given scale-out
 *       index. On the other hand, a client that uses transactions or performs
 *       historical reads will need to have a view of the metadata index as of
 *       the timestamp associated with the transaction or historical read.
 * 
 * @todo support failover metadata service discovery.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the client or service.
 */
abstract public class AbstractDistributedFederation<T> extends AbstractScaleOutFederation<T> {

    public AbstractDistributedFederation(final IBigdataClient<T> client) {

        super(client);

    }

    final public boolean isDistributed() {
        
        return true;
        
    }
    
    /**
     * Assumes that the federation is stable through failover services if
     * nothing else.
     */
    public boolean isStable() {

        return true;
        
    }

    /**
     * Return a proxy object for an {@link IAsynchronousIterator} suiteable for
     * use in an RMI environment.
     * 
     * @param src
     *            The source iterator. Note that the iterator normally visits
     *            elements of some array type (chunks).
     * @param serializer
     *            The object responsible for (de-)serializing a chunk of
     *            elements visited by the iterator.
     * @param capacity
     *            The capacity for the internal buffer that is used to
     *            asynchronously transfer elements (chunks) from the remote
     *            iterator to the client iterator.
     * 
     * @return Either a thick iterator (when the results would fit within a
     *         single chunk) or a thin iterator that uses RMI to fetch chunks
     *         from the remote {@link IAsynchronousIterator}.
     * 
     * @throws IllegalArgumentException
     *             if the iterator is <code>null</code>.
     */
    public abstract <E> IAsynchronousIterator<E> getProxy(
            IAsynchronousIterator<E> src,//
            IStreamSerializer<E> serializer, //
            int capacity
    );

    /**
     * Return a proxy object for a {@link Future} suitable for use in an RMI
     * environment.
     * 
     * @param future
     *            The future.
     * 
     * @return The proxy for that future.
     */
    public abstract <E> Future<E> getProxy(Future<E> future);

    /**
     * Return a proxy object for an {@link IBuffer} suitable for use in an RMI
     * environment.
     * 
     * @param buffer
     *            The future.
     * 
     * @return A proxy for that {@link IBuffer} that masquerades any RMI
     *         exceptions.
     */
    public abstract <E> IBuffer<E> getProxy(final IBuffer<E> buffer);
    
    /**
     * Return a proxy for an object.
     * 
     * @param obj
     *            The object.
     * @param enableDGC
     *            If distributed garbage collection should be used for the
     *            object.
     *            
     * @return The proxy.
     */
    public abstract <E> E getProxy(E obj, boolean enableDGC);
    
}
