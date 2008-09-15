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

import com.bigdata.io.ISerializer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.striterator.IRemoteChunkedIterator;

/**
 * This class encapsulates access to the services for a remote bigdata
 * federation - it is in effect a proxy object for the distributed set of
 * services that comprise the federation.
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
 */
abstract public class AbstractDistributedFederation extends AbstractScaleOutFederation {

    public AbstractDistributedFederation(IBigdataClient client) {

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
     * Return a proxy object for an iterator suiteable for use in an RMI
     * environment.
     * <p>
     * Note: The method MAY either return an {@link IRemoteChunkedIterator} -or-
     * return a "thick" iterator that fully buffers the results. A "thick"
     * iterator is generally better if the results would fit within a single
     * "chunk" since you avoid additional RMI calls.
     * <p>
     * Note: The elements visited by the source iterator are an array type. Each
     * visited element corresponds to a single chunk of elements of the
     * component type of the array.
     * 
     * @param itr
     *            The source iterator. Note that the iterator visits elements of
     *            some array type (chunks).
     * @param serializer
     *            The object responsible for (de-)serializing a chunk of
     *            elements visited by the iterator.
     * @param keyOrder
     *            The natural order in which the elements will be visited iff
     *            known and otherwise <code>null</code>.
     * 
     * @return Either a thick iterator (when the results would fit within a
     *         single chunk) or an {@link IRemoteChunkedIterator} (when multiple
     *         chunks are expected).
     * 
     * @throws IllegalArgumentException
     *             if the iterator is <code>null</code>.
     */
    public abstract Object getProxy(
            IAsynchronousIterator<? extends Object[]> itr,//
            ISerializer<? extends Object[]> serializer, //
            IKeyOrder<? extends Object> keyOrder//
    );

}
