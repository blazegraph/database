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
 * Created on Jul 10, 2008
 */

package com.bigdata.journal;

import java.util.concurrent.TimeoutException;

import com.bigdata.relation.AbstractResource;

/**
 * Interface named synchronous distributed locks without deadlock detection.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Two features need to be teased apart. On the one hand there are full
 * tx, which provide deadlock detection and could support hierarchical locking
 * (with changes to the lock manager to decouple threads from lock queues, the
 * imposition of a path syntax on the resource names, and perhaps the
 * introduction of 2PL lock modes {S,I,X}). This lock manager could then be
 * exposed, providing applications with the possibility of 2PL, but bigdata does
 * not require that in and of itself (it might be of interest for others).
 * <p>
 * The other feature is distributed locks without deadlock detection. This is
 * the kind of lock that you get with zookeeper. When running in a single JVM
 * java.util.concurrent.locks provides this kind of locking. These locks are
 * good for coordinating actions such as who is the master in some computation.
 * <p>
 * This second role (global synchronous locks without deadlock detection) can be
 * realized by a client library. When running in a single JVM, the
 * java.util.concurrent.locks package can provide the implementation. When
 * running in a distributed environment, zookeeper can provide the
 * implementation.
 * 
 * FIXME The main use of this interface today is creating and destroying
 * relations, relation containers, and their indices "atomically". Unisolated
 * operations CAN NOT be used to create more than a single resource atomically.
 * A full tx should be used instead. The place where this happens is
 * {@link AbstractResource#acquireExclusiveLock()}. That is invoked only when
 * creating or destroying a resource. All callers should be using a full tx
 * instead when creating or destroying a resource!
 */
public interface IResourceLockService {

    /**
     * Acquire an exclusive lock on the named resource. The request will block
     * until the lock is available.
     * 
     * @param namespace
     *            The named resource.
     * 
     * @return The lock.
     * 
     * @throws RuntimeException
     *             wrapping {@link InterruptedException} or
     *             {@link TimeoutException}
     */
    public IResourceLock acquireLock(String namespace);

    /**
     * Acquire an exclusive lock the named resource. The request will block
     * until the lock is available or the timeout expires.
     * 
     * @param namespace
     *            The named resource.
     * @param timeout
     *            Timeout in milliseconds for the request -or-
     *            {@link Long#MAX_VALUE} to wait forever.
     * 
     * @return The lock.
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public IResourceLock acquireLock(String namespace, long timeout)
            throws InterruptedException, TimeoutException;

}
