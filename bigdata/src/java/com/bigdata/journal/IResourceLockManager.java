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

import com.bigdata.concurrent.TimeoutException;

/**
 * Interface for a low latency global lock service. Resources are identified by
 * a namespace and locks are granted for a <em>resource hierarchy</em>.The
 * resource hierarchy is defined as all strings having the namespace as a
 * prefix. A lock is either <em>exclusive</em> or <em>shared</em>. A exclusive
 * lock is exclusive of all other locks on the resource hierarchy, including
 * shared locks and other exclusive locks. Exclusive locks are normally used to
 * create or destroy a resource hierarchy. A shared lock simply prevents a
 * process from acquiring an exclusive lock and thereby ensures that the
 * resource hierarchy remains available.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IResourceLockManager {

    /**
     * Acquire an exclusive lock on a resource hierarchy. The request will block
     * until the lock is available.
     * 
     * @param namespace
     *            Identifies the resource hierarchy to be locked.
     * 
     * @return The lock.
     * 
     * @throws RuntimeException
     *             wrapping {@link InterruptedException} or
     *             {@link TimeoutException}
     */
    public IResourceLock acquireExclusiveLock(String namespace);

    /**
     * Acquire an exclusive lock on a resource hierarchy. The request will block
     * until the lock is available or the timeout expires.
     * 
     * @param namespace
     *            Identifies the resource hierarchy to be locked.
     * @param timeout
     *            Timeout in milliseconds for the request -or-
     *            {@link Long#MAX_VALUE} to wait forever.
     * 
     * @return The lock.
     * 
     * @throws RuntimeException
     *             wrapping {@link InterruptedException} or
     *             {@link TimeoutException}
     */
    public IResourceLock acquireExclusiveLock(String namespace, long timeout);

    /**
     * Acquire a shared lock on a resource hierarchy (permits concurrent read and/or write operations on
     * the resource). The request will block until the lock is available.
     * 
     * @param namespace
     *            Identifies the resource hierarchy to be locked.
     * 
     * @return The lock.
     * 
     * @throws RuntimeException
     *             wrapping {@link InterruptedException}
     */
    public IResourceLock acquireSharedLock(String namespace);

    /**
     * Acquire a shared lock on a resource hierarchy (permits concurrent read
     * and/or write operations on the resource). The request will block until
     * the lock is available or the timeout expires.
     * 
     * @param namespace
     *            Identifies the resource hierarchy to be locked.
     * @param timeout
     *            Timeout in milliseconds for the request -or-
     *            {@link Long#MAX_VALUE} to wait forever.
     * 
     * @return The lock.
     * 
     * @throws RuntimeException
     *             wrapping {@link InterruptedException} or
     *             {@link TimeoutException}
     */
    public IResourceLock acquireSharedLock(String namespace, long timeout);

}
