/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.btree;

import java.util.concurrent.locks.Lock;

/**
 * Interface for managing read/write locks on persistence capable data
 * structures.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError: Child does
 *      not have persistent identity </a>
 */
public interface IReadWriteLockManager {

    /**
     * Return a {@link Lock} that may be used to obtain a shared read lock which
     * is used (in the absence of other concurrency control mechanisms) to
     * permit concurrent readers on an unisolated index while serializing access
     * to that index when a writer must run. This is exposed for processes which
     * need to obtain the write lock to coordinate external operations.
     * <p>
     * Note: If the persistence capable data structure is read-only then the
     * returned {@link Lock} is a singleton that ignores all lock requests. This
     * is because our read-only persistence capable data structures are already
     * thread-safe for concurrent readers.
     * 
     * @return The lock.
     */
    Lock readLock();

    /**
     * Return a {@link Lock} that may be used to obtain an exclusive write lock
     * which is used (in the absence of other concurrency control mechanisms) to
     * serialize all processes accessing an unisolated index when a writer must
     * run. This is exposed for processes which need to obtain the write lock to
     * coordinate external operations.
     * 
     * @return The lock.
     * 
     * @throws UnsupportedOperationException
     *             unless the view supports mutation.
     */
    Lock writeLock();

    /**
     * Return the #of read-locks held by the current thread for a mutable index
     * view.
     * 
     * @return The #of reentrant read locks held by the current thread -or- ZERO
     *         if the index is read-only (read locks are not tracked for a
     *         read-only index view).
     */
    int getReadLockCount();

    /**
     * Return <code>true</code> iff the data structure is read-only.
     */
    boolean isReadOnly();
    
}
