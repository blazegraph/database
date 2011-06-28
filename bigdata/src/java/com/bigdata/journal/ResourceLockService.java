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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import com.bigdata.concurrent.NamedLock;

/**
 * An implementation using {@link NamedLock}s suitable for within JVM locking.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final
public class ResourceLockService implements IResourceLockService {
    
    /**
     * Table of locks. Locks are weakly held. 
     * 
     * @todo could use simple locks in weak value hash map with the name as the
     *       key.
     */
    private final NamedLock<String/* namespace */> locks = new NamedLock<String>();

    public ResourceLockService() {

    }

    public IResourceLock acquireLock(final String namespace) {

        final Lock lock = locks.acquireLock(namespace);

        return new ResourceLock(lock);

    }

    public IResourceLock acquireLock(final String namespace,
            final long timeout) throws InterruptedException, TimeoutException {

        final Lock lock = locks.acquireLock(namespace, timeout,
                TimeUnit.MILLISECONDS);

        return new ResourceLock(lock);

    }
    
    /**
     * Inner class (NOT serializable).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static final class ResourceLock implements IResourceLock {
        
        private final Lock lock;

        /**
         * 
         * @param lock
         *            The lock (must have been acquired by the caller).
         */
        protected ResourceLock(final Lock lock) {

            if (lock == null)
                throw new IllegalArgumentException();

            this.lock = lock;

        }

        public void unlock() {

            lock.unlock();

        }

    }
    
}
