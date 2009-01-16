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

package com.bigdata.zookeeper;

import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.service.jini.JiniFederation;

/**
 * Implementation using {@link ZLock}s. This is purely a gloss on the
 * {@link ZLock}. The interesting behavior is handled by zookeeper.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class ZooResourceLockService implements IResourceLockService {

    private final JiniFederation fed;

    public ZooResourceLockService(final JiniFederation fed) {

        if (fed == null)
            throw new IllegalArgumentException();

        this.fed = fed;
        
    }

    public IResourceLock acquireLock(final String namespace) {

        try {
            
            return acquireLock(namespace, Long.MAX_VALUE);
            
        } catch (InterruptedException t) {
            
            throw new RuntimeException(t);
            
        }
        
    }

    public IResourceLock acquireLock(final String namespace,
            final long timeout) throws InterruptedException {
    
        try {
            
            final String zpath = fed.getZooConfig().zroot
                    + BigdataZooDefs.LOCKS_RESOURCES + "/" + namespace;
            
            final ZLock zlock = ZNodeLockWatcher.getLock(fed.getZookeeper(), zpath,
                    fed.getZooConfig().acl);
            
            zlock.lock();
            
            return new IResourceLock() {

                public void unlock() {
                    
                    try {
                        zlock.unlock();
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                    
                }
                
            };
            
        } catch (Throwable t) {
         
            throw new RuntimeException(namespace, t);
            
        }

    }

}
