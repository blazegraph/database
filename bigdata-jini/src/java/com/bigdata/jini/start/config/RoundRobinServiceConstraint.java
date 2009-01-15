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
 * Created on Jan 12, 2009
 */

package com.bigdata.jini.start.config;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.jini.start.ServicesManagerServer;
import com.bigdata.zookeeper.ZLock;

/**
 * Imposes a round-robin constraint on new service allocation using a well known
 * {@link ZLock} for the federation. The constraint will succeed iff the
 * {@link ZLock} is held by this {@link ZooKeeper} client. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated I think that we may get the right behavior naturally and it is
 * difficult to construct otherwise.
 */
abstract public class RoundRobinServiceConstraint implements IServiceConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = -825934290822471678L;

    protected static final Logger log = Logger.getLogger(ServicesManagerServer.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * 
     */
    public RoundRobinServiceConstraint() {

    }

//    /**
//     * {@link ZLock}s are retained along as the zookeeper reference is valid.
//     * The zookeeper reference is linked to the {@link JiniFederation}, so this
//     * is really the same as the life cycle of the {@link JiniClient}.
//     */
//    static private Map<ZooKeeper, String> locks = new WeakHashMap<ZooKeeper, String>(); 
//    
//    synchronized public boolean allow(JiniFederation fed) throws Exception {
//        
//        if (fed == null) {
//
//            /*
//             * Note: This constraint is disabled (not applied) if the federation
//             * reference is not available.
//             */
//
//            return true;
//            
//        }
//        
//        final ZooKeeper zookeeper = fed.getZookeeper();
//        
//        final List<ACL> acl = fed.getZooConfig().acl;
//        
//        /*
//         * The lock node is this classname in the federation namespace.
//         * 
//         */
//        final String locknode = fed.getZooConfig().zroot + "/"
//                + BigdataZooDefs.LOCKS + "/" + this.getClass().getName();
//
//        String zpath ;
//        synchronized(locks) {
//            
//            zpath = locks.get(zookeeper);
//
//            if (zpath == null) {
//                
//                zpath = zookeeper.create(zpath, new byte[0], acl, CreateMode.EPHEMERAL);
//                
//                locks.put(zookeeper, zpath);
//                
//            }
//            
//        }
//
//    }
    
}
