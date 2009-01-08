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
 * Created on Jan 8, 2009
 */

package com.bigdata.jini.start;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.DataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.AbstractServer.RemoteDestroyAdmin;
import com.bigdata.zookeeper.ZLock;
import com.sun.jini.tool.ClassServer;

/**
 * Task makes adjusts an imbalance between the serviceCount and the #of logical
 * services (creating or destroying a logical service) and then exits.
 * 
 * @todo If the task hangs then the {@link ZLock} must be broken so that another
 *       service can give it a go.
 * 
 * @todo for create, all watches on the {@link ServiceConfiguration} znode must
 *       now set a watcher to maintain the logical service replication count by
 *       starting and stopping physical services as necessary.
 *       <p>
 * @todo lots for destroy, but also release any watches.
 * 
 * @todo No mechanism is currently defined to reduce the #of logical services
 *       and there are a variety of issues to be considered.
 *       <p>
 *       For example, if the target logical data service count is reduced below
 *       the actual #of logical data services then we need to identify a logical
 *       data service to shutdown (probably one that is lightly used) and shed
 *       all index partitions for that data service before it is shutdown,
 *       otherwise the data would be lost.
 *       <p>
 *       However, some kinds of services do not pose any such problem. For
 *       example, it should be trivial to reduce the #of jini registrars that
 *       are running or the #of {@link ClassServer}s.
 *       <p>
 *       In order to destroy a logical service, first set the #of replicas to
 *       zero so that the physical instances will be destroyed (using the
 *       {@link RemoteDestroyAdmin} and any other APIs required to insure that
 *       the total system state is preserved). Then delete the logical service
 *       node.
 * 
 * @todo Make sure the {@link MetadataService}, the LBS, and the transaction
 *       server DO NOT allow more than one logical instance in a federation.
 *       they can (eventually) have failover instances, but not peers. The
 *       {@link DataService} may be the only one that already supports "peers".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ManageLogicalServiceTask<V extends ServiceConfiguration>
        implements Callable {

    protected static final Logger log = Logger
            .getLogger(ManageLogicalServiceTask.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    protected final JiniFederation fed;
    protected final IServiceListener listener;
    protected final String configZPath;
    protected final List<String> children;
    protected final V config;

    /**
     * 
     * @param fed
     * @param listener
     * @param configZPath
     * @param children
     * @param config
     */
    public ManageLogicalServiceTask(JiniFederation fed,
            IServiceListener listener, String configZPath,
            List<String> children,V config) {
     
        this.fed = fed;
        this.listener = listener;
        this.configZPath = configZPath;
        this.children = children;
        this.config = config;
        
    }

    public Object call() throws Exception {

        /*
         * compare serviceCount to #of logical services. if too few, then create
         * one (already holding a lock).
         * 
         * for each logical service, verify that the replicationCount is correct
         * (but only once we support failover chains and we might put the
         * replicationCount into the logicalService data so that it is ready to
         * hand). for now, the winner of the election for creating a logical
         * service should also create the physical service.
         */
        
        final int n = children.size();
        
        if (config.serviceCount > n) {

            newLogicalService();
            
        } else if (config.serviceCount < n) {

            destroyLogicalService();

        } else {

            if (INFO)
                log.info("No action required: zpath=" + configZPath);

        }

        return null;

    }

    /**
     * Create zpath for the new logical service, the "/election" znode under the
     * logical service (basically a lock node used for primary elections where
     * the winner is the master and the queue order is the failover chain), and
     * the {@link BigdataZooDefs#LOCKS_CREATE_PHYSICAL_SERVICE} znode, whose
     * data contains the zpath of the new logical service.
     * <p>
     * The creation of the {@link BigdataZooDefs#LOCKS_CREATE_PHYSICAL_SERVICE}
     * child trigger watchers looking for to contend for the right to create a
     * physical service which is an instance of that logical service. If the
     * watcher can create an instance of that service type (including
     * considering its recent service load, RAM, disk, etc), then it contends
     * for the lock. The winner creates the physical service, deletes the
     * remaining children in the queue, and deletes the lock node. That is the
     * end of the competition.
     * <p>
     * Note that this does not find the "best" host for the new service since
     * there is no global consideration of host scores. However, only hosts that
     * are "good enough" can compete.
     * <p>
     * Note: The {@link ServicesManagerServer} is responsible for watching the
     * {@link BigdataZooDefs#LOCKS_CREATE_PHYSICAL_SERVICE} znode. It does that
     * using a {@link MonitorCreatePhysicalServiceLocks} task.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * 
     * @todo One problem is that a lot of services could be created in
     *       succession on a new host. That kind of flooding might overwhelm a
     *       newly joined host. This could be dealt with introducing a delay
     *       before the host will contend for another lock. Also, a host that is
     *       getting a lot of action could just release the lock when it finds
     *       itself the winner. Failover to the remaining hosts will occur
     *       naturally.
     */
    protected void newLogicalService() throws KeeperException, InterruptedException {

        final ZooKeeper zookeeper = fed.getZookeeper();

        final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;// @todo acl
        
        log.warn("serviceCount=" + config.serviceCount + ", actual="
                + children.size() + ", configZPath=" + configZPath);

        // create zpath for the new logical service.
        final String logicalServiceZPath = zookeeper.create(configZPath
                + "/logicalService", SerializerUtil.serialize(config), acl,
                CreateMode.PERSISTENT_SEQUENTIAL);

        try {

            /*
             * Create the znode for the election of the primary physical service for
             * this logical service.
             */
            
            zookeeper.create(logicalServiceZPath + "/election", SerializerUtil
                    .serialize(config), acl, CreateMode.PERSISTENT);
            
        } catch (NodeExistsException ex) {
            
            // ignore.
            
        }

        try {

            /*
             * Create the znode used to decide the host on which the new
             * physical service will be created.
             */
            
            zookeeper.create(fed.getZooConfig().zroot + "/"
                    + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE + "/lock",
                    SerializerUtil.serialize(logicalServiceZPath), acl,
                    CreateMode.PERSISTENT_SEQUENTIAL);
            
        } catch (NodeExistsException ex) {
            
            // ignore.
            
        }

    }

    /**
     * Destroy a logical service (must destroy the physical services first).
     * 
     * FIXME not supported yet.
     */
    protected void destroyLogicalService() {

        throw new UnsupportedOperationException("serviceCount="
                + config.serviceCount + ", actual=" + children.size()
                + ", zpath=" + configZPath);

    }

}
