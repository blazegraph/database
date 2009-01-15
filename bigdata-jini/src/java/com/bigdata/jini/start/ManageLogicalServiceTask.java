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
import org.apache.zookeeper.data.ACL;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.zookeeper.ZLock;
import com.sun.jini.tool.ClassServer;

/**
 * Task makes adjusts an imbalance between the serviceCount and the #of logical
 * services (creating or destroying a logical service) and then exits.
 * 
 * @todo If the task hangs then the {@link ZLock} must be broken so that another
 *       service can give it a go.
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
 *       {@link DataService} may be the only one that already supports "peers"
 *       (but not failover).
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
            List<String> children, V config) {
     
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
     * Create zpath for the new logical service, create its direct children (the
     * {@link BigdataZooDefs#PHYSICAL_SERVICES_CONTAINER} and
     * {@link BigdataZooDefs#MASTER_ELECTION}), and create the
     * {@link BigdataZooDefs#LOCKS_CREATE_PHYSICAL_SERVICE} znode, whose data
     * contains the zpath of the new logical service.
     * <p>
     * The creation of the {@link BigdataZooDefs#LOCKS_CREATE_PHYSICAL_SERVICE}
     * child trigger watchers looking for to contend for the right to create a
     * physical service which is an instance of that logical service. All
     * service managers contend for that lock. If the winner can satisify the
     * constraints for that service type (including considering its recent
     * service load, RAM, disk, etc), and is able to successfully create the
     * service, then it destroys the lock node. Otherwise it releases the lock
     * and sleeps a bit. Either it or the other service managers will give it a
     * try. This will continue until the requirements change or the service is
     * successfully created somewhere.
     * <p>
     * Note that this does not find the "best" host for the new service since
     * there is no global consideration of host scores. However, the load
     * balancer will adjust the load on the {@link IDataService}s which are the
     * most heavily loaded part of the system.
     * <p>
     * Note: The {@link ServicesManagerServer} is responsible for watching the
     * {@link BigdataZooDefs#LOCKS_CREATE_PHYSICAL_SERVICE} znode. It does that
     * using a {@link MonitorCreatePhysicalServiceLocksTask} task.
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

        final List<ACL> acl = fed.getZooConfig().acl;
        
        log.warn("serviceCount=" + config.serviceCount + ", actual="
                + children.size() + ", configZPath=" + configZPath);

        /*
         * Create zpath for the new logical service.
         */
        final String logicalServiceZPath = zookeeper.create(configZPath + "/"
                + BigdataZooDefs.LOGICAL_SERVICE_PREFIX, new byte[0], acl,
                CreateMode.PERSISTENT_SEQUENTIAL);

        /*
         * The new znode (last path component of the new zpath).
         */
        final String logicalServiceZNode = logicalServiceZPath
                .substring(logicalServiceZPath.lastIndexOf('/') + 1);
        
        /*
         * Create the znode that is the parent for the physical service
         * instances (direct child of the logicalSevice znode).
         */
        zookeeper.create(logicalServiceZPath + "/"
                + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER, new byte[0], acl,
                CreateMode.PERSISTENT);

        /*
         * Create the znode for the election of the primary physical service for
         * this logical service (direct child of the logicalSevice znode).
         */
        zookeeper.create(logicalServiceZPath + "/"
                + BigdataZooDefs.MASTER_ELECTION, new byte[0], acl,
                CreateMode.PERSISTENT);

        try {

            /*
             * Create the znode used to decide the host on which the new
             * physical service will be created.
             * 
             * Note: The data is the zpath to the logicalService.
             * 
             * Note: MonitorCreatePhysicalServiceLocksTasks will notice the
             * create of this lock node, contend for the zlock, and create a new
             * service instance if it gains the lock, if it can satisify the
             * constraints for the new physical service on the local host, and
             * we are still short at least one physical service for this logical
             * service when it checks. If successful, it will ZLock#destroy()
             * the lock node. Otherwise it releases the lock and either it or
             * another services manager will give it a go when they gain the
             * lock.
             */

            final String lockNodeZPath = fed.getZooConfig().zroot + "/"
                    + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE + "/"
                    + config.className + "_" + logicalServiceZNode;

            zookeeper
                    .create(lockNodeZPath, SerializerUtil
                            .serialize(logicalServiceZPath), acl,
                            CreateMode.PERSISTENT);

            if (INFO)
                log.info("Created lock node: " + lockNodeZPath);

        } catch (NodeExistsException ex) {

            // ignore.
            
        }

    }

    /**
     * Destroy a logical service (must destroy the physical services first,
     * which is complex for data services since index partitions must be shed).
     * 
     * FIXME {@link #destroyLogicalService()} not implemented yet.
     */
    protected void destroyLogicalService() {

        log.warn("Operation not supported: serviceCount=" + config.serviceCount
                + ", actual=" + children.size() + ", zpath=" + configZPath);

    }

}
