package com.bigdata.jini.start;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.DataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.AbstractServer.RemoteDestroyAdmin;
import com.bigdata.zookeeper.AbstractZNodeConditionWatcher;
import com.sun.jini.tool.ClassServer;

/**
 * Watcher that manages the logical service instances for a
 * {@link ServiceConfiguration} znode. If the #of logical servies falls beneath
 * the threshold specified by the {@link ServiceConfiguration#serviceCount} then
 * a new logical services will be created. The watcher notices events for create /
 * delete, change data (the target #of logical services may have been changed),
 * and change children (the #of deployed logical services may have been changed,
 * which is used to make sure that we have sufficient logical service
 * instances).
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
public class ServiceConfigurationWatcher extends
        AbstractZNodeConditionWatcher {

    protected static final Logger log = Logger
            .getLogger(ServiceConfigurationWatcher.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    protected final JiniFederation fed;

    protected final IServiceListener listener;

    public ServiceConfigurationWatcher(final JiniFederation fed,
            final IServiceListener listener, final String zpath) {

        super(fed.getZookeeper(), zpath);

        if (fed == null)
            throw new IllegalArgumentException();

        if (listener == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.listener = listener;
        
    }
    
    /**
     * Compares the current state of the {@link ServiceConfiguration} with the
     * #of logical services (its child znodes). If necessary, joins an election
     * to decide who gets to start/stop a logical service.
     * 
     * @param event
     * 
     * @return always returns <code>false</code> since we do not want to stop
     *         watching the {@link ServiceConfiguration} znode.
     */
    @Override
    protected boolean isConditionSatisified(WatchedEvent event)
            throws KeeperException, InterruptedException {

        switch (event.getType()) {

        case NodeDeleted:
            // nothing to do until someone recreates the znode.
            break;
        case NodeCreated:
        case NodeChildrenChanged:
        case NodeDataChanged:
            /*
             * @todo this could be optimized by considering the data already in
             * the znode and its children, but we need both on hand to make any
             * decisions so the event is just delegated.
             */
            isConditionSatisified();
            
        }
        
        return false;
        
    }

    /**
     * Compares the current state of the {@link ServiceConfiguration} with the
     * #of logical services (its child znodes). If necessary, joins an election
     * to decide who gets to start/stop a logical service.
     * 
     * @return always returns <code>false</code> since we do not want to stop
     *         watching the {@link ServiceConfiguration} znode.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    protected boolean isConditionSatisified() throws KeeperException,
            InterruptedException {

        /*
         * getData() : serviceConfig
         * 
         * getChildren() : #of logical services.
         * 
         * compare serviceCount to #of logical services. if too few, then create
         * one atomically (an election or lock).
         * 
         * for each logical service, verify that the replicationCount is correct
         * (but only once we support failover chains and we might put the
         * replicationCount into the logicalService data so that it is ready to
         * hand). for now, the winner of the election for creating a logical
         * service should also create the physical service.
         */

        // get the service configuration (and reset our watch).
        final ServiceConfiguration config = (ServiceConfiguration) SerializerUtil
                .deserialize(zookeeper.getData(zpath, this, new Stat()));

        // get children (and reset our watch).
        final List<String> children = zookeeper.getChildren(zpath, this);
        
        final int n = children.size();
        
        int cmp = config.serviceCount - n; 
        
        if (cmp > 0) {

            log.warn("under capacity: n=" + n + ", target="
                    + config.serviceCount + ", zpath=" + zpath);

            /*
             * FIXME all we need here is a lock. if the conditions are still
             * true once we hold a lock we can create one or more logical
             * services, triggering off elections to create their physical
             * services. The lock node should not be deleted until the logical
             * service has been created successfully so that any contenders for
             * the lock provide failover for the process holding the lock. When
             * the process obtains the lock, it MUST obtain the current
             * ServiceConfiguration and the current list of logical services. At
             * that point it may create (or destroy) one or more logical
             * services before releasing the lock. 
             * 
             * @todo anytime a child (a logical service) is created, all service
             * configuration watches need to see that ChildrenChanged event and
             * setup a watch on the logical service znode. that is how we
             * propagate the watches to ensure that services are created.
             */
            
        } else if (cmp < 0) {

            // @todo condition not handled.
            log.warn("under capacity: n=" + n + ", target="
                    + config.serviceCount + ", zpath=" + zpath);

//            List<ACL> acl = null;
//            new ZooElection(zookeeper, fed.getZooConfig().zroot
//                    + "/election_delete_" + config.className, acl).awaitWinner(
//                    5L, TimeUnit.SECONDS);
            
        }
        
        return false;
        
    }

    /** clears all watches used by this class. */
    protected void clearWatch() throws KeeperException, InterruptedException {

        zookeeper.exists(zpath, false);

        zookeeper.getData(zpath, false, new Stat());

    }

    /**
     * This will set all watches, but only if the client is connected.
     */
    public void start() throws KeeperException, InterruptedException {

        synchronized(this) {

            isConditionSatisified();
            
        }

    }

    /**
     * This will cancel all watches, but only if the client is connected.
     */
    public void cancel() throws KeeperException, InterruptedException {

        synchronized(this) {

            clearWatch();
            
        }

    }

}
