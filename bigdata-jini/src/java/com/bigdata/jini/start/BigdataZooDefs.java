package com.bigdata.jini.start;

import java.util.UUID;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.jini.start.config.IServiceConstraint;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.service.AbstractService;
import com.bigdata.service.jini.JiniUtil;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.zookeeper.ZLock;

/**
 * Interface declaring constants that are used to name various znodes of
 * interest. The basic layout within zookeeper is as follows:
 * 
 * <pre>
 * 
 * zroot-for-federation
 *     / config 
 *              / jini {ServiceConfiguration}
 *                  ...
 *              / ClassServer {JavaConfiguration}
 *                  ...
 *              / TransactionServer {BigdataServiceConfiguration}
 *                  ...
 *              / MetadataServer
 *                  ...
 *              / DataServer {ServiceConfiguration}
 *                      / logicalService1 {logicalServiceUUID, params}
 *                                The order of the children determines
 *                                replication chain and new primary if the
 *                                master fails.
 *                          / physicalService1 {serviceUUID, host}
 *                          / physicalService2 {serviceUUID, host}
 *                          ...
 *                      / logicalService2 {logicalServiceUUID, params}
 *                  ...
 *              / LoadBalancerServer
 *                  ...
 *              / ResourceLockServer
 *                  ...
 *     / locks
 *          namespace used for some kinds of locks.
 *          / createPhysicalService (namespace monitored by service starters)
 * </pre> *
 * <p>
 * Each {@link ServiceConfiguration} znode defines the service type, the target
 * #of service instances, the replication count, etc for a service. The children
 * of the configuration node are the logical service instances and use
 * {@link CreateMode#PERSISTENT_SEQUENTIAL}.
 * <p>
 * The children of a logical service are the actual service instances. Those
 * nodes use {@link CreateMode#EPHEMERAL} so that zookeeper will remove them if
 * the client dies. The bigdata services DO NOT use the SEQUENTIAL flag since
 * they need to be able to restart with the save physical service znode.
 * Instead, they create the physical service znode using the {@link ServiceID}
 * assigned by jini. Since the physical services are NOT sequentially generated,
 * we maintain a {@link ZLock} for the logical service. Physical services
 * contend for that lock and whichever one holds the lock is the primary. The
 * order of the services in the lock queue is the failover order for the
 * secondaries.
 * <p>
 * A {@link ServiceConfigurationZNodeMonitorTask} is run for the each discovered
 * {@link ServiceConfiguration} znode and sets a
 * {@link ServiceConfigurationWatcher} on that znode. This allows it to observe
 * changes in the target serviceCount for a given service type and the target
 * replicationCount for a logical service of that type. If the #of logical
 * services is under the target count, then we need to create a new logical
 * service instance (just a znode) and set a watch on it (@todo anyone can
 * create the new logical service since it only involves assigning a UUID, but
 * we need an election or a lock to decide who actually does it so that we don't
 * get a flood of logical services created. All watchers need to set a watch on
 * the new logical service once it is created.) [note: we need the logical /
 * physical distinction for services such as jini which are peers even before we
 * introduce replication for bigdata services.]
 * 
 * The {@link ServicesManagerServer} also sets a {@link Watcher} for each
 * logical service of any service type. This allows it to observe the join and
 * leave of physical service instances. If it observes that a logical service is
 * under the target replication count (which either be read from the
 * configuration node which is the parent of that logical service or must be
 * copied onto the logical service when it is created) AND the host satisifies
 * the {@link IServiceConstraint}s, then it joins a priority election of
 * ephemeral nodes (@todo where) and waits for up to a timeout for N peers to
 * join. The winner of the election is the {@link ServicesManagerServer} on the
 * host best suited to start the new service instance and it starts an instance
 * of the service on the host where it is running. (@todo after the new service
 * starts, the logical service node will gain a new child (in the last
 * position). that will trigger the watch. if the logical service is still under
 * the target, then the process will repeat.)
 * 
 * @todo Replicated bigdata services are created under a parent having an
 *       instance number assigned by zookeeper. The parent corresponds to a
 *       logical service. It is assigned a UUID for compatibility with the
 *       existing APIs (which do not really support replication). The children
 *       of the logical service node are the znodes for the physical instances
 *       of that logical service.
 * 
 * Each physical service instance has a service UUID, which is assigned by jini
 * sometime after it starts and then recorded in the zookeeper znode for that
 * physical service instance.
 * 
 * The physical service instances use an election to determine which of them is
 * the primary, which are the secondaries, and the order for replicating data to
 * the secondaries.
 * 
 * The services create an EPHERMERAL znode when they (re)start. That znode
 * contains the service UUID and is deleted automatically by zookeeper when the
 * service's {@link ZooKeeper} client is closed. The services with persistent
 * state DO NOT use the SEQUENTIAL flag since the same znode MUST be re-created
 * if the service is restarted. Also, since the znodes are EPHEMERAL, no
 * children are allowed. Therefore all behavior performed by the services occurs
 * in queues, elections and other high-level data control structures using
 * znodes elsewhere in zookeeper.
 * 
 * FIXME javadoc edit.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BigdataZooDefs {

    /**
     * The slash character used as zpath component delimiter.
     */
    String ZSLASH = "/";
    
    /**
     * The name of the child of the bigdata federation root zpath whose
     * children are the {@link ServiceConfiguration} znodes.
     */
    String CONFIG = "config";

    /**
     * The zname of the child of the zroot where we put all of our lock nodes.
     */
    String LOCKS = "locks";
    
    /**
     * Relative path to a child of the zroot that is watched by the
     * {@link ServicesManagerService}s. Any time a new lock is created under
     * this znode, running {@link ServicesManagerServer} will contend for that
     * lock if they can satisify the {@link IServiceConstraint}s for the new
     * service. The lock node data itself contains the zpath to the
     * logicalService for which a new physicalService must be created. The
     * {@link ServiceConfiguration} is fetched from that zpath and gives the
     * {@link IServiceConstraint}s that must be satisified.
     */
    String LOCKS_CREATE_PHYSICAL_SERVICE = LOCKS + ZSLASH
            + "createPhysicalService";
    
    /**
     * The relative zpath to a node whose children are lock nodes. The children
     * are named for the service type, e.g., {@link TransactionServer}. The
     * task holding the {@link ZLock} is the one that handles state changes for
     * {@link ServiceConfiguration} znode, including its children, which are the
     * logical service instance znodes. Each {@link ServicesManagerServer}
     * creates a {@link ServiceConfigurationZNodeMonitorTask} that contends for
     * the locks which are the children of this znode.
     */
    String LOCKS_SERVICE_CONFIG_MONITOR = LOCKS + ZSLASH
            + "serviceConfigMonitor";
    
    /**
     * The prefix for the name of a znode that represents a logical service.
     * This znode is a {@link CreateMode#PERSISTENT_SEQUENTIAL} child of the
     * {@link #CONFIG} znode.
     */
    String LOGICAL_SERVICE = "logicalService";

    /**
     * The prefix for the name of a znode that represents a physical service.
     * The actual znode of a physical service is formed by appending the service
     * {@link UUID}. Jini will assign a {@link ServiceID} when the service is
     * first registered and is available from {@link ServiceItem#serviceID}.
     * The {@link ServiceID} can be converted to a normal {@link UUID} using
     * {@link JiniUtil#serviceID2UUID(ServiceID)}. You can also request the
     * service {@link UUID} using the service proxy.
     * 
     * @see AbstractService#getServiceUUID()
     * 
     * @todo the httpd URL could be written into this znode.
     */
    String PHYSICAL_SERVICE = "physicalService";
    
}
