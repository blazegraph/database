package com.bigdata.jini.start;

import java.util.UUID;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.jini.start.config.IServiceConstraint;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.service.jini.util.JiniUtil;
import com.bigdata.zookeeper.DumpZookeeper;
import com.bigdata.zookeeper.ZLock;

/**
 * Interface declaring constants that are used to name various znodes of
 * interest. A {@link DumpZookeeper} sample output shows the layout of the
 * znodes of interest within the federation.
 * 
 * <pre>
 * test-fed 
 *   locks 
 *     serviceConfigMonitor 
 *       com.bigdata.service.jini.DataServer 
 *         lock0000000000 (Ephemeral) 
 *       com.bigdata.service.jini.LoadBalancerServer 
 *         lock0000000000 (Ephemeral) 
 *       com.bigdata.service.jini.MetadataServer 
 *         lock0000000000 (Ephemeral) 
 *       com.bigdata.service.jini.TransactionServer 
 *         lock0000000000 (Ephemeral) 
 *     createPhysicalService 
 *   config 
 *     com.bigdata.service.jini.DataServer {DataServiceConfiguration}
 *       logicalService0000000001 
 *         election 
 *         physicalServiceea8d5902-c7b1-4dc2-9f01-dd343aed8230 (Ephemeral) {UUID}
 *       logicalService0000000000 
 *         election 
 *         physicalServiceb2bf8b98-da0c-42f5-ac65-027bf3304429 (Ephemeral) {UUID}
 *     com.bigdata.service.jini.LoadBalancerServer {LoadBalancerServiceConfiguration}
 *       logicalService0000000000 
 *         election 
 *         physicalService911a9b28-7396-4932-ab80-77078119e7e2 (Ephemeral) {UUID}
 *     com.bigdata.service.jini.MetadataServer {MetadataServiceConfiguration}
 *       logicalService0000000000 
 *         election 
 *         physicalServicec0f35d2e-0a20-40c4-bb76-c97e7cb72eb3 (Ephemeral) {UUID}
 *     com.bigdata.service.jini.TransactionServer {TransactionServiceConfiguration}
 *       logicalService0000000000 
 *         election 
 *         physicalService87522080-2da6-42be-84a8-4a863b420042 (Ephemeral) {UUID}
 *   services
 *       com.bigdata.service.jini.TransactionServer 
 *          instances (persistent znodes)
 *       com.bigdata.service.jini.LoadBalancerServer
 *           instances (persistent znodes)
 *       com.bigdata.service.jini.MetadataServer
 *          instances (persistent znodes) 
 *       com.bigdata.service.jini.DataServerServer
 *          instances (persistent znodes) 
 * </pre>
 * 
 * Each {@link ServiceConfiguration} znode defines the service type, the target
 * #of service instances, the replication count, etc for a service. The children
 * of the configuration node are the logicalService instances and use
 * {@link CreateMode#PERSISTENT_SEQUENTIAL}.
 * <p>
 * The children of a logicalService are "election" and "physicalServices". The
 * <code>election</code> znode is a lock node that is used for master election
 * and determining the failover chain for services which support failover. The
 * children of the <code>physicalServices</code> {@link CreateMode#EPHEMERAL}
 * znodes representing the physical service instances. Since they are ephemeral,
 * zookeeper will remove them if the physical service dies.
 * <p>
 * Note: The bigdata services DO NOT use the SEQUENTIAL flag since they need to
 * be able to restart with the save physical service znode. Instead, they create
 * the physicalService znode using the {@link ServiceID} assigned by jini. Since
 * the physical services are NOT sequentially generated, we maintain a
 * {@link ZLock} for the logical service. Physical services contend for that
 * lock and whichever one holds the lock is the primary. The order of the
 * services in the lock queue is the failover order for the secondaries.
 * <p>
 * A {@link ServiceConfigurationZNodeMonitorTask} is run for the each discovered
 * {@link ServiceConfiguration} znode and maintains watches on the dominated
 * logical services znodes and physical service znodes. This allows it to
 * observe changes in the target serviceCount for a given service type and the
 * target replicationCount for a logical service of that type. If the #of
 * logical services is under the target count, then we need to create a new
 * logical service instance (just a znode) and set a watch on it (@todo anyone
 * can create the new logical service since it only involves assigning a UUID,
 * but we need an election or a lock to decide who actually does it so that we
 * don't get a flood of logical services created. All watchers need to set a
 * watch on the new logical service once it is created.) [note: we need the
 * logical / physical distinction for services such as jini which are peers even
 * before we introduce replication for bigdata services.]
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
     * The zname of the child of the zroot where we put all the state for
     * distributed jobs.
     */
    String JOBS = "jobs";
    
    /**
     * Relative path to a child of the zroot that is a container for lock nodes.
     * There is at most one such lock node for each logical service instance of
     * each service type. The lock nodes are named using the
     * {@link ServiceConfiguration#className} followed by an underscore ("_")
     * followed by the znode of the logical service instance. For example,
     * 
     * <pre>
     * com.bigdata.service.jini.DataServer_logicalService0000000000
     * </pre>
     * 
     * Each such znode is a lock node. Its children represent processes
     * contending for that lock. The lock node data is the zpath to the
     * logicalService for which a new physicalService must be created. The
     * {@link ServiceConfiguration} is fetched from that zpath and gives the
     * {@link IServiceConstraint}s that must be satisified.
     * <p>
     * The {@link LOCKS_CREATE_PHYSICAL_SERVICE} container is watched by the
     * {@link ServicesManagerService} using a
     * {@link MonitorCreatePhysicalServiceLocksTask}. Any time a new lock node
     * is created in the container, the
     * {@link MonitorCreatePhysicalServiceLocksTask} will contend for that lock.
     * If it obtains the lock and can satisfy the {@link IServiceConstraint}s
     * for the new service, then it will attempt to create an instance of that
     * service. If successful, it {@link ZLock#destroyLock()}s the lock node.
     * Otherwise it releases the {@link ZLock} and sleeps a bit. Either it or
     * another {@link ServicesManagerServer} will try again once they gain the
     * {@link ZLock}.
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
     * The relative zpath to a node whose children are {@link ZLock} acquired
     * using the {@link IResourceLockService}.
     */
    String LOCKS_RESOURCES = LOCKS + "/" + "resources";
    
    /**
     * The prefix for the name of a znode that represents a logical service.
     * This znode is a {@link CreateMode#PERSISTENT_SEQUENTIAL} child of the
     * {@link #CONFIG} znode. This znode should have two children:
     * {@link #PHYSICAL_SERVICES_CONTAINER} and {@link #MASTER_ELECTION}.
     */
    String LOGICAL_SERVICE_PREFIX = "logicalService";

    /**
     * The name of the znodes whose children represent the physical service
     * instances.
     * <p>
     * Most bigdata services are persistent. Each service has a persistent
     * identifier and persistent state. The persistent identifier is its
     * {@link ServiceID}, which is assigned by jini. The persistent state of a
     * bigdata service is stored in its service specific data directory. In
     * order to support restart, persistent services are represented by
     * PERSISTENT znodes. The data in the znode is the
     * {@link ServiceConfiguration} used to (re-)start the service. When the
     * service is shutdown, its ephemeral znodes (used for master election,
     * etc.) will be destroyed, but its persistent znode and the
     * {@link ServiceConfiguration} required for restart will remain.
     * <p>
     * Non-persistent services MUST use an EPHEMERAL znode which will be
     * automatically deleted once zookeeper notices a timeout for that service.
     * <p>
     * For both persistent and non-persistent services, the znode of a physical
     * service is named by its service {@link UUID}. Jini assigns a
     * {@link ServiceID} when the service is first registered. Persistent
     * services store their {@link ServiceID} in their data directory. The
     * {@link ServiceID} for any jini service is available from
     * {@link ServiceItem#serviceID}.
     * <p>
     * The bigdata APIs use a service {@link UUID} in order to decouple the core
     * modules from jini. The {@link ServiceID} may be converted to a service
     * {@link UUID} using {@link JiniUtil#serviceID2UUID(ServiceID)}.
     */
    String PHYSICAL_SERVICES_CONTAINER = "physicalServices";

    /**
     * The name of the znode that is a child of {@link #LOGICAL_SERVICE_PREFIX}
     * serving as a lock node for the election of the master physical service
     * for that logical service.
     * 
     * @see ZLock
     */
    String MASTER_ELECTION = "masterElection";

}
