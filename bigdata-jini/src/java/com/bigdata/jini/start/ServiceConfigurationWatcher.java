package com.bigdata.jini.start;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.AbstractServer.RemoteAdministrable;

/**
 * Watcher that manages the logical service instances for a
 * {@link ServiceConfiguration} znode.
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
 * </pre>
 * 
 * Each configuration node defines the service type, the target #of service
 * instances, the replication count, etc for a service. The children of the
 * configuration node are the logical service instances and use
 * {@link CreateMode#PERSISTENT_SEQUENTIAL}.
 * 
 * The children of a logical service are the actual service instances. Those
 * nodes use {@link CreateMode#EPHEMERAL_SEQUENTIAL} so that zookeeper will
 * remove them if the client dies. The ordered set of instances for a logical
 * service is in effect an election, and the winner is the current primary for
 * that service (for services which have a primary rather than just peers).
 * 
 * Each {@link ServicesManager} sets a {@link Watcher} for the each service
 * configuration node. This allows it to observe changes in the target
 * serviceCount for a given service type and the target replicationCount for a
 * logical service of that type. If the #of logical services is under the target
 * count, then we need to create a new logical service instance (just a znode)
 * and set a watch on it (@todo anyone can create the new logical service since
 * it only involves assigning a UUID, but we need an election or a lock to
 * decide who actually does it so that we don't get a flood of logical services
 * created. All watchers need to set a watch on the new logical service once it
 * is created.) [note: we need the logical / physical distinction for services
 * such as jini which are peers even before we introduce replication for bigdata
 * services.]
 * 
 * The {@link ServicesManager} also sets a {@link Watcher} for each logical
 * service of any service type. This allows it to observe the join and leave of
 * physical service instances. If it observes that a logical service is under
 * the target replication count (which either be read from the configuration
 * node which is the parent of that logical service or must be copied onto the
 * logical service when it is created) AND the host satisifies the
 * {@link IServiceConstraint}s, then it joins a priority election of ephemeral
 * nodes (@todo where) and waits for up to a timeout for N peers to join. The
 * winner of the election is the {@link ServicesManager} on the host best suited
 * to start the new service instance and it starts an instance of the service on
 * the host where it is running. (@todo after the new service starts, the
 * logical service node will gain a new child (in the last position). that will
 * trigger the watch. if the logical service is still under the target, then the
 * process will repeat.)
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
 * @todo <strong>Destroying a service instance is dangerous - if it is not
 *       replicated then you can loose all your data!</strong>. In order to
 *       destroy a specific service instance you have to use the
 *       {@link RemoteAdministrable} API or zap it from the command line (that
 *       will only take the service down, but not destroy its data). However, if
 *       the federation is now undercapacity for that service type (or under the
 *       replication count for a service instance), then a new instance will
 *       simply be created.
 *       <p>
 *       Note: management semantics for deleting znodes are not yet defined.
 *       Right now, nothing happens if you remove the znode for a logical
 *       service (well, this class will no longer manage the #of running
 *       instances of that service) or a physical service instance.
 * 
 * @todo the service should clean up its state in zookeeper as part of its
 *       shutdown or destroy protocol. that will happen automatically if the
 *       service instance uses the EPHEMERAL flag for its znode (no children
 *       will be allowed).
 * 
 * @todo If the target local data service count is reduced below the #of logical
 *       data services then we need to identify a logical data service to
 *       shutdown (probably one that is lightly used) and shed all index
 *       partitions for that data service before it is shutdown, otherwise the
 *       data would be lost.
 * 
 * @todo The federation can be destroyed using {@link JiniFederation#destroy()}.
 *       (Modify this to to delete the zroot, which gives us an ACL for
 *       destroying the federation).
 * 
 * @todo edit javadoc and put it where?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceConfigurationWatcher implements Watcher {

    protected static final Logger log = Logger.getLogger(ServiceConfigurationWatcher.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    protected final JiniFederation fed;

    protected final IServiceListener listener;
    
    public ServiceConfigurationWatcher(final JiniFederation fed,
            final IServiceListener listener) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (listener == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.listener = listener;
        
    }
    
    /**
     * @see https://deployutil.dev.java.net/
     * 
     * FIXME using zookeeper, contend for a lock on the root node for the
     * bigdata federation instance. If there is no such node, then create,
     * populate it with the federation configuration, and set a watch.
     * 
     * FIXME read the configuration from zookeeper for bigdata federation
     * instance. for each node corresponding to a service having a non-zero
     * requirement (target - actual as discovered using jini), and for which
     * the max instances of that service per host has not been exceeded by
     * this host, create an instance of that service running on this host.
     * 
     * @todo start httpd for downloadable code. (contend for lock on node,
     *       start instance if insufficient instances are running). The
     *       codebase URI should be the concatenation of the URIs for each
     *       httpd instance that has been configured. Unlike some other
     *       configuration properties, I am not sure that the codebase URI
     *       can be changed once a service has been started. We will have to
     *       unpack all of the classes into the file system, and then
     *       possibly create a single JAR from them, and expose that use the
     *       ClassServer. This should be done BEFORE starting jini since
     *       jini can then recognize our services in the service browser
     *       (the codebase URI needs to be set for that to work).
     * 
     * @todo start jini using configured RMI codebase URO (contend for lock
     *       on node, start instance if insufficient instances are running).
     *       jini should be started iff one of the locators corresponds to
     *       an address for this host and jini is not found to be running.
     *       We will need some sort of lock to resolve contention concerning
     *       which instance of this class should start jini. That can be
     *       done using zookeeper.
     * 
     * @todo Do the txService. NO dependencies (well, jini must be running
     *       for it to be discoverable).
     * 
     * @todo Do the resource lock server. NO dependencies (jini must be
     *       running for it to be discoverable). For the short term, uses
     *       zookeeper to provide non-hierarchical locks w/o deadlock
     *       detection. Will be replaced by full transactions. A client
     *       library can provide queues and barriers using zookeeper.
     * 
     * @todo Do the LBS.
     * 
     * @todo Do the MDS.
     * 
     * @todo Do the data services.
     */
    public void process(final WatchedEvent event) {

        if(INFO)
            log.info(event.toString());
        
        final String zpath = event.getPath();
        
        final ZooKeeper zookeeper = fed.getZookeeper();

        try {

            // get the service configuration (and reset our watch).
            final ServiceConfiguration config = (ServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(zpath, this, new Stat()));

            /*
             * Verify that we could start this service.
             * 
             * @todo add load-based constraints, e.g., can not start a new
             * service if heavily swapping, near limits on RAM or on disk.
             */
            for (IServiceConstraint constraint : config.constraints) {

                if (!constraint.allow(fed)) {

                    if (INFO)
                        log.info("Constraint(s) do not allow service start: "
                                        + config);
                    
                    return;
                    
                }
                
            }
            
            /*
             * FIXME Examine priority queue to figure out which
             * ServicesManager is best suited to start the service instance.
             * Only ServiceManagers which satisify the constraints are
             * allowed to participate, so we need to make that an election
             * for the specific service, give everyone some time to process
             * the event, and then see who is the best candidate to start
             * the service.
             * 
             * @todo does this design violate zookeeper design principles?
             * Especially, we can't wait for asynchronous events so the
             * election will have to be on a created and the ephemeral
             * sequential joins with the election will have to wait for the
             * timeout or for the last to join (satisified the N vs timeout
             * criterion) to write the winner into the data.
             * 
             * Or, how about if we have different standing elections
             * corresponding to different constraints and have the hosts
             * update each election every 60 seconds with their current
             * priority (if it has changed significantly).  The problem
             * with this is that a host can't allow replicated instances
             * of a service onto the same host.
             */
            
            // get task to start the service.
            final Callable task = config.newServiceStarter(fed,
                    listener, zpath);
            
            /*
             * Submit the task.
             * 
             * The service will either start or fail to start. We don't
             * check the future since all watcher events are serialized.
             */
            fed.getExecutorService().submit(task)
            .get()// forces immediate start of the service.
            ;
            
        } catch (Throwable e) {

            log.error("zpath=" + zpath, e);

            throw new RuntimeException(e);

        }

    }

}