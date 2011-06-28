package com.bigdata.jini.start;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.InnerCause;

/**
 * Task restarts persistent physical services that should be running on this
 * host but which are not discoverable using jini (not found when we query for
 * their {@link ServiceID}) and apparently disconnected from zookeeper (their
 * ephemeral znode is not found in the {@link BigdataZooDefs#MASTER_ELECTION}
 * container). Service restarts are logged. An error is logged if a service can
 * not be restarted. It is an error if the {@link ZooKeeper} client is not
 * connected. It is an error if no {@link ServiceRegistrar}s are joined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RestartPersistentServices implements Callable<Boolean> {

    protected static final Logger log = Logger.getLogger(RestartPersistentServices.class);
    
    /**
     * A fatal error (jini registrars not joined, zookeeper client is
     * disconnected, etc).
     */
    static transient final String ERR_WILL_NOT_RESTART_SERVICES = "Will not restart services";
    
    protected final JiniFederation fed;
    
//    protected final ZooKeeper zookeeper;
    
    final MonitorCreatePhysicalServiceLocksTask monitorCreatePhysicalServiceLocksTask;
    
    public RestartPersistentServices(
            final JiniFederation fed,
            final MonitorCreatePhysicalServiceLocksTask monitorCreatePhysicalServiceLocksTask) {
        
        if(fed == null)
            throw new IllegalArgumentException();
    
        if(monitorCreatePhysicalServiceLocksTask == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
//        this.zookeeper = fed.getZookeeper();
        
        this.monitorCreatePhysicalServiceLocksTask = monitorCreatePhysicalServiceLocksTask;
        
    }
    
    /**
     * Runs until all services are running.
     */
    public Boolean call() throws Exception {

        if(log.isInfoEnabled())
            log.info("Running.");

        while (true) {

            try {

                return runOnce();

            } catch(Throwable t) {

                if(InnerCause.isInnerCause(t, InterruptedException.class)) {
                
                    if(log.isInfoEnabled())
                        log.info("Interrupted");
                    
                    throw new RuntimeException(t);
                    
                }
                
                log.error(this, t);

                /*
                 * @todo A tight loop can appear here if the znodes for the
                 * federation are destroyed. This shows up as a NoNodeException
                 * thrown out of getZLock(), which indicates that zroot/locks
                 * does not exist so the lock node can not be created.
                 * 
                 * A timeout introduces a delay which reduces the problem. This
                 * can arise if the federation is being destroyed -or- if this
                 * is a new federation but we are not yet connected to a
                 * zookeeper ensemble so the znodes for the federation do not
                 * yet exist.
                 */

                Thread.sleep(2000/* ms */);
                
            }
            
        }
        
    }
    
    private boolean runOnce() throws KeeperException, InterruptedException {
        
//        /*
//         * Make sure that the zookeeper client is connected (of course it could
//         * disconnect at any time).
//         */
//        {
//            final ZooKeeper.States state = zookeeper.getState();
//            switch (state) {
//            default:
//                log.error(ERR_WILL_NOT_RESTART_SERVICES
//                        + " : zookeeper not connected: state=" + state);
//                return null;
//            case CONNECTED:
//                break;
//            }
//        }

        // Obtain valid zk connection.
        final ZooKeeper zookeeper = fed.getZookeeper();
        
        // Make sure that we are joined with at least one jini registrar.
        if(!fed.awaitJiniRegistrars(Long.MAX_VALUE, TimeUnit.SECONDS)) {
        
//        if (fed.getDiscoveryManagement().getRegistrars().length == 0) {
            
            log.error(ERR_WILL_NOT_RESTART_SERVICES
                    + " : not joined with any service registrars.");
            
            return false;
            
        }
        
        // root znode for the federation.
        final String zroot = fed.getZooConfig().zroot;

        // znode for configuration metadata.
        final String zconfig = zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;

        // these are the ServiceConfigurations.
        final List<String> serviceConfigZNodes;
        try {
            
            serviceConfigZNodes = zookeeper.getChildren(zconfig, false);
            
        } catch (NoNodeException ex) {

            log.error(ERR_WILL_NOT_RESTART_SERVICES
                    + " : configuration znode not found: " + zconfig);

            return false;
            
        }

        if (log.isInfoEnabled())
			log.info("Considering " + serviceConfigZNodes.size()
					+ " service configurations");
        
        for (String serviceConfigZNode : serviceConfigZNodes) {

            if (log.isInfoEnabled())
				log.info("Considering service configuration: "
						+ serviceConfigZNode);
            
            /*
             * Get the service configuration. We will need it iff we restart an
             * instance of this service type.
             */
            final ManagedServiceConfiguration serviceConfig = (ManagedServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(zconfig + "/"
                            + serviceConfigZNode, false, new Stat()));

            final String serviceConfigZPath = zconfig + "/"
                    + serviceConfigZNode;

            /*
             * The children are the logical service instances for that service
             * type.
             */
            final List<String> logicalServiceZNodes = zookeeper.getChildren(
                    serviceConfigZPath, false);

            if (log.isInfoEnabled())
				log.info("Considering " + logicalServiceZNodes.size()
						+ " logical services configurations for "
						+ serviceConfigZNode);

            for (String logicalServiceZNode : logicalServiceZNodes) {

                final String logicalServiceZPath = serviceConfigZPath + "/"
                        + logicalServiceZNode;

                final String physicalServicesContainerZPath = logicalServiceZPath
                        + "/" + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER;
                
                /*
                 * The children are the physical service instances for that
                 * logical service.
                 */
                
                final List<String> physicalServiceZNodes = zookeeper
                        .getChildren(physicalServicesContainerZPath,
                                false);

                if (log.isInfoEnabled())
					log
							.info("Considering " + physicalServiceZNodes.size()
									+ " physical services configurations for "
									+ logicalServiceZNode + " of "
									+ serviceConfigZNode);
                
                for (String physicalServiceZNode : physicalServiceZNodes) {

                    final String physicalServiceZPath = physicalServicesContainerZPath
                            + "/" + physicalServiceZNode;

                    final Entry[] attributes = (Entry[]) SerializerUtil
                            .deserialize(zookeeper.getData(
                                    physicalServiceZPath, false, new Stat()));

                    if (log.isInfoEnabled())
                        log.info("Considering: "
                                + physicalServicesContainerZPath);
                    
                    monitorCreatePhysicalServiceLocksTask.restartIfNotRunning(
                            serviceConfig, logicalServiceZPath,
                            physicalServiceZPath, attributes);
                    
                }

            }

        }

        // Success.
        return true;

    }
    
}
