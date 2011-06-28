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
 * Created on Jan 11, 2009
 */

package com.bigdata.jini.start.config;

import java.io.File;
import java.util.List;
import java.util.UUID;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.lookup.entry.Name;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import com.bigdata.jini.lookup.entry.ServiceUUID;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.ManageLogicalServiceTask;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.service.jini.JiniFederation;

/**
 * Configuration of a managed service. These are generally jini services whose
 * ephemeral state is registered with zookeeper. Explicitly excluded are the
 * {@link JiniCoreServicesConfiguration jini core services} and zookeeper
 * itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ManagedServiceConfiguration extends JavaServiceConfiguration {

    public interface Options extends JavaServiceConfiguration.Options {

    }

    /**
     * @param className
     * @param config
     * @throws ConfigurationException
     */
    public ManagedServiceConfiguration(String className, Configuration config)
            throws ConfigurationException {
     
        super(className, config);

    }

    /**
     * Not supported - use
     * {@link #newServiceStarter(JiniFederation, IServiceListener, String)}
     * instead.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public ManagedServiceStarter newServiceStarter(IServiceListener listener)
            throws Exception{
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Factory method returns an object that may be used to (re-)start a
     * service.
     * 
     * @param fed
     * @param listener
     * @param logicalServiceZPath
     *            The path to the logical service whose instance will be
     *            started.
     * @param attributes
     *            This provides the information required to restart a persistent
     *            service. When not given a new service instance will be
     *            started. When given, the same services instance will be
     *            restarted.
     * 
     * @throws Exception
     *             if there is a problem creating the service starter.
     */
    public ManagedServiceStarter newServiceStarter(final JiniFederation fed,
            final IServiceListener listener, final String logicalServiceZPath,
            final Entry[] attributes) throws Exception {

        return new ManagedServiceStarter<ProcessHelper>(fed, listener,
                logicalServiceZPath, attributes);
        
    }

    /**
     * Return a task that will correct any imbalance between the
     * {@link ServiceConfiguration} and the #of logical services.
     * 
     * @param fed
     * @param listener
     * @param configZPath
     *            The zpath of the {@link ManagedServiceConfiguration}
     * @param children
     *            The list of logical service instances. These are the children
     *            of the {@link ManagedServiceConfiguration} znode.
     * @return
     */
    public ManageLogicalServiceTask<ManagedServiceConfiguration> newLogicalServiceTask(
            JiniFederation fed, IServiceListener listener, String configZPath,
            List<String> children) {

        return new ManageLogicalServiceTask<ManagedServiceConfiguration>(fed,
                listener, configZPath, children, this);

    }

    /**
     * This class and subclasses support re-start of persistent services.
     * Restart is only possible for persistent services. Persistent services
     * must satisify several criteria in order for restart to succeed, including
     * the stable assignment of the physical service znode name and the stable
     * assignment of the service directory.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <V>
     */
    public class ManagedServiceStarter<V extends ProcessHelper>
            extends JavaServiceStarter<V> {

        protected final JiniFederation fed;

        protected final String logicalServiceZPath;

//        protected final ZooKeeper zookeeper;

        /**
         * The znode for the logical service (the last component of the
         * {@link AbstractServiceStarter#logicalServiceZPath}.
         */
        public final String logicalServiceZNode;

        /**
         * <code>true</code> iff this is a service restart and
         * <code>false</code> if this is a new service start.
         */
        public final boolean restart;
        
        /**
         * The representation of the assigned {@link ServiceID} as a
         * {@link UUID}. This is pre-assigned so that we can recognize the
         * service when it joins with a jini registrar, which is how we know
         * that the service has started successfully. It is also used to watch
         * for the create of the znode representing the physical service
         * instance.
         */
        public final UUID serviceUUID;
        
        /**
         * The canonical service name. This is formed in much the same manner as
         * the {@link #serviceDir} using the service type, the
         * {@link #logicalServiceZNode}, and the unique {@link #serviceToken}.
         * While the {@link #serviceToken} alone is unique, the path structure
         * of the service name make is possible to readily classify a physical
         * service by its type and logical instance.
         */
        public final String serviceName;

        /**
         * The service instance directory. This is where we put any
         * configuration files and should be the default location for the
         * persistent state associated with the service (services may of course
         * be configured to put aspects of their state in a different location,
         * such as the zookeeper log files).
         * <p>
         * The serviceDir is created using path components from the service
         * type, the {@link #logicalServiceZNode}, and finally the unique
         * {@link #serviceToken} assigned to the service.
         */
        public final File serviceDir;

        /**
         * 
         * @param fed
         * @param listener
         * @param logicalServiceZPath
         *            The zpath to the logical service.
         * @param attributes
         *            This provides the information required to restart a
         *            persistent service. When not given a new service instance
         *            will be started. When given, the same service instance
         *            will be restarted.
         */
        protected ManagedServiceStarter(final JiniFederation fed,
                final IServiceListener listener,
                final String logicalServiceZPath,
                final Entry[] attributes) {

            super(listener);
            
            if (fed == null)
                throw new IllegalArgumentException();

            if (listener == null)
                throw new IllegalArgumentException();
            
            if (logicalServiceZPath == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;
            
            this.logicalServiceZPath = logicalServiceZPath;
            
//            this.zookeeper = fed.getZookeeper();
            
            // just the child name for the logical service.
            logicalServiceZNode = logicalServiceZPath
                    .substring(logicalServiceZPath.lastIndexOf('/') + 1);

            this.restart = attributes != null;

            if (restart) {

                String serviceName = null;
                UUID serviceUUID = null;

                for(Entry e : attributes) {
                    
                    if (serviceName == null && e instanceof Name) {

                        serviceName = ((Name) e).name;

                    } else if (serviceUUID == null && e instanceof ServiceUUID) {

                        serviceUUID = ((ServiceUUID) e).serviceUUID;

                    }
                    
                }
                
                if (serviceName == null)
                    throw new RuntimeException("Attribute not found: "
                            + Name.class.getName());

                if (serviceUUID == null)
                    throw new RuntimeException("Attribute not found: "
                            + ServiceUUID.class.getName());
                
                this.serviceName = serviceName;
                
                this.serviceUUID = serviceUUID;

            } else {

                // Generate a random UUID.
                this.serviceUUID = UUID.randomUUID();
                
                // The canonical service name.
                this.serviceName = cls.getSimpleName() + "/"
                        + logicalServiceZNode + "/" + serviceUUID;

            }
            
            // The actual service directory (chosen at runtime).
            serviceDir = new File(new File(new File(
                    ManagedServiceConfiguration.this.serviceDir, cls
                            .getSimpleName()), logicalServiceZNode),
                    serviceUUID.toString());

        }

        /**
         * Returns the actual service directory which is choosen at runtime
         * based on the {@link #logicalServiceZNode} and the
         * {@link #serviceToken}.
         */
        protected File getServiceDir() {
            
            return serviceDir;
            
        }
        
        /**
         * Extended to verify that the {@link #logicalServiceZPath}, the
         * {@link BigdataZooDefs#PHYSICAL_SERVICES_CONTAINER}, and the
         * {@link BigdataZooDefs#MASTER_ELECTION} znodes exist.
         */
        protected void setUp() throws Exception {

            /*
             * Note: This is not robust to a SessionExpiredException. Such an
             * event will be propagated up and the service will not be started.
             * The failure to start the service is handled by the logic which
             * decides which service to start on a node.
             */
            
            final ZooKeeper zookeeper = fed.getZookeeper();
            
            String zpath = logicalServiceZPath;

            if (zookeeper.exists(zpath, false/* watch */) == null) {

                throw new IllegalStateException("Not found: " + zpath);

            }

            zpath = logicalServiceZPath + "/"
                    + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER;

            if (zookeeper.exists(zpath, false/* watch */) == null) {

                throw new IllegalStateException("Not found: " + zpath);

            }

            zpath = logicalServiceZPath + "/" + BigdataZooDefs.MASTER_ELECTION;

            if (zookeeper.exists(zpath, false/* watch */) == null) {

                throw new IllegalStateException("Not found: " + zpath);

            }

            super.setUp();

        }

    }

}
