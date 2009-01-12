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

import java.util.List;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

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
abstract public class ManagedServiceConfiguration extends ServiceConfiguration {

    public interface Options extends ServiceConfiguration.Options {

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
    protected ManagedServiceStarter newServiceStarter(IServiceListener listener)
            throws Exception{
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Factory method returns an object that may be used to start an new
     * instance of the service for the specified path.
     * 
     * @param fed
     * @param listener
     * @param logicalServiceZPath
     *            The path to the logical service whose instance will be
     *            started.
     * 
     * @throws Exception
     *             if there is a problem creating the service starter.
     */
    abstract public ManagedServiceStarter newServiceStarter(JiniFederation fed,
            IServiceListener listener, String logicalServiceZPath)
            throws Exception;

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
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <V>
     */
    public abstract class ManagedServiceStarter<V extends ProcessHelper>
            extends AbstractServiceStarter<V> {

        protected final JiniFederation fed;

        protected final String logicalServiceZPath;

        protected final ZooKeeper zookeeper;

        /**
         * 
         * @param fed
         * @param listener
         * @param logicalServiceZPath
         *            The zpath to the logical service whose instance will be
         *            started. Note that the zpath to the
         *            {@link CreateMode#EPHEMERAL_SEQUENTIAL} node for the
         *            physical service instance MUST be created by that process
         *            so that the life cycle of the ephemeral node is tied to
         *            the life cycle of the process (or at least to its
         *            {@link ZooKeeper} client).
         */
        protected ManagedServiceStarter(final JiniFederation fed,
                final IServiceListener listener,
                final String logicalServiceZPath) {

            super(listener);
            
            if (listener == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;
            
            this.logicalServiceZPath = logicalServiceZPath;
            
            this.zookeeper = fed.getZookeeper();
            
        }

        /**
         * Extended to verify that the {@link #logicalServiceZPath} exists.
         */
        protected void setUp() throws Exception {

            if (zookeeper.exists(logicalServiceZPath, false/* watch */) == null) {

                throw new IllegalStateException(
                        "Logical service zpath not found: "
                                + logicalServiceZPath);

            }

            super.setUp();

        }

    }

}
