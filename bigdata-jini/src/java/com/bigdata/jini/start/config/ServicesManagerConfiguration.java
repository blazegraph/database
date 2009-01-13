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
 * Created on Jan 13, 2009
 */

package com.bigdata.jini.start.config;

import java.util.LinkedList;
import java.util.List;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import cern.colt.Arrays;

import com.bigdata.jini.start.ServicesManagerServer;
import com.bigdata.jini.start.process.ZookeeperServerConfiguration;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.LoadBalancerServer;
import com.bigdata.service.jini.MetadataServer;
import com.bigdata.service.jini.TransactionServer;
import com.sun.jini.tool.ClassServer;

/**
 * For the {@link ServicesManagerServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServicesManagerConfiguration extends BigdataServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 5087489087279869212L;
    
    /**
     * {@link Configuration} options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends BigdataServiceConfiguration.Options {
        
        /**
         * An array of the (class) names of the services that will be started by
         * the {@link ServicesManagerServer}. For each service declared in this
         * array, there must be a corresponding component defined within the
         * {@link Configuration}. For each {@link ManagedServiceConfiguration},
         * an entry will be made in zookeeper and logical and physical service
         * instances will be managed automatically. For unmanaged services, such
         * as {@link JiniCoreServicesConfiguration} and zookeeper itself,
         * instances will be started iff necessary by the services manager when
         * it starts up.
         * 
         * @todo the resolution of the {@link ServiceConfiguration} class to
         *       instantiate from the (class) name is completely hacked. See
         *       {@link ServicesManagerConfiguration#getServiceConfigurations(Configuration)}.
         */
        String SERVICES = "services";

    }

    public final String[] services;

    protected void toString(StringBuilder sb) {

        sb.append(", " + Options.SERVICES + "=" + Arrays.toString(services));
        
    }

    /**
     * @param config
     * 
     * @throws ConfigurationException
     */
    public ServicesManagerConfiguration(Configuration config)
            throws ConfigurationException {

        super(ServicesManagerServer.class, config);

        services = (String[]) config.getEntry(ServicesManagerServer.class
                .getName(), Options.SERVICES, String[].class);
        
    }

    /**
     * Generates {@link ServiceConfiguration}s from the {@link Configuration}
     * file. Only those declared in {Options#SERVICES} will be returned.
     * 
     * @param config
     *            The {@link Configuration} file.
     * 
     * @return An array of {@link ServiceConfiguration}s populated from the
     *         {@link Configuration} file.
     * 
     * @throws ConfigurationException
     * 
     * @todo the resolution of the {@link ServiceConfiguration} class to
     *       instantiate from the (class) name is completely hacked in the code
     *       here. Nothing declarative about it.
     * 
     * @todo support the {@link ClassServer}.
     */
    public ServiceConfiguration[] getServiceConfigurations(final Configuration config)
            throws ConfigurationException {

        final List<ServiceConfiguration> v = new LinkedList<ServiceConfiguration>();

        for (String a : services) {

            if (a == null)
                throw new ConfigurationException(Options.SERVICES
                        + ": Contains null elements.");

            if (a.equals("jini")) {

                v.add(new JiniCoreServicesConfiguration(config));

            } else if (a.equals(QuorumPeerMain.class.getName())) {

                    v.add(new ZookeeperServerConfiguration(config));

            } else if (a.equals(TransactionServer.class.getName())) {

                v.add(new TransactionServerConfiguration(config));

            } else if (a.equals(MetadataServer.class.getName())) {

                v.add(new MetadataServerConfiguration(config));

            } else if (a.equals(DataServer.class.getName())) {

                v.add(new MetadataServerConfiguration(config));

            } else if (a.equals(LoadBalancerServer.class.getName())) {

                v.add(new LoadBalancerConfiguration(config));

            } else {

                throw new ConfigurationException(Options.SERVICES
                        + " : Unknown class/name: " + a);

            }
            
        }

        // // class server(s).
        // zoo.create(zconfig + ZSLASH
        // + ClassServer.class.getSimpleName(), SerializerUtil
        // .serialize(classServerConfig), acl,
        // CreateMode.PERSISTENT);

//        // metadata server
//        v.add(new MetadataServerConfiguration(config));
//
//        // data server(s) (lots!)
//        v.add(new DataServerConfiguration(config));
//
//        // load balancer server.
//        v.add(new LoadBalancerConfiguration(config));

        return v.toArray(new ServiceConfiguration[0]);
            
    }
    
}
