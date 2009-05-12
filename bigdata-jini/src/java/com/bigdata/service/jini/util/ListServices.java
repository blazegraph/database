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
 * Created on Jan 10, 2009
 */

package com.bigdata.service.jini.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;

import org.apache.log4j.Logger;

import com.bigdata.service.IService;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

/**
 * Utility will list the discovered services in federation to which it connects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ListServices {
    
    protected static final Logger log = Logger.getLogger(ListServices.class);

    protected static final String COMPONENT = ListServices.class.getName();

    /**
     * Lists the discovered services.
     * <p>
     * Configuration options use {@link #COMPONENT} as their namespace. The
     * following options are defined:
     * <dl>
     * 
     * <dt>discoveryDelay</dt>
     * <dd>The time in milliseconds to wait for service discovery before
     * proceeding.</dd>
     * 
     * <dt>showServiceItems</dt>
     * <dd>When <code>true</code> the {@link ServiceItem} will be written out
     * for each discovered service.</dd>
     * 
     * </dl>
     * 
     * @param args
     *            Configuration file and optional overrides.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     * @throws IOException
     */
    public static void main(final String[] args) throws InterruptedException,
            ConfigurationException, IOException {

        final JiniFederation fed = JiniClient.newInstance(args).connect();

        final long discoveryDelay = (Long) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, "discoveryDelay", Long.TYPE, 5000L/* default */);

        final boolean showServiceItems = (Boolean) fed.getClient()
                .getConfiguration().getEntry(COMPONENT, "showServiceItems",
                        Boolean.TYPE, false/* default */);
        
        System.out.println("Waiting " + discoveryDelay
                + "ms for service discovery.");

        Thread.sleep(discoveryDelay/* ms */);

        final ServiceItem[] a = fed.getServicesManagerClient()
                .getServiceCache()
                .getServiceItems(0/* maxCount */, null/* filter */);

        System.out.println("Found " + a.length + " services after "
                + discoveryDelay + "ms");

        // Aggregate the bigdata services by their most interesting interfaces.
        final Map<Class<? extends IService>, List<ServiceItem>> bigdataServices = new HashMap<Class<? extends IService>, List<ServiceItem>>(
                a.length);

        int bigdataServiceCount = 0;
        
        final List<ServiceItem> otherServices = new LinkedList<ServiceItem>();
        {
            
            for (ServiceItem serviceItem : a) {

                if (!(serviceItem.service instanceof IService)) {

                    otherServices.add(serviceItem);

                    continue;

                }

                final Class<?extends IService> serviceIface = ((IService) serviceItem.service)
                .getServiceIface();
                
                List<ServiceItem> lst = bigdataServices.get(serviceIface); 
                if(lst == null) {

                    lst = new LinkedList<ServiceItem>();
                    
                    bigdataServices.put(serviceIface, lst);
                    
                }

                lst.add(serviceItem);

                bigdataServiceCount++;

            }

        }

        /*
         * Figure out if zookeeper is running.
         * 
         * Note: We don't wait long here since we already waited for service
         * discovery above.
         */
        final boolean foundZooKeeper = fed.getZookeeperAccessor()
                .awaitZookeeperConnected(10, TimeUnit.MILLISECONDS);

        /*
         * Figure out how many service registrars have been discovered.
         */
        final ServiceRegistrar[] registrars = fed.getDiscoveryManagement()
                .getRegistrars();
        
        /*
         * Write out a summary of the discovered services.
         */

        System.out.println("Zookeeper: is " + (foundZooKeeper ? "" : " not ")
                + "running");

        System.out.println("Discovered " + registrars.length
                + " service registrars.");

        System.out.println("Discovered " + bigdataServiceCount
                + " bigdata services.");

        System.out.println("Discovered " + otherServices.size()
                + " other services.");

        for (Map.Entry<Class<? extends IService>, List<ServiceItem>> e : bigdataServices
                .entrySet()) {

            System.out.println("There are " + e.getValue().size()
                    + " instances of " + e.getKey().getName());

            if (showServiceItems)
                for (ServiceItem t : e.getValue()) {

                    System.out.println(t.toString());

                }

        }

        if (showServiceItems)
            for (ServiceItem t : otherServices) {

                System.out.println(t.toString());

            }

        System.exit(0);

    }

}
