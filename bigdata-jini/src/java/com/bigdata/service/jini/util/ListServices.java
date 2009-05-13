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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.jini.config.ConfigurationException;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;

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
     * for each discovered service (default <code>false</code>).</dd>
     * 
     * <dt>repeatCount</dt>
     * <dd>The #of times to repeat the discovery process and list the discovered
     * services before terminating (default <code>1</code>). When ZERO (0), the
     * utility will repeatedly discover and list the discovered processes until
     * killed.</dd>
     * 
     * </dl>
     * 
     * @param args
     *            Configuration file and optional overrides.
     * 
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws ExecutionException 
     */
    public static void main(final String[] args) throws InterruptedException,
            ConfigurationException, IOException, ExecutionException {

        final JiniFederation fed = JiniClient.newInstance(args).connect();

        final int repeatCount = (Integer) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, "repeatCount", Integer.TYPE, 1/* default */);

        /*
         * Install a shutdown hook (normal kill will trigger this hook).
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {

                fed.shutdownNow();

            }

        });
        
        if (repeatCount == 0) {
         
            while(true) {

                final Future<String> f = fed.getExecutorService().submit(
                        new DiscoverAndListTask(fed));
             
                System.out.println(f.get());
                
            }
            
        } else {
            
            for(int i=0; i<repeatCount; i++) {

                final Future<String> f = fed.getExecutorService().submit(
                        new DiscoverAndListTask(fed));
             
                System.out.println(f.get());

            }
            
        }

        fed.shutdown();
        
        System.exit(0);

    }

    /**
     * Task waits service discovery and then lists out the discovered services.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class DiscoverAndListTask implements Callable<String> {
        
        final JiniFederation fed;

        final long discoveryDelay;

        final boolean showServiceItems;

        public DiscoverAndListTask(final JiniFederation fed)
                throws ConfigurationException {

            this.fed = fed;

            discoveryDelay = (Long) fed
                    .getClient()
                    .getConfiguration()
                    .getEntry(COMPONENT, "discoveryDelay", Long.TYPE, 5000L/* default */);

            showServiceItems = (Boolean) fed.getClient().getConfiguration()
                    .getEntry(COMPONENT, "showServiceItems", Boolean.TYPE,
                            false/* default */);

        }

        public String call() throws Exception {

            System.out.println("Waiting " + discoveryDelay
                    + "ms for service discovery.");

            // match all services.
            final ServiceItem[] a = fed.getServiceDiscoveryManager().lookup(//
                    new ServiceTemplate(//
                            null, //serviceID
                            new Class[0],// serviceTypes[]
                            new Entry[0] // attributes
                            ),
                    Integer.MAX_VALUE,// minMatches
                    Integer.MAX_VALUE,// maxMatches
                    null,// filter
                    discoveryDelay// waitDur(ms)
                    );

            final StringBuilder sb = new StringBuilder();
            
            // A list of non-bigdata services.
            final List<ServiceItem> otherServices = new LinkedList<ServiceItem>();
            
            // Aggregate the bigdata services by their most interesting interfaces.
            final Map<Class<? extends IService>, List<ServiceItem>> bigdataServices = new HashMap<Class<? extends IService>, List<ServiceItem>>(
                    a.length);

            // Aggregate the bigdata services by the host on which they are
            // running.
            final Map<String, List<ServiceItem>> bigdataServicesByHost = new HashMap<String, List<ServiceItem>>(
                    a.length);

            // A list of bigdata services where RMI failed.
            final List<ServiceItem> staleServices = new LinkedList<ServiceItem>();

            // The #of live bigdata services (where RMI succeeds).
            int bigdataServiceCount = 0;

            {

                for (ServiceItem serviceItem : a) {

                    if (!(serviceItem.service instanceof IService)) {

                        otherServices.add(serviceItem);

                        continue;

                    }

                    final Class<? extends IService> serviceIface;
                    final String hostname;
                    try {

                        serviceIface = ((IService) serviceItem.service)
                                .getServiceIface();

                        hostname = ((IService) serviceItem.service)
                                .getHostname();

                    } catch (IOException ex) {

                        log.warn("RMI error: " + ex + " for " + serviceItem);

                        staleServices.add(serviceItem);

                        continue;

                    }

                    // aggregate by serviceIface
                    {

                        List<ServiceItem> list = bigdataServices
                                .get(serviceIface);

                        if (list == null) {

                            list = new LinkedList<ServiceItem>();

                            bigdataServices.put(serviceIface, list);

                        }

                        list.add(serviceItem);

                    }

                    // aggregate by hostname
                    {

                        List<ServiceItem> list = bigdataServicesByHost
                                .get(hostname);

                        if (list == null) {

                            list = new LinkedList<ServiceItem>();

                            bigdataServicesByHost.put(hostname, list);

                        }

                        list.add(serviceItem);

                    }

                    bigdataServiceCount++;

                }

            }

            /*
             * Figure out if zookeeper is running.
             * 
             * Note: We don't wait long here since we already waited for service
             * discovery above and the zookeeper discover was running
             * asynchronously with that (the federation object handles this).
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

            sb.append("Zookeeper is " + (foundZooKeeper ? "" : "not ")
                    + "running.\n");

            sb.append("Jini is " + (registrars.length > 0 ? "" : "not ")
                    + "running (discovered " + registrars.length
                    + " jini service registrars).\n");

            sb.append("Discovered " + a.length + " services\n");

            sb.append("Discovered " + staleServices.size()
                    + " stale bigdata services.\n");

            sb.append("Discovered " + bigdataServiceCount
                    + " live bigdata services.\n");

            sb.append("Discovered " + otherServices.size()
                    + " other services.\n");

            // aggregation by serviceIface
            {

                sb.append("Bigdata services by serviceIface:\n");

                final SortedMap<String, Class<? extends IService>> sortedMap = new TreeMap<String, Class<? extends IService>>();

                for (Class<? extends IService> serviceIface : bigdataServices
                        .keySet()) {

                    sortedMap.put(serviceIface.getName(), serviceIface);

                }

                for (Class<? extends IService> serviceIface : sortedMap
                        .values()) {

                    final List<ServiceItem> list = bigdataServices
                            .get(serviceIface);

                    sb.append("\tThere are " + list.size() + " instances of "
                            + serviceIface + "\n");

                    if (showServiceItems)
                        for (ServiceItem t : list) {

                            sb.append(t.toString());

                            sb.append("\n");

                        }

                }
            }

            // aggregation by hostname
            {
            
                sb.append("Bigdata services by hostname:\n");

                final String[] keys = bigdataServicesByHost.keySet().toArray(
                        new String[] {});
        
                Arrays.sort(keys);
        
                for (String hostname : keys) {

                    final List<ServiceItem> list = bigdataServicesByHost
                            .get(hostname);

                    sb.append("\tThere are " + list.size()
                            + " instances on " + hostname + "\n");

                    if (false && showServiceItems)
                        for (ServiceItem t : list) {

                            sb.append(t.toString());

                            sb.append("\n");

                        }

                }

            }

            if (showServiceItems)
                for (ServiceItem t : otherServices) {

                    sb.append(t.toString());

                    sb.append("\n");

                }

            return sb.toString();

        }

    } // class DiscoveryAndListTask

}
