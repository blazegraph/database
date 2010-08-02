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

import com.bigdata.jini.lookup.entry.Hostname;
import java.io.IOException;
import java.net.UnknownHostException;
import org.apache.log4j.Logger;

import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceItem;

import com.bigdata.jini.start.IServicesManagerService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.config.NicUtil;
import java.net.InetAddress;
import net.jini.config.Configuration;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.ServiceItemFilter;

/**
 * Utility will broadcast the 
 * {@link IServicesManagerService#sighup(boolean,boolean)} method or
 * {@link ILoadBalancerService#sighup()} method to either local or
 * all discovered {@link IServicesManagerService}s 
 * or {@link ILoadBalancerService}s in federation to which it
 * connects. Each discovered {@link IServicesManagerService} will push the
 * service configuration to zookeeper and then restart any processes for which
 * it has responsibility which are not currently running.
 * Each discovered {@link ILoadBalancerService} will log current counters to
 * files.
 * <p>
 * Note: If you are running a federation on a cluster, you can achieve the same
 * effect by changing the federation run state to <code>hup</code> and then
 * changing it back to <code>status</code> after the bigdata controller script
 * has been executed at least once by each machine.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BroadcastSighup {
    
    protected static final Logger log = Logger.getLogger(BroadcastSighup.class);

    protected static final String COMPONENT = BroadcastSighup.class.getName(); 
    
    /**
     * Sends the signal to all discovered {@link IServicesManagerService}s.
     * <p>
     * Configuration options use {@link #COMPONENT} as their namespace. The
     * following options are defined:
     * <dl>
     * 
     * <dt>localOrRemote</dt>
     * <dd>If "local", then consider only services running on the local host
     * (similar to what linux "kill -hup" signal used to do). If
     * "all" then call sighup() on all services found. </dd>
     * 
     * <dt>signalTarget</dt>
     * <dd>If "servicesManager", then send signals only to instances of 
     * IServicesManagerService. If "loadBalancer", then send signals only to 
     * instances of ILoadBalancerService. </dd>
     * 
     * <dt>discoveryDelay</dt>
     * <dd>The time in milliseconds to wait for service discovery before
     * proceeding.</dd>
     * 
     * <dt>pushConfig</dt>
     * <dd> If you want to do a service configuration push. </dd>
     * 
     * <dt>restartServices</dt>
     * <dd> If you want the services manager that receives the message to
     * restart any services for which it is responsible which are not currently
     * running. </dd>
     * 
     * </dl>
     * 
     * @param args
     *            Configuration file and optional overrides.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     */
    public static void main(final String[] args) {
        try {
            main2(args);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("Unexpected exception", e);
        }
    }

    private static void main2(final String[] args) throws InterruptedException,
            ConfigurationException, UnknownHostException, IOException {

        // Get the configuration and set up the federation.

        final JiniClient client = JiniClient.newInstance(args);
        final JiniFederation fed = client.connect();
        final Configuration config = client.getConfiguration();

        final long discoveryDelay = (Long) config
                .getEntry(COMPONENT, "discoveryDelay", Long.TYPE, 5000L/* default */);

        final boolean pushConfig = (Boolean) config
                .getEntry(COMPONENT, "pushConfig", Boolean.TYPE, true/* default */);

        final String localOrAll = (String) config
                .getEntry(COMPONENT, "localOrAll", String.class, "all");

        final String signalTarget = (String) config
                .getEntry(COMPONENT, "signalTarget", String.class,
                          "servicesManager");

        final boolean restartServices = (Boolean) config
                .getEntry(COMPONENT, "restartServices",
                        Boolean.TYPE, true/* default */);

        // Identify the bigdata interface associated with the service
        // to which the signal will be delivered.

        Class iface = null;
        if (signalTarget.equals("servicesManager")) {
            iface = IServicesManagerService.class;
        } else if (signalTarget.equals("loadBalancer")) {
            iface = ILoadBalancerService.class;
        } else {
            log.warn("Unexpected target for signal: " + signalTarget);
            System.exit(1);
        }

        // Set up the service template and filter used to identify the service.

        final String hostname = 
            NicUtil.getIpAddress("default.nic", "default", false);
        ServiceTemplate template = new ServiceTemplate(null,
                new Class[] { iface }, null);
        ServiceItemFilter thisHostFilter = null;
        if (localOrAll.equals("local")) {
            thisHostFilter = new ServiceItemFilter() {
                    public boolean check(ServiceItem item) {
                        for (Entry entry : item.attributeSets) {
                            if (entry instanceof Hostname &&
                                ((Hostname)entry).hostname.equals(hostname)) {
                                return true;
                            }
                        }
                        return false;
                    }
                };
        } else if (!localOrAll.equals("all")) {
            log.warn("Unexpected option for signal: " + localOrAll);
            System.exit(1);
        }

        // Use the federation's discovery manager to lookup bigdata
        // services of interest.

        System.out.println("Waiting " + discoveryDelay
                + "ms for service discovery.");
        ServiceItem[] items =
                fed.getServiceDiscoveryManager()
                .lookup(template, Integer.MAX_VALUE, Integer.MAX_VALUE,
                thisHostFilter, discoveryDelay);

        // Call the service's appropriate interface method.

        int n = 0;
        for (ServiceItem item : items) {
            try {
                if (signalTarget.equals("servicesManager")) {
                    ((IServicesManagerService) item.service)
                        .sighup(pushConfig, restartServices);
                    ++n;

                } else if (signalTarget.equals("loadBalancer")) {
                    ((ILoadBalancerService) item.service).sighup();
                    ++n;

                } else {
                    log.warn("Unexpected target for signal: " + signalTarget);
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.warn("Unexpected target for signal: " + signalTarget);
            }
        }
        System.out.println("Signal sent to " + n + " of " + items.length
                + " instances of " + signalTarget + ".");
    }
}
