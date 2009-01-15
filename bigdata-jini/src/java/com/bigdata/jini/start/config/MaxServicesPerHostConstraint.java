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
 * Created on Jan 15, 2009
 */

package com.bigdata.jini.start.config;

import java.net.InetAddress;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Logger;

import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.DataServer.AdministrableDataService;
import com.bigdata.service.jini.MetadataServer.AdministrableMetadataService;

/**
 * Places a limit on the #of instances of a service which can be created on the
 * same host. Jini is used to discover the instances of the specified class of
 * service. The constraint is satisified iff the #of discovered instances is
 * less than the maximum.
 * <p>
 * Note: Since jini does not directly report the hostname of a service, this
 * relies on the {@link Hostname} attribute which is placed onto the bigdata
 * service instances.
 * <p>
 * Note: Since a {@link MetadataService} is a {@link DataService}, you want to
 * use {@link AdministrableMetadataService} and {@link AdministrableDataService}
 * to specify the constraints for those services. If you use only
 * {@link IDataService} or {@link DataService} the constraint will be applied to
 * both and instances of both will be counted against the maximum.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Other kinds of rules could be imposed. Such as no allowing one any
 *       other kind of service on a host running a specified kind of service.
 *       For example, do not run any other service on a host running zookeeper
 *       (listed as a zookeeper server) or running the metadata service. You can
 *       accomplish all of that using static constraints, but the constraints
 *       could also be specified dynamically. However, such dynamic constraints
 *       could not be atomic as concurrent service start decisions could lead to
 *       a post-condition which violates the constraint.
 */
public class MaxServicesPerHostConstraint implements IServiceConstraint {

    protected static final Logger log = Logger.getLogger(MaxServicesPerHostConstraint.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -4578030743078117032L;

    protected final String className;
    
    protected final int maxServices;
    
    /**
     * The timeout in milliseconds for service discovery.
     */
    protected final long timeout;

    /**
     * Creates a constraint with a default service discovery timeout (2000 ms).
     * 
     * @param className
     *            The name of a class or interface that identifies the services
     *            to be discovered.
     * @param maxServices
     *            The maximum #of services which are instances of that class or
     *            interface which may run on any given host.
     */
    public MaxServicesPerHostConstraint(final String className,
            final int maxServices) {

        this(className, maxServices, 2000/* ms */);

    }

    /**
     * 
     * @param className
     *            The name of a class or interface that identifies the services
     *            to be discovered.
     * @param maxServices
     *            The maximum #of services which are instances of that class or
     *            interface which may run on any given host.
     * @param timeout
     *            The timeout for service discovery in milliseconds.
     */
    public MaxServicesPerHostConstraint(final String className,
            final int maxServices, final long timeout) {

        if (className == null)
            throw new IllegalArgumentException();
        
        if (maxServices <= 0)
            throw new IllegalArgumentException();
        
        this.className = className;
        
        this.maxServices = maxServices;
        
        this.timeout = 2000;// ms.
        
    }

    public boolean allow(final JiniFederation fed) throws Exception {
        
        final String hostname = InetAddress.getLocalHost().getHostName();

        final String canonicalHostname = InetAddress.getLocalHost()
                .getCanonicalHostName();
        
        final Class cls = Class.forName(className);

        final ServiceTemplate tmpl = new ServiceTemplate(null/* serviceID */,
        // must match the class or interface.
                new Class[] { cls }, new Entry[] {
                        // must be on this host (match any of these).
                        new Hostname(hostname),
                        new Hostname(canonicalHostname), });

        final ServiceDiscoveryManager serviceDiscoveryManager = new ServiceDiscoveryManager(
                fed.getDiscoveryManagement(), new LeaseRenewalManager());

        try {

            final ServiceItem[] serviceItems = serviceDiscoveryManager
                    .lookup(tmpl, maxServices/* minMatches */,
                            maxServices/* maxMatches */, null/* filter */,
                            timeout/* waitDur */);

            final boolean allowed = serviceItems.length < maxServices;
            
//            if (INFO)
//                log.info // @todo lower logging level.
                log.warn("New instance: allowed=" + allowed + ", #found="
                        + serviceItems.length + ", host=" + canonicalHostname);

            return allowed;

        } finally {

            serviceDiscoveryManager.terminate();

        }

    }

}
