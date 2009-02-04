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
 * Created on Sep 17, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.counters.CounterSet;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Interface allowing services to take over handling of events normally handled
 * by the {@link AbstractFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IFederationDelegate {

    /**
     * Return a name for the service.  It is up to administrators to ensure that
     * service names are unique.
     * 
     * @return A name for the service.
     */
    public String getServiceName();
    
    /**
     * Return the class or interface that is the most interesting facet of the
     * client and which will be used to identify this client in the performance
     * counters reported to the {@link ILoadBalancerService}.
     * 
     * @return The class or interface and never <code>null</code>.
     */
    public Class getServiceIface();

    /**
     * The {@link UUID} assigned to the {@link IBigdataClient} or
     * {@link AbstractService}.
     * 
     * @see AbstractService#setServiceUUID(UUID)
     */
    public UUID getServiceUUID();
    
    /**
     * Offers the service an opportunity to dynamically detach and re-attach
     * performance counters. This can be invoked either in response to an http
     * GET or the periodic reporting of performance counters to the
     * {@link ILoadBalancerService}. In general, implementations should limit
     * the frequency of update, e.g., to no more than once a second.
     */
    public void reattachDynamicCounters();

    /**
     * Return <code>true</code> iff the service is ready to start.
     */
    public boolean isServiceReady();

    /**
     * Invoked by the {@link AbstractFederation} once the deferred startup tasks
     * are executed. Services may use this event to perform additional
     * initialization.
     */
    public void didStart();

    /**
     * Notice that the service has been discovered. This notice will be
     * generated the first time the service is discovered by a given
     * {@link IBigdataClient}.
     * 
     * @param service
     *            The service.
     * @param serviceUUID
     *            The service {@link UUID}.
     */
    public void serviceJoin(IService service, UUID serviceUUID);
    
    /**
     * Notice that the service is no longer available. This notice will be
     * generated once for a given {@link IBigdataClient} when the service is no
     * longer available from any of its service registrars.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     */
    public void serviceLeave(UUID serviceUUID);

    /**
     * Create a new {@link AbstractHTTPD} instance.
     * 
     * @param port
     *            The port, or zero for a random port.
     * @param counterSet
     *            The root {@link CounterSet} that will be served up.
     * 
     * @return The httpd daemon.
     * 
     * @throws IOException
     */
    public AbstractHTTPD newHttpd(final int httpdPort,
            final CounterSet counterSet) throws IOException;

}
