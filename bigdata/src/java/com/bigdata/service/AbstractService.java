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
 * Created on Apr 28, 2008
 */

package com.bigdata.service;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ITimestampService;

/**
 * Abstract base class defines protocols for setting the service {@link UUID},
 * etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractService implements IService {

    protected static final Logger log = Logger.getLogger(AbstractService.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    private String serviceName;
    private UUID serviceUUID;
    
    /**
     * 
     */
    protected AbstractService() {
        
        serviceName = getHostname() + "#" + getServiceIface();
        
    }

    /**
     * This method must be invoked to set the service {@link UUID}.
     * <p>
     * Note: This method gets invoked at different times depending on whether
     * the service is embedded (generally invoked from the service constructor)
     * or written against a service discovery framework (invoked once the
     * service has been assigned a UUID by a registrar or during re-start since
     * the service UUID is read from a local file).
     * <p>
     * Several things depend on when this method is invoked, including the setup
     * of the per-service {@link CounterSet} reported by the service to the
     * {@link ILoadBalancerService}.
     * 
     * @param serviceUUID
     *            The {@link UUID} assigned to the service.
     * 
     * @throws IllegalArgumentException
     *             if the parameter is null.
     * @throws IllegalStateException
     *             if the service {@link UUID} has already been set.
     */
    synchronized public void setServiceUUID(UUID serviceUUID)
            throws IllegalStateException {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (this.serviceUUID != null)
            throw new IllegalStateException();

        if (INFO)
            log.info("uuid=" + serviceUUID);

        this.serviceUUID = serviceUUID;

    }

    final public UUID getServiceUUID() {

        return serviceUUID;

    }
    
    /**
     * Return the most interesting interface for the service.
     */
    abstract public Class getServiceIface();

    /**
     * Starts the {@link AbstractService}.
     * <p>
     * Note: A {@link #start()} is required in order to give subclasses an
     * opportunity to be fully initialized before they are required to begin
     * operations. It is impossible to encapsulate the startup logic cleanly
     * without this ctor() + start() pattern. Those familiar with Objective-C
     * will recognized this.
     * 
     * @return <i>this</i> (the return type should be strengthened by the
     *         concrete implementation to return the actual type).
     */
    abstract public AbstractService start();
    
    /**
     * Return the proxy used to access other services in the federation.
     * 
     * @todo access to the {@link ITimestampService}, {@link IMetadataService}
     *       and the {@link ILoadBalancerService} now goes through this method.
     *       The code making those requests needs to be modified since it used
     *       to except a <code>null</code> return for the individual services
     *       if they were not available and it can not see an exception if the
     *       federation itself is not available (I believe that this can only
     *       happen for the JiniFederation since the various embedded
     *       federations wind up returning a closely held reference whereas the
     *       JiniFederation is obtained from the JiniClient#getClient().
     */
    abstract public AbstractFederation getFederation();

    public final String getHostname() {
        
        return AbstractStatisticsCollector.fullyQualifiedHostName;
        
    }
    
//    /**
//     * Shuts down the {@link #getFederation()} used by this service.
//     */
    public synchronized void shutdown() {
        
//        if(!isOpen()) return;
        
//        getFederation().shutdown();

//        open = false;

    }
    
//    /**
//     * Shuts down the {@link #getFederation()} used by this service.
//     */
    public synchronized void shutdownNow() {

//        if(!isOpen()) return;
        
//        getFederation().shutdownNow();
        
//        open = false;
        
    }
    
//    final public boolean isOpen() {
//        
//        return open;
//        
//    }
//    private boolean open = true;

    /**
     * Note: This is overriden in the jini integration to return a configured
     * name for the service.
     */
    public String getServiceName() {
    
        return serviceName;
        
    }
    
}
