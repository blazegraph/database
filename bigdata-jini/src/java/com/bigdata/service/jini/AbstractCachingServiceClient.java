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
 * Created on Jan 19, 2009
 */

package com.bigdata.service.jini;

import java.lang.reflect.Method;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

import com.bigdata.jini.lookup.entry.ServiceItemFilterChain;
import com.sun.jini.admin.DestroyAdmin;

/**
 * Abstract base class for a jini client which uses a {@link LookupCache} to
 * reduce round trips to the discovered {@link ServiceRegistrar}s for a
 * specific class of services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 *            A class or interface implemented by all services whose proxies
 *            will be discovered and cached by this class.
 */
abstract public class AbstractCachingServiceClient<S extends Remote> {

    protected static final transient Logger log = Logger
            .getLogger(AbstractCachingServiceClient.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUTG = log.isInfoEnabled();

    final protected JiniFederation fed;

    /**
     * A {@link LookupCache} that will be populated with all services that match
     * a filter. This is used to keep track of all services registered with any
     * {@link ServiceRegistrar} to which the client is listening.
     */
    protected final LookupCache serviceLookupCache;

    /**
     * The template provided to the ctor.
     */
    protected final ServiceTemplate template;

    /**
     * The filter provided to the ctor.
     */
    protected final ServiceItemFilter filter;
    
    /**
     * Timeout for remote lookup on cache miss (milliseconds).
     */
    protected final long timeout;

    /**
     * Provides direct cached lookup of services by their {@link ServiceID}.
     */
    protected final ServiceCache serviceCache;

    /**
     * An object that provides direct cached lookup of proxies by their
     * {@link ServiceID}.
     */
    final public ServiceCache getServiceCache() {
        
        return serviceCache;
        
    }

    /**
     * An object that provides cached lookup of discovered services.
     */
    final public LookupCache getLookupCache() {
        
        return serviceLookupCache;
        
    }
    
    /**
     * Sets up service discovery for the designed class of services.
     * 
     * @param fed
     *            The {@link JiniFederation}. This class will use the
     *            {@link DiscoveryManagement } and
     *            {@link ServiceDiscoveryManager} objects exposed by the
     *            {@link JiniFederation}. Also, {@link ServiceDiscoveryEvent}s
     *            will be passed by this class to the {@link JiniFederation},
     *            which implements {@link ServiceDiscoveryListener}. Those
     *            events are used to notice service joins.
     * @param template
     *            A template used to restrict the services which are discovered
     *            and cached by this class (required).
     * @param filter
     *            A filter used to further restrict the services which are
     *            discovered and cached by this class (optional).
     * @param timeout
     *            The timeout in milliseconds that the client will await the
     *            discovery of a service if there is a cache miss.
     * 
     * @throws RemoteException
     *             if we could not setup the {@link LookupCache}
     */
    public AbstractCachingServiceClient(final JiniFederation fed,
            final ServiceTemplate template, final ServiceItemFilter filter,
            final long timeout) throws RemoteException {

        if (fed == null)
            throw new IllegalArgumentException();

        if (template == null)
            throw new IllegalArgumentException();

        if (timeout < 0)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.template = template;
        
        this.filter = filter;
        
        this.timeout = timeout;

        serviceCache = new ServiceCache(fed);

        serviceLookupCache = fed.getServiceDiscoveryManager()
                .createLookupCache(//
                        template,//
                        filter, //
                        serviceCache // ServiceDiscoveryListener
                );

    }

    /**
     * Terminates asynchronous processing by the {@link LookupCache}.
     */
    final public void terminate() {

        serviceLookupCache.terminate();

    }

    /**
     * Return an arbitrary service from the cache -or- <code>null</code> if
     * there is no such service in the cache and a remote lookup times out.
     */
    @SuppressWarnings("unchecked")
    final public S getService() {

        return getService(filter);
        
    }
    
    /**
     * Return the proxy for an arbitrary service from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     * 
     * @param filter
     *            An optional filter. If given it will be applied in addition to
     *            the optional filter specified to the ctor.
     */
    @SuppressWarnings("unchecked")
    final public S getService(final ServiceItemFilter filter) {

        ServiceItem item = getServiceItem(filter);

        if (item != null)
            return (S) item.service;
        else
            return null;

    }
    
    /**
     * Return an arbitrary {@link ServiceItem} from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     * 
     * @param filter
     *            An optional filter. If given it will be applied in addition to
     *            the optional filter specified to the ctor.
     */
    final public ServiceItem getServiceItem(ServiceItemFilter filter) {

        /*
         * If the instance was configured with a filter and the caller specified
         * a filter then combine the filters. Otherwise use whichever one is non
         * null.
         */
        if (filter != null && this.filter != null) {

            filter = new ServiceItemFilterChain(new ServiceItemFilter[] {
                    filter, this.filter });

        } else if (this.filter != null) {

            filter = this.filter;

        } // else filter is non-null and we use it.

        ServiceItem item = serviceLookupCache.lookup(filter);

        if (item == null) {

            if (INFO)
                log.info("Cache miss.");

            item = handleCacheMiss(filter);

            if (item == null) {

                log.warn("No matching service.");

                return null;

            }

        }

        return item;

    }

    /**
     * Handles a cache miss by a remote query on the managed set of service
     * registrars.
     * 
     * @param filter
     *            The specific filter to be applied.
     */
    final private ServiceItem handleCacheMiss(final ServiceItemFilter filter) {

        ServiceItem item = null;

        try {

            item = fed.getServiceDiscoveryManager().lookup(template, filter,
                    timeout);

        } catch (RemoteException ex) {

            log.error(ex);

            return null;

        } catch (InterruptedException ex) {

            if (INFO)
                log.info("Interrupted - no match.");

            return null;

        }

        if (item == null) {

            // Could not discover a matching service.

            log.warn("Could not discover matching service");

            return null;

        }

        if (INFO)
            log.info("Found: " + item);

        return item;

    }

    /**
     * Return the {@link ServiceItem} associated with the {@link UUID}.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * 
     * @return The service item iff it is found in the cache and
     *         <code>null</code> otherwise.
     */
    final public ServiceItem getServiceItem(final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        final ServiceItem serviceItem = serviceCache.getServiceItemByID(JiniUtil
                .uuid2ServiceID(serviceUUID));

        return serviceItem;

    }

//    /**
//     * Destroy all services which have been discovered by this client.
//     */
//    public void destroyDiscoveredServices(ExecutorService executorService) {
//
//        destroyDiscoveredServices(executorService, null/* filter */);
//        
//    }

    /**
     * Destroy all services in the cache which match the filter. Errors are
     * logged for any service which could not be sent a
     * {@link RemoteDestroyAdmin#destroy()} request.
     * 
     * @param filter
     *            An optional filter. When <code>null</code> all services in
     *            the cache will be destroyed.
     * 
     * @throws InterruptedException
     */
    protected void destroyDiscoveredServices(
            final ExecutorService executorService,
            final ServiceItemFilter filter) throws InterruptedException {

        if (executorService == null)
            throw new IllegalArgumentException();
        
        // return everything in the cache.
        final ServiceItem[] items = serviceCache.getServiceItems(
                0/* maxCount */, filter);

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                items.length);

        for (ServiceItem serviceItem : items) {

            final ServiceItem item = serviceItem;

            tasks.add(new Callable<Void>() {

                public Void call() throws Exception {

                    try {

                        if(destroyService(item)) {
                            
                            log.warn("Could not destroy service: " + item);

                        }

                    } catch (Exception e) {

                        log.error("Could not destroy service: " + item, e);

                    }

                    return null;

                }

            });

        }

        // blocks until all data services are down.
        executorService.invokeAll(tasks);

    }

    /**
     * Shutdown all services in the cache which match the filter. Errors are
     * logged for any service which could not be sent a
     * {@link RemoteDestroyAdmin#shutdown()} or
     * {@link RemoteDestroyAdmin#shutdownNow()} request.
     * 
     * @param filter
     *            An optional filter. When <code>null</code> all services in
     *            the cache will be shutdown.
     * @param immediateShutdown
     *            if shutdownNow() should be used.
     * 
     * @throws InterruptedException
     */
    protected void shutdownDiscoveredServices(
            final ExecutorService executorService,
            final ServiceItemFilter filter,
            final boolean immediateShutdown
            ) throws InterruptedException {

        if (executorService == null)
            throw new IllegalArgumentException();
        
        // return everything in the cache.
        final ServiceItem[] items = serviceCache.getServiceItems(
                0/* maxCount */, filter);

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                items.length);

        for (ServiceItem serviceItem : items) {

            final ServiceItem item = serviceItem;

            tasks.add(new Callable<Void>() {

                public Void call() throws Exception {

                    try {

                        if (shutdownService(item, immediateShutdown)) {

                            log.warn("Could not shutdown service: " + item);

                        }

                    } catch (Exception e) {

                        log.error("Could not shutdown service: " + item, e);

                    }

                    return null;

                }

            });

        }

        // blocks until all data services are down.
        executorService.invokeAll(tasks);
        
    }

    /**
     * Sends {@link RemoteDestroyAdmin#destroy()} to the service.
     * 
     * @param serviceItem
     *            The service item.
     * 
     * @return <code>true</code> if we were able to send that message to the
     *         service.
     * 
     * @throws Exception
     *             if anything goes wrong.
     */
    protected boolean destroyService(final ServiceItem serviceItem)
            throws Exception {

        if (serviceItem == null)
            throw new IllegalArgumentException();
        
        /*
         * Attempt to obtain the Administrable object from the service. The
         * methods that we want will be on that object if they are offered by
         * the service.
         */
        final Object admin;
        {

            final Remote proxy = (Remote) serviceItem.service;

            final Method getAdmin = proxy.getClass().getMethod("getAdmin",
                    new Class[] {});

            admin = getAdmin.invoke(proxy, new Object[] {});

        }

        if (admin instanceof DestroyAdmin) {

            /*
             * This interface destroys the persistent state of the service. A
             * service shutdown in this manner MAY NOT be restarted. We DO NOT
             * invoke this method if the service offers a normal shutdown
             * alternative. However, there are many kinds of services for which
             * destroy() is perfectly acceptable. For example, a jini registrar
             * may be destroyed as its state will be regained from the running
             * services if a new registrar is started.
             */

            // Destroy the service and its persistent state.
            log.warn("will destroy() service: " + this);

            ((DestroyAdmin) admin).destroy();

            return true;
            
        }

        return false;
        
    }

    /**
     * Applies {@link RemoteDestroyAdmin#shutdown()} or
     * {@link RemoteDestroyAdmin#shutdownNow()} as indicated by the parameter.
     * 
     * @param serviceItem
     *            The service item.
     * @param immediateShutdown
     *            When <code>true</code> uses shutdownNow().
     *            
     * @return <code>true</code> if we were able to send either of these
     *         requests to the service.
     * 
     * @throws Exception
     *             if anything goes wrong.
     */
    protected boolean shutdownService(final ServiceItem serviceItem,
            final boolean immediateShutdown) throws Exception {
        
        /*
         * Attempt to obtain the Administrable object from the service. The
         * methods that we want will be on that object if they are offered by
         * the service.
         */
        final Object admin;
        {

            final Remote proxy = (Remote) serviceItem.service;

            final Method getAdmin = proxy.getClass().getMethod("getAdmin",
                    new Class[] {});

            admin = getAdmin.invoke(proxy, new Object[] {});

        }

        if (admin instanceof RemoteDestroyAdmin) {

            /*
             * This interface allows us to shutdown the service without
             * destroying its persistent state. A service shutdown in this
             * manner MAY be restarted.
             */
            if (immediateShutdown) {

                // Fast termination (can have latency).
                log.warn("will shutdownNow() service: " + this);
                
                ((RemoteDestroyAdmin) admin).shutdownNow();
                
            } else {
                
                // Normal termination (can have latency).
                log.warn("will shutdown() service: " + this);
                
                ((RemoteDestroyAdmin) admin).shutdown();
                
            }

            return true;
            
        }

        return false;
        
    }
    
}
