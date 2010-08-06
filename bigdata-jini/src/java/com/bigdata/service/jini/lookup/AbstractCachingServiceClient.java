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

package com.bigdata.service.jini.lookup;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

import com.bigdata.jini.lookup.entry.ServiceItemFilterChain;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.util.InnerCause;
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

//    final protected JiniFederation fed;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    
    protected ServiceDiscoveryManager getServiceDiscoveryManager() {
        
        return serviceDiscoveryManager;
        
    }
    
    /**
     * A {@link LookupCache} that will be populated with all services that match
     * a filter. This is used to keep track of all services registered with any
     * {@link ServiceRegistrar} to which the client is listening.
     */
    protected final LookupCache lookupCache;

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
    protected final long cacheMissTimeout;

    /**
     * Provides direct cached lookup of discovered services matching both the
     * {@link #template} and the optional {@link #filter} by their
     * {@link ServiceID}.
     */
    protected final ServiceCache serviceCache;

    /**
     * An object that provides direct cached lookup of discovered services
     * matching both the {@link #template} and the optional {@link #filter} by
     * their {@link ServiceID}.
     */
    final public ServiceCache getServiceCache() {

        return serviceCache;

    }

    /**
     * An object that provides cached lookup of discovered services matching
     * both the {@link #template} and the optional {@link #filter}.
     */
    final public LookupCache getLookupCache() {

        return lookupCache;

    }

    /**
     * The most interesting interface on the services to be discovered (this is
     * used for log messages).
     */
    private final Class serviceIface;

    /**
     * Sets up service discovery for the designed class of services.
     * 
     * @param serviceDiscoveryManager
     *            Used to discovery services matching the template and filter.
     * @param serviceDiscoveryListener
     *            Service discovery notices are delivered to this class.
     * @param serviceIface
     *            The most interesting interface on the service (this is used
     *            for log messages).
     * @param template
     *            A template used to restrict the services which are discovered
     *            and cached by this class (required).
     * @param filter
     *            A filter used to further restrict the services which are
     *            discovered and cached by this class (optional).
     * @param cacheMissTimeout
     *            The timeout in milliseconds that the client will await the
     *            discovery of a service if there is a cache miss.
     * 
     * @throws RemoteException
     *             if we could not setup the {@link LookupCache}
     */
    public AbstractCachingServiceClient(
            final ServiceDiscoveryManager serviceDiscoveryManager,
            final ServiceDiscoveryListener serviceDiscoveryListener,
            final Class serviceIface, final ServiceTemplate template,
            final ServiceItemFilter filter, final long cacheMissTimeout)
            throws RemoteException {

        if (serviceDiscoveryManager == null)
            throw new IllegalArgumentException();

        if (serviceIface == null)
            throw new IllegalArgumentException();

        if (template == null)
            throw new IllegalArgumentException();

        if (cacheMissTimeout < 0)
            throw new IllegalArgumentException();

        this.serviceDiscoveryManager = serviceDiscoveryManager;

        this.serviceIface = serviceIface;

        this.template = template;

        this.filter = filter;

        this.cacheMissTimeout = cacheMissTimeout;

        serviceCache = new ServiceCache(serviceDiscoveryListener);

        lookupCache = getServiceDiscoveryManager().createLookupCache(//
                template,//
                filter, //
                serviceCache // ServiceDiscoveryListener
                );

    }

    /**
     * Terminates asynchronous processing by the {@link LookupCache}.
     */
    final public void terminate() {

        lookupCache.terminate();

    }

    /**
     * Return an arbitrary service from the cache -or- <code>null</code> if
     * there is no such service in the cache and a remote lookup times out.
     */
//    @SuppressWarnings("unchecked")
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
    final public S getService(final ServiceItemFilter filter) {

    	final ServiceItem item = getServiceItem(filter);

        if (item == null)
        	return null;
        
        @SuppressWarnings("unchecked")
        final S service = (S)item.service;
        
        return service;
        
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

        ServiceItem item = lookupCache.lookup(filter);

        if (item == null) {

            if (log.isInfoEnabled())
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

            item = serviceDiscoveryManager.lookup(template, filter,
                    cacheMissTimeout);

        } catch (RemoteException ex) {

            log.error(ex);

            return null;

        } catch (InterruptedException ex) {

            if (log.isInfoEnabled())
                log.info("Interrupted - no match.");

            return null;

        }

        if (item == null) {

            // Could not discover a matching service.

            log.warn("Could not discover matching service: template="
                    + template + ", filter=" + filter + ", timeout="
                    + cacheMissTimeout);

            return null;

        }

        if (log.isInfoEnabled())
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

        final ServiceItem serviceItem = serviceCache
                .getServiceItemByID(JiniUtil.uuid2ServiceID(serviceUUID));

        return serviceItem;

    }

    /**
     * Return an array {@link ServiceItem}s for up to <i>maxCount</i>
     * discovered services which satisify the optional <i>filter</i>.
     * 
     * @param maxCount
     *            The maximum #of data services whose {@link UUID} will be
     *            returned. When zero (0) the {@link UUID} for all known data
     *            services will be returned.
     * @param filter
     *            An optional filter. If given it will be applied in addition to
     *            the optional filter specified to the ctor.
     * 
     * @return An array of {@link ServiceItem}s for matching discovered
     *         services.
     */
    public ServiceItem[] getServiceItems(final int maxCount,
            ServiceItemFilter filter) {

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
       
        final ServiceItem[] items = serviceCache.getServiceItems(maxCount,
                filter);

        if (log.isInfoEnabled())
            log.info("There are at least " + items.length
                    + " services : maxCount=" + maxCount);

        return items;
        
    }
    
    /**
     * Return an array {@link UUID}s for up to <i>maxCount</i> discovered
     * services which satisify the optional <i>filter</i>.
     * 
     * @param maxCount
     *            The maximum #of data services whose {@link UUID} will be
     *            returned. When zero (0) the {@link UUID} for all known data
     *            services will be returned.
     * @param filter
     *            An optional filter. If given it will be applied in addition to
     *            the optional filter specified to the ctor.
     * 
     * @return An array of service {@link UUID}s for matching discovered
     *         services.
     */
    public UUID[] getServiceUUIDs(final int maxCount,
            final ServiceItemFilter filter) {

        final ServiceItem[] items = getServiceItems(maxCount, filter);

        final UUID[] uuids = new UUID[items.length];

        for (int i = 0; i < items.length; i++) {

            uuids[i] = JiniUtil.serviceID2UUID(items[i].serviceID);

        }

        return uuids;

    }

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
    public void destroyDiscoveredServices(
            final ExecutorService executorService,
            final ServiceItemFilter filter) throws InterruptedException {

        if (executorService == null)
            throw new IllegalArgumentException();

        // return everything in the cache.
        final ServiceItem[] items = serviceCache.getServiceItems(
                0/* maxCount */, filter);

        log.warn("Will destroy " + items.length + " " + serviceIface.getName()
                + " services" + (filter == null ? "" : " matching " + filter));

        final List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(
                items.length);

        for (ServiceItem serviceItem : items) {

            final ServiceItem item = serviceItem;

            tasks.add(new Callable<Boolean>() {

                public Boolean call() throws Exception {

                    try {

                        // send the request.
                        destroyService(item);
                        
                        return true;
                        
                    } catch (InvocationTargetException ex) {
                        
                        final java.rmi.ConnectException t = (java.rmi.ConnectException) InnerCause
                                .getInnerCause(ex,
                                        java.rmi.ConnectException.class);

                        if(t != null) {
                        
                            // probably already dead.
                            log.warn(t + ":" + item);
                            
                            return false;
                            
                        }
                        
                        throw ex;

                    }

                }

            });

        }

        /*
         * This blocks until all services have been sent a destroy request.
         * However, they may handle the destroy request asynchronously.
         */
        final List<Future<Boolean>> futures = executorService.invokeAll(tasks);

        for (int i = 0; i < items.length; i++) {

            final Future<Boolean> f = futures.get(i);

            try {

                f.get();

            } catch (Throwable t) {

                log.error(items[i], t);

            }

        }

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
    public void shutdownDiscoveredServices(
            final ExecutorService executorService,
            final ServiceItemFilter filter, final boolean immediateShutdown)
            throws InterruptedException {

        if (executorService == null)
            throw new IllegalArgumentException();

        // return everything in the cache.
        final ServiceItem[] items = serviceCache.getServiceItems(
                0/* maxCount */, filter);

        log.warn("Will shutdown " + items.length + " " + serviceIface.getName()
                + " services");

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                items.length);

        for (ServiceItem serviceItem : items) {

            final ServiceItem item = serviceItem;

            tasks.add(new Callable<Void>() {

                public Void call() throws Exception {

                    // send the request.
                    shutdownService(item, immediateShutdown);

                    return null;

                }

            });

        }

        /*
         * This blocks until all services have been sent a shutdown request.
         * However, they may handle the shutdown request asynchronously.
         */
        final List<Future<Void>> futures = executorService.invokeAll(tasks);

        for (int i = 0; i < items.length; i++) {

            final Future f = futures.get(i);

            try {

                f.get();

            } catch (Throwable t) {

                log.error(items[i], t);

            }

        }

    }

    /**
     * Sends {@link RemoteDestroyAdmin#destroy()} request to the service. Note
     * that the service may process the request asynchronously.
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
            log.warn("will destroy() service: " + serviceItem);

            ((DestroyAdmin) admin).destroy();

            return true;

        }

        log.warn("Service does not implement " + DestroyAdmin.class + " : "
                + serviceItem);

        return false;

    }

    /**
     * Sends a shutdown request to the service. This will invoke either
     * {@link RemoteDestroyAdmin#shutdown()} or
     * {@link RemoteDestroyAdmin#shutdownNow()} as indicated by the parameter.
     * Note that the service may process the shutdown request asynchronously.
     * 
     * @param serviceItem
     *            The service item.
     * @param immediateShutdown
     *            When <code>true</code> uses shutdownNow().
     * 
     * @return <code>true</code> if we were able to send the request to the
     *         service.
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
                log.warn("will shutdownNow() service: " + serviceItem);

                ((RemoteDestroyAdmin) admin).shutdownNow();

            } else {

                // Normal termination (can have latency).
                log.warn("will shutdown() service: " + serviceItem);

                ((RemoteDestroyAdmin) admin).shutdown();

            }

            return true;

        }

        log.warn("Service does not implement " + RemoteDestroyAdmin.class
                + " : " + serviceItem);

        return false;

    }
    
}
