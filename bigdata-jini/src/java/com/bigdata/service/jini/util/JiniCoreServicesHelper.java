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

package com.bigdata.service.jini.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.TimeUnit;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupDiscoveryManager;

import org.apache.log4j.Logger;

import com.bigdata.jini.start.config.ServiceConfiguration;

/**
 * Helper class for jini core services (reggie, etc).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniCoreServicesHelper {

    protected final static Logger log = Logger
            .getLogger(JiniCoreServicesHelper.class);

    protected final static boolean INFO = log.isInfoEnabled();

    protected final static boolean DEBUG = log.isDebugEnabled();

    /**
     * Return <code>true</code> if Jini appears to be running on the
     * localhost.
     * 
     * @throws Exception
     */
    public static boolean isJiniRunning() {
        
        return isJiniRunning(new String[] { "jini://localhost/" });
        
    }
    
    /**
     * Return <code>true</code> if Jini appears to be running on ANY of the
     * identified hosts.
     * 
     * @param url
     *            One or more unicast URIs of the form <code>jini://host/</code>
     *            or <code>jini://host:port/</code> -or- an empty array if you
     *            want to use <em>multicast</em> discovery.
     */
    public static boolean isJiniRunning(String[] url) {
        
        final LookupLocator[] locators = new LookupLocator[url.length];

        for (int i = 0; i < url.length; i++) {
           
            try {

                locators[i] = new LookupLocator(url[i]);

            } catch (MalformedURLException e) {

                throw new RuntimeException(e);

            }
            
        }
        
        try {
            
            return isJiniRunning(LookupDiscovery.ALL_GROUPS, locators, 2000,
                    TimeUnit.MILLISECONDS);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Return <code>true</code> any Jini registrars can be discovered on ANY
     * of the identified hosts (or on any reachable host if the array is empty)
     * within the specified timeout.
     * 
     * @param groups
     *            An array of groups or {@link LookupDiscovery#ALL_GROUPS} if
     *            you will be using multicast discovery.
     * @param locators
     *            An array of {@link LookupLocator}s. These use URIs of the
     *            form <code>jini://host/</code> or
     *            <code>jini://host:port/</code>. This MAY be an empty array
     *            if you want to use <em>multicast</em> discovery.
     * 
     * @throws IOException
     */
    public static boolean isJiniRunning(final String[] groups,
            final LookupLocator[] locators, long timeout, final TimeUnit unit)
            throws InterruptedException, IOException {

        return getServiceRegistrars(1/* maxCount */, groups, locators, timeout,
                unit).length > 0;
        
    }

    /**
     * Return Jini registrars discovered within the specified timeout.
     * 
     * @param maxCount
     *            The maximum #of registrars to discover.
     * @param groups
     *            An array of groups or {@link LookupDiscovery#ALL_GROUPS} if
     *            you will be using multicast discovery.
     * @param locators
     *            An array of {@link LookupLocator}s. These use URIs of the
     *            form <code>jini://host/</code> or
     *            <code>jini://host:port/</code>. This MAY be an empty array
     *            if you want to use <em>multicast</em> discovery.
     * 
     * @throws IOException
     */
    static public ServiceRegistrar[] getServiceRegistrars(int maxCount,
            final String[] groups, final LookupLocator[] locators,
            long timeout, final TimeUnit unit) throws InterruptedException,
            IOException {
        
        final long begin = System.nanoTime();

        timeout = unit.toNanos(timeout);

        final Object signal = new Object();

        final LookupDiscoveryManager discovery = new LookupDiscoveryManager(groups,
                locators,
                /*
                 * Add a listener that wakes us up if a registrar is discovered.
                 */
                new DiscoveryListener() {

                    public void discarded(DiscoveryEvent e) {

                        if(DEBUG)
                            log.debug("discarded: "+e);

                        // ignored.

                    }

                    public void discovered(DiscoveryEvent e) {

                        if(DEBUG)
                            log.debug("discovered: "+e);
                        
                        synchronized (signal) {

                            signal.notify();

                        }

                    }
        
        });
                
        try {

            long elapsed;

            // demand some results.
            ServiceRegistrar[] registrars = new ServiceRegistrar[0];

            while ((timeout -= (elapsed = (System.nanoTime() - begin))) > 0
                    && registrars.length < maxCount) {

                synchronized (signal) {

                    try {
                        signal.wait(TimeUnit.NANOSECONDS.toMillis(timeout));
                    } catch(InterruptedException ex) {
                        // fall through
                    }

                    if(DEBUG)
                        log.debug("woke up.");

                }

                registrars = discovery.getRegistrars();

            }

            if (log.isInfoEnabled())
                log.info("Found " + registrars.length + " registrars in "
                        + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms.");

            return registrars;

        } finally {

            discovery.terminate();

        }

    }

    /**
     * Combines the two arrays, appending the contents of the 2nd array to the
     * contents of the first array.
     * 
     * @param a
     * @param b
     * @return
     */
    @SuppressWarnings("unchecked")
    protected static <T> T[] concat(final T[] a, final T[] b) {

        return ServiceConfiguration.concat(a, b);
        
    }

}
