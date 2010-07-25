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

package com.bigdata.util;

import com.bigdata.util.config.ConfigDeployUtil;
import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.jini.config.Config;
import com.sun.jini.thread.InterruptedStatusThread;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.NoSuchEntryException;
import net.jini.core.lookup.ServiceID;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.export.Exporter;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryManager;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

/**
 * Miscellaneous, convenient utility methods.
 */
public class Util {

    public static <T extends Comparable<T>> T max(final T... elements){
        T max = elements[0];
        for (final T element : elements) {
            if(0 < element.compareTo(max)) {
                max = element;
            }
        }
        return max;
    }

    public static <T extends Comparable<T>> T min(final T... elements){
        T min = elements[0];
        for (final T element : elements) {
            if(0 > element.compareTo(min)) {
                min = element;
            }
        }
        return min;
    }

    /* Convenience method that can be called when a service exits, or
     * when failure occurs during the service's initialization process.
     * This method un-does any work that may have already been completed;
     * for example, un-exports the service if it has already been 
     * exported, closes any open sockets or file descriptors, terminates
     * threads that may have been started, etc.
     * <p>
     * Note that multiple versions of this method are provided. One version
     * is intended to be used by entities that act only as a service (that
     * is, entities that export a proxy and use a <code>JoinManager</code>).
     * One version is intended to be used by entities that act only as a client
     * (that is, entites that use a <code>ServiceDiscoveryManager</code>). 
     * And the final version can be used by entities that act as both a
     * service and as a client.
     */
    public static void cleanupOnExit
                             (Object                  innerProxy,
                              Exporter                serverExporter,
                              JoinManager             joinManager,
                              DiscoveryManagement     discoveryManager)
    {
        cleanupOnExit(innerProxy, serverExporter, joinManager,
                      null, discoveryManager);

    }

    public static void cleanupOnExit
                             (ServiceDiscoveryManager serviceDiscoveryManager,
                              DiscoveryManagement     discoveryManager)
    {
        cleanupOnExit(null, null, null,
                      serviceDiscoveryManager, discoveryManager);
    }


    public static void cleanupOnExit
                             (Object                  innerProxy,
                              Exporter                serverExporter,
                              JoinManager             joinManager,
                              ServiceDiscoveryManager serviceDiscoveryManager,
                              DiscoveryManagement     discoveryManager)
    {
        if(innerProxy != null)  {
            try {
                if(serverExporter != null) serverExporter.unexport(true);
            } catch(Throwable t) { }
        }

        if(joinManager != null)  {
            try {
                joinManager.terminate();
            } catch(Throwable t) { }
        }

        if(serviceDiscoveryManager != null)  {
            try {
                serviceDiscoveryManager.terminate();
            } catch(Throwable t) { }
        }

        if(discoveryManager != null)  {
            try {
                discoveryManager.terminate();
            } catch(Throwable t) { }
        }
    }


    /**
     * Unexports the remote object that was exported by the given
     * <code>Exporter</code> parameter; which removes the object
     * from the RMI runtime so that the object can no longer accept
     * incoming remote calls.er accept incoming RMI calls.
     * <P>
     * This method first makes an attempt to unexport the object
     * 'gracefully'. That is, for a finite period of time, an attempt
     * is made to allow all calls to the object that are in progress,
     * or pending, to complete before the object is actually unexported.
     * If, after that finite period of time, the object has not been
     * successfully unexported, the object is then 'forcibly' unexported;
     * that is, the object is unexported even if there are calls to
     * the object that are in progress or still pending.
     * <P>
     * Upon successfully unexporting the given <code>Exporter</code>,
     * <code>true</code> is returned. If the given <code>Exporter</code>
     * cannot be unexported, or if the value input for that parameter
     * is <code>null</code> or has not exported any interfaces, then
     * <code>false</code> is returned.
     */
    public static boolean unexportRemoteObject(Exporter exporter) {
        if (exporter == null) return false;

        // delay no more than 1 minute
        final long endTime = System.currentTimeMillis() + (1L*60L*1000L);
        boolean unexported = false;
        try {
            // Unexport only if there are no pending or in-progress calls
            while (!unexported && System.currentTimeMillis() < endTime) {
                unexported = exporter.unexport(false);//do not force
                if (!unexported) Thread.yield();
            }//end loop
            if (!unexported) unexported = exporter.unexport(true);//force
        } catch ( IllegalStateException e ) {
            // Thrown if no object has been exported with the
            // Exporter instance
            return false;
        }
        return unexported;
    }


    /**
     * Convenience method that can be called in an entity's constructor
     * when failure occurs during the initialization process. This
     * method simply rethrows the given <code>Throwable</code> so the
     * constructor doesn't have to.
     */
    public static void handleInitThrowable(Throwable t, Logger logger) 
                                            throws IOException,
                                                   ConfigurationException
    {
        if( logger != null ) {
            logger.log(Level.FATAL, "initialization failure ... ", t);
        } else {
            System.err.println("FATAL: initialization failure ... "+t);
        }//endif
        if (t instanceof IOException) {
            throw (IOException)t;
        } else if (t instanceof ConfigurationException) {
            throw (ConfigurationException)t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException)t;
        } else if (t instanceof Error) {
            throw (Error)t;
        }//endif
    }

    /** 
     * Convenience method that returns a <code>String</code> containing
     * a common-separated list the elements (group names) of the given
     * array.
     */
    public static String writeGroupArrayToString(String[] groups) {
        if(groups == null) {
            return new String("[ALL_GROUPS]");
        }//endif
        if(groups.length <= 0) {
            return new String("[]");
        }//endif
        StringBuffer strBuf = null;
        if(groups[0].compareTo("") == 0) {
            strBuf = new StringBuffer("[The PUBLIC Group");
        } else {
            strBuf = new StringBuffer("["+groups[0]);
        }//endif
        for(int i=1;i<groups.length;i++) {
            if(groups[i].compareTo("") == 0) {
                strBuf.append(", The PUBLIC Group");
            } else {
                strBuf.append(", ").append(groups[i]);
            }//endif
        }//end loop
        strBuf.append("]");
        return strBuf.toString();
    }

    /** 
     * Convenience method that returns a <code>String</code> containing
     * a common-separated list the elements (locators) of the given
     * array.
     */
    public static String writeArrayElementsToString(Object[] arr) {
        if(arr == null) return new String("[]");
        if(arr.length <= 0) {
            return new String("[]");
        }//endif
        StringBuffer strBuf = new StringBuffer("["+arr[0]);
        for(int i=1;i<arr.length;i++){
            strBuf.append(", ").append(arr[i]);
        }//end loop
        strBuf.append("]");
        return strBuf.toString();
    }

    /**
     * Convenience method to simplify the throwing of exceptions with embedded
     * causes (avoids having to cast the return value of Throwable.initCause
     * back to the exception's type).  Use as follows:
     * <pre>
     *     throw Util.initCause(new SomeException("foo"), cause);
     * </pre>
     */
    public static <T extends Throwable> T initCause(T t, Throwable cause) {
        t.initCause(cause);
        return t;
    }

    /**
     * Verifies that all non-<code>null</code> elements of the given
     * <code>Collection</code> are assignable to the specified type,
     * throwing a <code>ClassCastException</code> if any are not.
     */
    public static void checkElementTypes(Collection<?> c, Class<?> type) {
        for (Object elt : c) {
            if (!type.isInstance(elt)) {
                throw new ClassCastException(
                    elt + " not assignable to " + type);
            }
        }
    }

    /**
     * Returns a UUID with the same bit value as the given
     * <code>ServiceID</code>.
     */
    public static UUID toUUID(ServiceID serviceId) {
        return new UUID( serviceId.getMostSignificantBits(),
                         serviceId.getLeastSignificantBits() );
    }

    /**
     * Returns a string representation of the given
     * <code>ServiceDiscoveryEvent</code> (since
     * <code>ServiceDiscoveryEvent</code> doesn't define
     * its own <code>toString</code> method).
     */
    public static String eventToString(ServiceDiscoveryEvent event) {
        return "ServiceDiscoveryEvent[source=" + event.getSource() +
            ",preEventItem=" + event.getPreEventServiceItem() +
            ",postEventItem=" + event.getPostEventServiceItem() + "]";
    }

    /** 
     * Convenience method that encapsulates common functions that services
     * or clients may wish to perform to be able to discover lookup services
     * in the system.
     * <p>
     * This method retrieves and returns a lookup discovery manager from
     * the given <code>Configuration</code>. If no lookup discovery manager
     * has been configured, this method will return an instance of the
     * <code>LookupDiscoveryManager</code> helper utility class,
     * initialized to discover NO_GROUPS and no locators. When such a
     * discovery manager is returned, the calling entity can call the 
     * <code>setGroups</code> and/or </code>setLocators</code> method 
     * to initiate the lookup discovery process.
     * <p>
     * Note that this method expects that the discovery manager 
     * that has been configured is an instance of both 
     * <code>DiscoveryGroupManagement</code> and 
     * <code>DiscoveryLocatorManagement</code>.
     *
     * @param config        The calling service's <code>Configuration</code>
     *                      from which this method will retrieve the items
     *                      needed to perform the desired initialization.
     *
     * @param componentName <code>String</code> whose value is the name of
     *                      the <i>component</i> used to index the calling
     *                      service's configuration <i>entries</i>.
     *
     * @param entryName     <code>String</code> whose value is the name of
     *                      the configuration entry that references the 
     *                      the desired lookup discovery manager instance
     *                      specified in the configuration.
     *
     * @return An instance of <code>DiscoveryManagement</code> that supports
     *         both group and locator discovery; where the instance returned
     *         is either retrieved from the given <code>Configuration</code>,
     *         or is a default instance of <code>LookupDiscoveryManager</code>.
     *
     * @throws <code>ConfigurationException</code> when there is a problem
     *         retrieving the desired entry from the configuration.
     *
     * @throws IOException when there is a problem with multicast discovery.
     */
    public static DiscoveryManagement getDiscoveryManager
                                            (Configuration config,
                                             String        componentName,
                                             String        entryName )
                                               throws ConfigurationException,
                                                      IOException
    {
        // The discovery manager must be an instance of both 
        // DiscoveryGroupManagement and DiscoveryLocatorManagement, so that
        // the groupsToJoin and locatorsToJoin can both be retrieved from
        //the discovery manager and displayed.
        DiscoveryManagement dMgr;
        try {
            dMgr = (DiscoveryManagement)Config.getNonNullEntry
                                                  (config,
                                                   componentName,
                                                   entryName,
                                                   DiscoveryManagement.class);
            if( !(dMgr instanceof DiscoveryGroupManagement) ) {
                throw new ConfigurationException
                                        (entryName + " entry must "
                                         +"implement DiscoveryGroupManagment");
            }
            if( !(dMgr instanceof DiscoveryLocatorManagement) ) {
                throw new ConfigurationException
                                     (entryName + " entry must "
                                      +"implement DiscoveryLocatorManagement");
            }
        } catch (NoSuchEntryException e) {
            return ( new LookupDiscoveryManager
                             (ConfigDeployUtil.getGroupsToDiscover(),
                              ConfigDeployUtil.getLocatorsToDiscover(),
                              null, config) );
        }
        return dMgr;
    }

    /** 
     * Retrieves and returns a lookup discovery manager from the given
     * <code>Configuration</code>, using a default entry name of
     * <i>discoveryManager</i>.
     */
    public static DiscoveryManagement getDiscoveryManager
                                            (Configuration config,
                                             String        componentName)
                                               throws ConfigurationException,
                                                      IOException
    {
        return getDiscoveryManager(config, componentName, "discoveryManager");
    }

    public static class WaitOnInterruptThread extends InterruptedStatusThread {
        private Logger logger;
	public WaitOnInterruptThread(final Logger logger) {
	    super("WaitOnInterruptThread");
	    setDaemon(true);
            this.logger = (logger == null ? 
                LogUtil.getLog4jLogger((this.getClass()).getName()) :
                logger);
	}
	public void run() {
            while (!hasBeenInterrupted()) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    if( logger.isDebugEnabled() ) {
                        logger.log(Level.DEBUG, 
                                   "Util.WaitOnInterruptThread: "
                                   +"interrupt received");
                    }
                }
            }
        }
    }
}
