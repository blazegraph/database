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
 * Created on Jan 8, 2009
 */

package com.bigdata.jini.start.process;

import java.io.IOException;
import java.lang.reflect.Method;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;

import net.jini.admin.Administrable;
import net.jini.core.lookup.ServiceItem;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.sun.jini.admin.DestroyAdmin;

/**
 * Class for managing a service written using the jini framework. This extends
 * the base class to prefer {@link RemoteDestroyAdmin} for
 * {@link #kill(boolean)}. It will use {@link RemoteDestroyAdmin#shutdown()}
 * (normal shutdown, allowing a service to be restarted) if defined and
 * otherwise {@link DestroyAdmin#destroy()} (destroys the service along with its
 * persistent state).
 * <p>
 * Note: {@link RemoteDestroyAdmin} is defined for the bigdata services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniServiceProcessHelper extends ProcessHelper {

    /**
     * @param name
     * @param builder
     * @param listener
     * @throws IOException
     */
    public JiniServiceProcessHelper(String name, ProcessBuilder builder,
            IServiceListener listener) throws IOException {

        super(name, builder, listener);

    }

    private ServiceItem serviceItem = null;
    
    synchronized public void setServiceItem(final ServiceItem serviceItem) {
        
        if (serviceItem == null)
            throw new IllegalArgumentException();
        
        if (this.serviceItem != null)
            throw new IllegalStateException();

        this.serviceItem = serviceItem;

    }

    /**
     * If {@link #setServiceItem(ServiceItem)} was invoked and the service
     * implements {@link RemoteAdministrable}, then uses
     * {@link RemoteAdministrable} to obtain the {@link Administrable} object
     * and then requests {@link RemoteDestroyAdmin#shutdown()} if defined (the
     * bigdata services define this method which shuts down the services without
     * destroying its persistent state) and otherwise
     * {@link RemoteDestroyAdmin#destroy()}, which destroy both the service and
     * its persistent state. In any of those methods was defined, we then wait
     * for the exit value of the process before messaging the listener.
     * <p>
     * If none of those options was available, we delegate the behavior to the
     * super class.
     * <p>
     * Note: Normal termination of some services can have significant latency
     * (seconds or more). For example, services may refuse new operations while
     * permitting running operations to complete.
     * 
     * @return The exitValue of the process.
     * 
     * @throws InterruptedException
     *             if interrupted - the process may or may not have been killed
     *             and the listener will not have been notified.
     */
    public int kill(final boolean immediateShutdown) throws InterruptedException {
        
        if (serviceItem == null) {

            return super.kill(immediateShutdown);

        }

        /*
         * Attempt to obtain the Administrable object from the service. The
         * methods that we want will be on that object if they are offered by
         * the service.
         */
        final Object admin;
        {
            
            final Remote proxy = (Remote) serviceItem.service;

            final Method getAdmin;
            try {

                getAdmin = proxy.getClass().getMethod("getAdmin",
                        new Class[] {});

            } catch (Throwable t) {
                // can't resolve the method for some reason.
                log.warn(this, t);
                return super.kill(immediateShutdown);
            }

            try {
                admin = getAdmin.invoke(proxy, new Object[] {});
            } catch (Throwable t) {
                // can't invoke the method for some reason.
                log.warn(this, t);
                return super.kill(immediateShutdown);
            }
            
        }

        if (admin instanceof RemoteDestroyAdmin) {
            /*
             * This interface allows us to shutdown the service without
             * destroying its persistent state. A service shutdown in this
             * manner MAY be restarted.
             */
            try {
                if (immediateShutdown) {
                    // Fast termination (can have latency).
                    log.warn("will shutdownNow() service: " + this);
                    ((RemoteDestroyAdmin) admin).shutdownNow();
                } else {
                    // Normal termination (can have latency).
                    log.warn("will shutdown() service: " + this);
                    ((RemoteDestroyAdmin) admin).shutdown();
                }
            } catch (NoSuchObjectException ex) {
                // probably already dead.
                log.warn("Process already gone? name=" + name + " : " + ex);
                // but delegate to the super class anyway.
                return super.kill(immediateShutdown);
            } catch (Throwable t) {
                // delegate to the super class.
                log.warn(this, t);
                return super.kill(immediateShutdown);
            }
        } else if (admin instanceof DestroyAdmin) {
            /*
             * This interface destroys the persistent state of the service. A
             * service shutdown in this manner MAY NOT be restarted. We DO NOT
             * invoke this method if the service offers a normal shutdown
             * alternative. However, there are many kinds of services for which
             * destroy() is perfectly acceptable. For example, a jini registrar
             * may be destroyed as its state will be regained from the running
             * services if a new registrar is started.
             */
            try {
                // Destroy the service and its persistent state.
                log.warn("will destroy() service: "+this);
                ((DestroyAdmin) admin).destroy();
            } catch (NoSuchObjectException ex) {
                // probably already dead.
                log.warn("Process already gone? name=" + name + " : " + ex);
                // but delegate to the super class anyway.
                return super.kill(immediateShutdown);
            } catch (Throwable t) {
                // delegate to the super class.
                log.error(this, t);
                return super.kill(immediateShutdown);
            }
        }

        // wait for the process to die
        final int exitValue = exitValue();

        // notify listener.
        listener.remove(this);

        return exitValue;
        
    }
    
}
