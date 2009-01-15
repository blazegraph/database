/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 22, 2007
 */

package com.bigdata.service.jini;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.jini.config.Configuration;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lookup.entry.Name;

import org.apache.log4j.MDC;

import com.bigdata.journal.IResourceManager;
import com.bigdata.service.DataService;
import com.bigdata.service.DataService.DataServiceFederationDelegate;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * The bigdata data server.
 * <p>
 * The {@link DataServer} starts the {@link DataService}. The server and
 * service are configured using a {@link Configuration} file whose name is
 * passed to the {@link DataServer#DataServer(String[])} constructor or
 * {@link #main(String[])}.
 * <p>
 * 
 * @see src/resources/config for sample configurations.
 * 
 * @todo identify the minimum set of permissions required to run a
 *       {@link DataServer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServer extends AbstractServer {

    /**
     * Options for this server.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends AdministrableDataService.Options {
        
    }
    
    /**
     * Creates a new {@link DataServer}.
     * 
     * @param args
     *            Either the command line arguments or the arguments from the
     *            {@link ServiceDescriptor}. Either way they identify the jini
     *            {@link Configuration} (you may specify either a file or URL)
     *            and optional overrides for that {@link Configuration}.
     * @param lifeCycle
     *            The life cycle object. This is used if the server is started
     *            by the jini {@link ServiceStarter}. Otherwise specify a
     *            {@link FakeLifeCycle}.
     */
    public DataServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);

    }

    /**
     * Starts a new {@link DataServer}.  This can be done programmatically
     * by executing
     * <pre>
     *    new DataServer(args, new FakeLifeCycle()).run();
     * </pre>
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(final String[] args) {
        
        new DataServer(args, new FakeLifeCycle()).run();
        
        System.exit(0);
//        Runtime.getRuntime().halt(0);
        
    }
    
    protected DataService newService(Properties properties) {

        final DataService service = new AdministrableDataService(this,
                properties);
        
        /*
         * Setup a delegate that let's us customize some of the federation
         * behaviors on the behalf of the data service.
         * 
         * Note: We can't do this with the local or embedded federations since
         * they have only one client per federation and an attempt to set the
         * delegate more than once will cause an exception to be thrown!
         */
        getClient().setDelegate(new DataServiceFederationDelegate(service));
        
        return service;

    }

    /**
     * Extends the behavior to close and delete the journal in use by the data
     * service.
     */
    public void destroy() {

        final DataService service = (DataService)impl;
        
        final IResourceManager resourceManager = service.getResourceManager();
        
        super.destroy();
        
        // destroy all resources.
        resourceManager.deleteResources();

    }

    /**
     * Adds jini administration interfaces to the basic {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableDataService extends DataService implements
            RemoteAdministrable, RemoteDestroyAdmin {
        
        protected DataServer server;
        
        public AdministrableDataService(DataServer server, Properties properties) {

            super(properties);
            
            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

            if (INFO)
                log.info("" + getServiceUUID());

            return server.proxy;
            
        }
        
        /**
         * Adds the following parameters to the {@link MDC}
         * <dl>
         * 
         * <dt>hostname
         * <dt>
         * <dd>The hostname or IP address of this server.</dd>
         * 
         * <dt>clientname
         * <dt>
         * <dd>The hostname or IP address of the client making the request.</dd>
         * 
         * </dl>
         * 
         * Note: {@link InetAddress#getHostName()} is used. This method makes a
         * one-time best effort attempt to resolve the host name from the
         * {@link InetAddress}.
         * 
         * @todo we could pass the class {@link ClientSubject} to obtain the
         *       authenticated identity of the client (if any) for an incoming
         *       remote call.
         */
        protected void setupLoggingContext() {
            
            super.setupLoggingContext();
            
            try {
                
                final InetAddress clientAddr = ((ClientHost) ServerContext
                        .getServerContextElement(ClientHost.class))
                        .getClientHost();

                MDC.put("clientname", clientAddr.getHostName());

            } catch (ServerNotActiveException e) {

                /*
                 * This exception gets thrown if the client has made a direct
                 * (vs RMI) call so we just ignore it.
                 */

            }

            MDC.put("hostname", server.getHostName());

        }

        protected void clearLoggingContext() {

            MDC.remove("hostname");

            MDC.remove("clientname");

            super.clearLoggingContext();

        }

        /*
         * DestroyAdmin
         */

        /**
         * Destroy the service and deletes any files containing resources (<em>application data</em>)
         * that was in use by that service.
         * 
         * @throws RemoteException
         */
        public void destroy() throws RemoteException {

            server.runDestroy();

        }

        synchronized public void shutdown() {
            
            // normal service shutdown (blocks).
            super.shutdown();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }
        
        synchronized public void shutdownNow() {
            
            // immediate service shutdown (blocks).
            super.shutdownNow();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }

        @Override
        public JiniFederation getFederation() {

            return server.getClient().getFederation();

        }

        /**
         * Extends the base behavior to return an RMI compatible proxy.
         */
        public Future<? extends Object> submit(
                final Callable<? extends Object> task)
                throws InterruptedException, ExecutionException {

            return getFederation().getProxy(super.submit(task));

        }

        /**
         * Extends the base behavior to return a {@link Name} of the service
         * from the {@link Configuration}. If no name was specified in the
         * {@link Configuration} then the value returned by the base class is
         * returned instead.
         */
        public String getServiceName() {

            String s = server.getServiceName();

            if (s == null)
                s = super.getServiceName();

            return s;

        }

    }

}
