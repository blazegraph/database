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
 * Created on Apr 6, 2008
 */

package com.bigdata.service.jini;

import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;
import net.jini.io.context.ClientSubject;
import net.jini.lookup.entry.Name;

import org.apache.log4j.MDC;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ITransactionService;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.DataService;
import com.bigdata.service.DefaultServiceFederationDelegate;
import com.bigdata.service.DistributedTransactionService;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * Server exposing a discoverable {@link ITransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo verify that time is strictly ascending on restart or failover.
 * 
 * @todo rename various configuration files as well.
 */
public class TransactionServer extends AbstractServer {

    /**
     * Options for this server.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends AdministrableTransactionService.Options {

    }
    
    /**
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
    public TransactionServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);
        
    }

    /**
     * Starts a new {@link TransactionServer}.  This can be done programmatically
     * by executing
     * <pre>
     *    new TimestampServer(args, new FakeLifeCycle()).run();
     * </pre>
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(final String[] args) {
        
        new TransactionServer(args, new FakeLifeCycle()).run();

        System.exit(0);
//      Runtime.getRuntime().halt(0);

    }

    /**
     * Extended to attach the various performance counters reported by the
     * {@link DistributedTransactionService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class TransactionServiceFederationDelegate extends
            DefaultServiceFederationDelegate<DistributedTransactionService> {

        public TransactionServiceFederationDelegate(
                DistributedTransactionService service) {

            super(service);

        }

        /**
         * Extended to setup {@link AbstractTransactionService} specific
         * counters
         * 
         * @todo write the client URL onto a file in the service's data
         *       directory.
         */
        public void didStart() {

            super.didStart();

            setupCounters();

            // logHttpdURL(dir);

        }

        /**
         * Sets up {@link AbstractTransactionService} specific counters.
         */
        protected void setupCounters() {

            if (getServiceUUID() == null) {

                throw new IllegalStateException(
                        "The ServiceUUID is not available yet");

            }

            if (!service.isOpen()) {

                /*
                 * The service has already been closed.
                 */

                log.warn("Service is not open.");

                return;

            }

            /*
             * Service specific counters.
             */

            final CounterSet serviceRoot = service.getFederation()
                    .getServiceCounterSet();

            serviceRoot.attach(service.getCounters());

        }

    }

    @Override
    protected AbstractTransactionService newService(final Properties properties) {

        final DistributedTransactionService service = new AdministrableTransactionService(
                this, properties);

        /*
         * Setup a delegate that let's us customize some of the federation
         * behaviors on the behalf of the service.
         * 
         * Note: We can't do this with the local or embedded federations since
         * they have only one client per federation and an attempt to set the
         * delegate more than once will cause an exception to be thrown!
         */
        getClient().setDelegate(
                new TransactionServiceFederationDelegate(service));

        return service;
        
    }

    /**
     * Adds jini administration interfaces to the basic {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableTransactionService extends
            DistributedTransactionService implements RemoteAdministrable,
            RemoteDestroyAdmin {
        
        final protected TransactionServer server;

        public AdministrableTransactionService(final TransactionServer server,
                final Properties properties) {
            
            super(properties);
            
            this.server = server;
            
        }
        
        @Override
        public JiniFederation<?> getFederation() {

            return server.getClient().getFederation();
            
        }

        public Object getAdmin() throws RemoteException {

            if (log.isInfoEnabled())
                log.info("" + getServiceUUID());

            return server.proxy;
            
        }
        
        /**
         * Adds the following parameters to the {@link MDC}
         * <dl>
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
        @Override
        protected void setupLoggingContext() {
            
            super.setupLoggingContext();
            
            try {
                
                final InetAddress clientAddr = ((ClientHost) ServerContext
                        .getServerContextElement(ClientHost.class))
                        .getClientHost();
                
                MDC.put("clientname",clientAddr.getHostName());
                
            } catch (ServerNotActiveException e) {
                
                /*
                 * This exception gets thrown if the client has made a direct
                 * (vs RMI) call so we just ignore it.
                 */
                
            }
            
        }

        @Override
        protected void clearLoggingContext() {

            MDC.remove("clientname");

            super.clearLoggingContext();
            
        }
        
        /*
         * DestroyAdmin
         */

        /**
         * Destroy the service.
         */
        @Override
        synchronized public void destroy() {

            if (!server.isShuttingDown()) {

                /*
                 * Run thread which will destroy the service (asynchronous).
                 * 
                 * Note: By running this is a thread, we avoid closing the
                 * service end point during the method call.
                 */

                server.runDestroy();

            } else if (isOpen()) {

                /*
                 * The server is already shutting down, so invoke our super
                 * class behavior to destroy the persistent state.
                 */

                super.destroy();

            }

        }

        @Override
        synchronized public void shutdown() {

            // normal shutdown for the transaction service (blocks).
            super.shutdown();

            // jini service and server shutdown.
            server.shutdownNow(false/*destroy*/);

        }

        @Override
        synchronized public void shutdownNow() {

            // immediate service shutdown (blocks).
            super.shutdownNow();

            // jini service and server shutdown.
            server.shutdownNow(false/*destroy*/);

        }

        /**
         * Extends the base behavior to return a {@link Name} of the service
         * from the {@link Configuration}. If no name was specified in the
         * {@link Configuration} then the value returned by the base class is
         * returned instead.
         */
        @Override
        public String getServiceName() {

            String s = server.getServiceName();

            if (s == null)
                s = super.getServiceName();

            return s;
            
        }

    }

}
