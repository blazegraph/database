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
 * Created on Sep 21, 2007
 */

package com.bigdata.service.mapred.jini;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.UUID;

import net.jini.config.Configuration;

import org.apache.log4j.MDC;

import com.bigdata.jini.util.JiniUtil;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.service.mapred.ReduceService;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * Used to start and manage a {@link ReduceService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReduceServer extends AbstractServer {

    /**
     * Creates a new {@link ReduceServer}.
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
    public ReduceServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);

    }

    /**
     * Starts a new {@link ReduceServer}. This can be done programmatically by
     * executing
     * 
     * <pre>
     * new ReduceServer(args, new FakeLifeCycle()).run();
     * </pre>
     * 
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(final String[] args) {

        new ReduceServer(args, new FakeLifeCycle()).run();

        System.exit(0);
//      Runtime.getRuntime().halt(0);

    }

    protected Remote newService(final Properties properties) {

        return new AdministrableReduceService(this, properties);

    }

    /**
     * Adds jini administration interfaces to the basic {@link ReduceService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo define the {@link MDC} logging context in the base class and extend
     *       it here.
     */
    public static class AdministrableReduceService extends ReduceService
            implements RemoteAdministrable, RemoteDestroyAdmin {

        protected final AbstractServer server;

        private UUID serviceUUID;

        public AdministrableReduceService(final AbstractServer server,
                final Properties properties) {

            super(properties);

            this.server = server;

        }

        public Object getAdmin() throws RemoteException {

            if (log.isInfoEnabled())
                log.info("" + getServiceUUID());

            return server.getProxy();

        }

        /*
         * DestroyAdmin
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

            // normal service shutdown.
            super.shutdown();

            // jini service and server shutdown.
            server.shutdownNow(false/* destroy */);

        }

        @Override
        synchronized public void shutdownNow() {

            // immediate service shutdown.
            super.shutdownNow();

            // jini service and server shutdown.
            server.shutdownNow(false/* destroy */);

        }

        public UUID getServiceUUID() {

            if (serviceUUID == null) {

                serviceUUID = JiniUtil.serviceID2UUID(server.getServiceID());

            }

            return serviceUUID;

        }

        public JiniClient<?> getBigdataClient() {
            
            return server.getClient();
//            return JiniClient.newInstance(new String[]{});
            
        }

        public JiniFederation<?> getFederation() {

            return server.getClient().getFederation();

        }

    }

}
