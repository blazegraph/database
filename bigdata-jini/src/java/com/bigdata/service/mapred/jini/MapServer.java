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
import net.jini.discovery.DiscoveryManagement;

import org.apache.log4j.MDC;

import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniUtil;
import com.bigdata.service.mapred.MapService;

/**
 * Used to start and manage a {@link MapService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MapServer extends AbstractServer {

    /**
     * Creates a new {@link MapServer}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public MapServer(String[] args) {

        super(args);
        
    }

    /**
     * Starts a new {@link MapServer}.  This can be done programmatically
     * by executing
     * <pre>
     *    new MapServer(args).run();
     * </pre>
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(String[] args) {
        
        new MapServer(args) {
            
            /**
             * Overriden to use {@link System#exit()} since this is the command
             * line interface.
             */
            protected void fatal(String msg, Throwable t) {

                log.fatal(msg, t);

                try {

                    shutdownNow();
                    
                } catch (Throwable t2) {
                    
                    log.error(t2.getMessage(), t2);
                    
                }

                System.exit(1);

            }
            
        }.run();
        
    }
    
//    @Override
//    protected void setupClients(DiscoveryManagement discoveryManager) throws Exception {
//        
//    }

    protected Remote newService(Properties properties) {

        return new AdministrableMapService(this, properties);

    }

    /**
     * Adds jini administration interfaces to the basic {@link MapService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo define the {@link MDC} logging context in the base class and extend
     *       it here.
     */
    public static class AdministrableMapService extends MapService implements
            RemoteAdministrable, RemoteDestroyAdmin {

        protected AbstractServer server;

        private UUID serviceUUID;

        public AdministrableMapService(AbstractServer server,
                Properties properties) {

            super(properties);

            this.server = server;

        }

        public Object getAdmin() throws RemoteException {

            log.info("" + getServiceUUID());

            return server.getProxy();

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

            log.info("" + getServiceUUID());

            new Thread() {

                public void run() {

                    server.destroy();

                    log.info(getServiceUUID() + " - Service stopped.");

                }

            }.start();

        }

        synchronized public void shutdown() {
            
            // normal service shutdown.
            super.shutdown();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }
        
        synchronized public void shutdownNow() {
            
            // immediate service shutdown.
            super.shutdownNow();
            
            // jini service and server shutdown.
            server.shutdownNow();
            
        }

        public UUID getServiceUUID() {

            if (serviceUUID == null) {

                serviceUUID = JiniUtil.serviceID2UUID(server.getServiceID());

            }

            return serviceUUID;
            
        }

        public JiniClient getBigdataClient() {
            
            return JiniClient.newInstance(new String[]{});
            
        }

    }

}
