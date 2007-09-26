/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Sep 21, 2007
 */

package com.bigdata.service.mapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.UUID;

import net.jini.config.Configuration;

import com.bigdata.service.AbstractServer;
import com.bigdata.service.BigdataClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.JiniUtil;

/**
 * Used to manage a {@link MapService}.
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
        
        new MapServer(args).run();
        
    }
    
    protected Remote newService(Properties properties) {
        
        return new AdministrableMapService(this,properties);
        
    }

    /**
     * Adds jini administration interfaces to the basic {@link MapService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableMapService
        extends MapService
        implements RemoteAdministrable, RemoteDestroyAdmin
    {
        
        protected AbstractServer server;
        private UUID serviceUUID;
        
        public AdministrableMapService(AbstractServer server,Properties properties) {
            
            super(properties);
            
            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

            log.info(""+getServiceUUID());

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

            log.info(""+getServiceUUID());

            new Thread() {

                public void run() {

                    server.destroy();
                    
                    log.info(getServiceUUID()+" - Service stopped.");

                }

            }.start();

        }

        public UUID getServiceUUID() {

            if(serviceUUID==null) {

                serviceUUID = JiniUtil.serviceID2UUID(server.getServiceID());
                
            }
            
            return serviceUUID;
            
        }

        public IBigdataClient getBigdataClient() {
            
            // @todo this assumes the default federation.
            return new BigdataClient(new String[]{});
            
        }

    }

}
