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
 * Created on Mar 22, 2007
 */

package com.bigdata.service;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Properties;

import net.jini.config.Configuration;

import com.bigdata.journal.IJournal;

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
 * @todo describe and implement the media replication mechanism and service
 *       failover. only the primary service is mutable. service instances for
 *       the same persistent data must determine whether or not another service
 *       is running (basically, can they obtain a lock to operate as the primary
 *       service). secondary service instances may provide redundent read-only
 *       operations from replicated media (or media on a distributed file
 *       system). the failover mechanisms are essentially the same for the data
 *       service and for the derived metadata service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServer extends AbstractServer {

    /**
     * Creates a new {@link DataServer}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public DataServer(String[] args) {

        super(args);
        
    }
    
//    public DataServer(String[] args, LifeCycle lifeCycle) {
//        
//        super( args, lifeCycle );
//        
//    }

    /**
     * Starts a new {@link DataServer}.  This can be done programmatically
     * by executing
     * <pre>
     *    new DataServer(args).run();
     * </pre>
     * within a {@link Thread}.
     * 
     * @param args
     *            The name of the {@link Configuration} file for the service.
     */
    public static void main(String[] args) {
        
        new DataServer(args).run();
        
    }
    
    protected Remote newService(Properties properties) {
        
        return new AdministrableDataService(this,properties);
        
    }

    /**
     * Extends the behavior to close and delete the journal in use by the data
     * service.
     */
    public void destroy() {

        DataService service = (DataService)impl;
        
        super.destroy();
        
        try {

            IJournal journal = service.journal;
            
            log.info("Closing and deleting: "+journal.getFile());
            
            journal.closeAndDelete();

            log.info("Journal deleted.");

        } catch (Throwable t) {

            log.warn("Could not delete journal: " + t, t);

        }

    }
    
    /**
     * Adds jini administration interfaces to the basic {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AdministrableDataService extends DataService implements
            RemoteAdministrable, RemoteDestroyAdmin {
        
        protected AbstractServer server;
        
        public AdministrableDataService(AbstractServer server,Properties properties) {
            
            super(properties);
            
            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

            log.info("");

            return server.proxy;
            
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

            log.info("");

            new Thread() {

                public void run() {

                    server.destroy();
                    
                    log.info("Service stopped.");

                }

            }.start();

        }

// /*
// * JoinAdmin
// */
//        
// public void addLookupAttributes(Entry[] arg0) throws RemoteException {
//            
// log.info("");
//            
//        }
//
//        public void addLookupGroups(String[] arg0) throws RemoteException {
//
//            log.info("");
//
//        }
//
//        public void addLookupLocators(LookupLocator[] arg0) throws RemoteException {
//
//            log.info("");
//            
//        }
//
//        public Entry[] getLookupAttributes() throws RemoteException {
//
//            log.info("");
//
//            return null;
//        }
//
//        public String[] getLookupGroups() throws RemoteException {
//         
//            log.info("");
//
//            return null;
//        }
//
//        public LookupLocator[] getLookupLocators() throws RemoteException {
//         
//            log.info("");
//
//            return null;
//        }
//
//        public void modifyLookupAttributes(Entry[] arg0, Entry[] arg1) throws RemoteException {
//         
//            log.info("");
//            
//        }
//
//        public void removeLookupGroups(String[] arg0) throws RemoteException {
//            log.info("");
//
//        }
//
//        public void removeLookupLocators(LookupLocator[] arg0) throws RemoteException {
//            log.info("");
//            
//        }
//
//        public void setLookupGroups(String[] arg0) throws RemoteException {
//            log.info("");
//            
//        }
//
//        public void setLookupLocators(LookupLocator[] arg0) throws RemoteException {
//            log.info("");
//            
//        }
        
    }

}
