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
 * Created on Mar 24, 2007
 */

package com.bigdata.service;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Properties;

import net.jini.core.lookup.ServiceMatches;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;

import com.bigdata.journal.IJournal;
import com.sun.jini.start.LifeCycle;

/**
 * A metadata server.
 * <p>
 * The metadata server is used to manage the life cycles of scale-out indices
 * and exposes proxies for read and write operations on indices to clients.
 * Clients use index proxies, which automatically direct reads and writes to the
 * {@link IDataService} on which specific index partitions are located.
 * <p>
 * On startup, the metadata service discovers active data services configured in
 * the same group. While running, it tracks when data services start and stop so
 * that it can (re-)allocate index partitions as necessary.
 * <p>
 * The metadata server uses a write through pipeline to replicate its data onto
 * registered secondary metadata servers. If the metadata server fails, clients
 * will automatically fail over to a secondary metadata server. Only the primary
 * metadata server actively tracks the state of data services since secondaries
 * are updated via the write through pipeline to ensure consistency. Secondary
 * metadata servers will notice if the primary dies and elect a new master.
 * 
 * @todo note that the service update registration is _persistent_ (assuming
 *       that the service registrar is persistent I suppose) so that will add a
 *       wrinkle to how a bigdata instance must be configured.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MetadataServer extends AbstractServer {

    /**
     * @param args
     */
    public MetadataServer(String[] args) {
       
        super(args);
        
    }

    /**
     * @param args
     * @param lifeCycle
     */
    public MetadataServer(String[] args, LifeCycle lifeCycle) {
        
        super(args, lifeCycle);
        
        
        
    }

    protected Remote newService(Properties properties) {

        return new AdministrableMetadataService(this,properties);
        
    }
    
    /**
     * Return an {@link IMetadataService}.
     * 
     * @param registrar
     *            A service registrar to query.
     *            
     * @return An {@link IMetadataService} if one was found using that
     *         registrar.
     */
    public IMetadataService getMetadataService(ServiceRegistrar registrar) {
        
        Class[] classes = new Class[] {IMetadataService.class};
        
        ServiceTemplate template = new ServiceTemplate(null, classes, null);
        
        IMetadataService proxy = null;
        
        try {
        
            proxy = (IMetadataService) registrar.lookup(template);
            
        } catch(java.rmi.RemoteException e) {

            log.warn(e);
            
        }

        return proxy;

    }

    /**
     * Return the data service(s) matched on this registrar.
     * 
     * @param registrar
     * 
     * @return The data service or <code>null</code> if none was matched.
     * 
     * @todo we need to describe the services to be discovered by their primary
     *       interface and only search within a designated group that
     *       corresponds to the bigdata federation of interest - that group is
     *       part of the client configuration.
     * 
     * @todo how do we ensure that we have seen all data services? If we query
     *       each registrar as it is discovered and then register for updates
     *       there are two ways in which we could miss some instances: (1) new
     *       data services register between the query and the registration for
     *       updates; and (2) the query will not return _ALL_ data services
     *       registered, but only as match as the match limit.
     */
    public ServiceMatches getDataServices(ServiceRegistrar registrar) {
        
        Class[] classes = new Class[] {IDataService.class};
        
        ServiceTemplate template = new ServiceTemplate(null, classes, null);
        
        try {
        
            return registrar.lookup(template,0);
            
        } catch(java.rmi.RemoteException e) {

            log.warn(e);

            return null;
            
        }

    }
    
    /**
     * Extends the behavior to close and delete the journal in use by the
     * metadata service.
     */
    public void destroy() {

        MetadataService service = (MetadataService)impl;
        
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
     * Adds jini administration interfaces to the basic {@link MetadataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class AdministrableMetadataService extends MetadataService
            implements Remote, RemoteAdministrable, RemoteDestroyAdmin {
        
        protected AbstractServer server;
        
        /**
         * @param properties
         */
        public AdministrableMetadataService(AbstractServer server, Properties properties) {

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
        
    }

}
