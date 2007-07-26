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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.io.NameAndExtensionFilter;
import com.bigdata.scaleup.MetadataIndex;

/**
 * An implementation that uses an embedded database rather than a distributed
 * database. An embedded federation runs entirely in process, but uses the same
 * {@link DataService} and {@link MetadataService} implementations as a
 * distributed federation. Unlike a distributed federation, an embedded
 * federation starts and stops with the client. An embedded federation may be
 * used to assess or remove the overhead of network operations, to simplify
 * testing of client code, or to deploy a scale-up (vs scale-out) solution.
 * 
 * @todo define variants either statically (two classes) or dynamically (in
 *       code) that allow either a single data service (and no metadata service
 *       is required) or a metadata service and one or more data services.
 * 
 * @todo refactor the tests suites to run the embedded as well as the
 *       distributed versions.
 * 
 * @todo define EmbeddedBigdataClient, share code w/ {@link BigdataFederation},
 *       and refactor the RDF test suites.
 * 
 * @todo clean up the restart logic and validate with test suites. The service
 *       UUIDs need to be persistent and must define a consistent mapping from
 *       the service UUID to the data stored by that service. In JINI, that is
 *       handled by the service registrar. Perhaps the easiest way to finese
 *       this issue here is to save the data files in metadataService/UUID/*.jnl
 *       and dataService/UUID/*.jnl directories. The service UUIDs can then be
 *       assigned using a property or parameter when the service is first
 *       started.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmbeddedBigdataFederation implements IBigdataFederation {

    public static final Logger log = Logger.getLogger(EmbeddedBigdataFederation.class);

    /**
     * The client or <code>null</code> iff the client has disconnected from
     * the federation.
     */
    private EmbeddedBigdataClient client;
    
    /**
     * The #of data service instances.
     */
    final int ndataServices;
    
    /**
     * The directory in which the data files will reside.
     */
    final File dataDir;
    
    /**
     * The (in process) {@link MetadataService}.
     */
    private MetadataService metadataService;
    
    /**
     * The (in process) {@link DataService}s.
     */
    private DataService[] dataService;
    
    /**
     * Map providing lookup of the (in process) {@link DataService}s by service
     * UUID.
     */
    private Map<UUID,DataService> dataServiceByUUID = new HashMap<UUID,DataService>();

    public IBigdataClient getClient() {
        
        return client;
        
    }

    /**
     * The (in process) {@link MetadataService}.
     */
    public IMetadataService getMetadataService() {
        
        return metadataService;
        
    }

    /**
     * Return the (in process) data service given its service UUID.
     * 
     * @param serviceUUID
     * 
     * @return The {@link DataService} for that UUID or <code>null</code> if
     *         there is no data service instance with that service UUID.
     */
    public IDataService getDataService(UUID serviceUUID) {
        
        return dataServiceByUUID.get(serviceUUID);
        
    }
    
    /**
     * Options for the embedded (in process) federation. Service instances will
     * share the same configuration properties except for the name of the
     * backing store file.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options extends MetadataService.Options {

        /**
         * The name of the optional property whose value is the #of data
         * services that will be (re-)started. The data services will be named
         * with a base name suffix pattern "_#" where # is an integer in
         * [0:ndataServices).
         * 
         * @todo work through restart - are the #of data services read from the
         *       metadata service, the file system, or from the properties?
         */
        public static String NDATA_SERVICES = "ndataServices";

        /**
         * The default is two (2).
         */
        public static String DEFAULT_NDATA_SERVICES = "2";
        
        /**
         * <code>data.dir</code> - The property whose value is the name of the
         * directory in which the store files will be created (default is the
         * current working directory).
         */
        public static final String DATA_DIR = "data.dir";

        /**
         * The default is the current working directory.
         */
        public static final String DEFAULT_DATA_DIR = ".";
        
    }
    
    /**
     * @exception IllegalStateException
     *                if the client has disconnected from the federation.
     */
    private void assertOpen() {

        if (client == null) {

            throw new IllegalStateException();

        }

    }

    /**
     * Start or restart an embedded bigdata federation.
     * 
     * @param properties
     *            The configuration properties as defined by {@link Options}.
     */
    protected EmbeddedBigdataFederation(EmbeddedBigdataClient client, Properties properties) {
        
        if (client == null)
            throw new IllegalArgumentException();
        
        if (properties == null)
            throw new IllegalArgumentException();
        
        this.client = client;
        
        // The basename of the journal file for the metadata service.
        final String metadataBasename = "metadataService";

        // The basename of the journal file for the data service.
        final String dataBasename = "dataService";
        
        /*
         * The directory in which the data files will reside.
         */
        {

            String val = properties.getProperty(Options.DATA_DIR,
                    Options.DEFAULT_DATA_DIR);
            
            log.info(Options.DATA_DIR+"="+val);
            
            dataDir = new File(val);

            if (dataDir.exists() && !dataDir.isDirectory()) {

                throw new RuntimeException(
                        "Regular file exists with that name: "
                                + dataDir.getAbsolutePath());

            }

            if (!dataDir.exists()) {

                if (!dataDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + dataDir.getAbsolutePath());

                }

            }

        }

        /*
         * Look for pre-existing data files.
         */
        {
            
            /*
             * @todo if we are supporting overflow then there will be multiple
             * versions of the files per the master journal. In that case it is
             * better to place each (meta)data service into its own
             * subdirectory.
             */
            
            /*
             * Scan the data directory for metadata service data files.
             */
            
            File[] metadataFiles = new NameAndExtensionFilter(new File(dataDir,
                    metadataBasename).toString(), Options.JNL).getFiles();

            /*
             * Scan the data directory for data service data files.
             */
            
            File[] dataFiles = new NameAndExtensionFilter(new File(dataDir,
                    dataBasename).toString(), Options.JNL).getFiles();

            if(metadataFiles.length==0) {
                
                if(dataFiles.length != 0) {
                    
                    throw new RuntimeException("Data files found, but no metadata files.");
                    
                }

                /*
                 * First time startup.
                 */
                
                /*
                 * The #of data services (used iff this is a 1st time start).
                 */
                {

                    String val = properties.getProperty(Options.NDATA_SERVICES,
                            Options.DEFAULT_NDATA_SERVICES);

                    ndataServices = Integer.parseInt(val);

                    if (ndataServices <= 0) {

                        throw new IllegalArgumentException(Options.NDATA_SERVICES + "="
                                + val);

                    }
                
                }
                
                /*
                 * Start the metadata service.
                 */
                {

                    Properties p = new Properties(properties);
                    
                    // name of the metadata journal.
                    p.setProperty(Options.FILE, new File(dataDir,
                            metadataBasename + Options.JNL).toString());
                    
                    metadataService = new EmbeddedMetadataService(this, UUID
                            .randomUUID(), p);
                    
                }
                
                /*
                 * Start the data services.
                 */
                {

                    dataService = new DataService[ndataServices];

                    for (int i = 0; i < ndataServices; i++) {

                        Properties p = new Properties(properties);

                        // name of the data journal.
                        p.setProperty(Options.FILE, new File(dataDir,
                                dataBasename + "_" + i + Options.JNL)
                                .toString());

                        dataService[i] = new EmbeddedDataService(UUID
                                .randomUUID(), p);
                        
                        final UUID serviceUUID;
                        
                        try {
                            
                            serviceUUID = dataService[i].getServiceUUID();
                        
                        } catch(IOException ex) {
                            
                            /*
                             * Note: IOException is declared for RMI
                             * compatability, but the embedded federation does
                             * not use RMI.
                             */

                            throw new RuntimeException(ex);
                            
                        }
                        
                        dataServiceByUUID.put(serviceUUID, dataService[i]);

                    }
                    
                }
                
                
            } else {

                /*
                 * Re-start.
                 */
                
                ndataServices = dataFiles.length;

                // @todo support restart.
                throw new UnsupportedOperationException(
                        "Restart not supported yet");
                
            }
            
        }
        
    }
    
    /**
     * Note: This does not return an {@link IIndex} since the client does
     * not provide a transaction identifier when registering an index (
     * index registration is always unisolated).
     * 
     * @see #registerIndex(String, UUID)
     */
    public UUID registerIndex(String name) {

        assertOpen();

        return registerIndex(name, null);

    }

    /**
     * @todo only statically partitioned indices are supported at this time.
     */
    public MetadataIndex getMetadataIndex(String name) {

        assertOpen();

        // The name of the metadata index.
        final String metadataName = MetadataService.getMetadataName(name);

        // The metadata service.
        final MetadataService metadataService = (MetadataService)getMetadataService();

        /*
         * @todo this is the live metadata index -- it should be a read-only
         * view according to our method declaration. This must be resolved in
         * order to support dynamic partitioning.
         */
        return (MetadataIndex)metadataService.journal.getIndex(metadataName);
        
    }
    
    /**
     * Registers a scale-out index and assigns the initial index partition
     * to the specified data service.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @param dataServiceUUID
     *            The data service identifier (optional). When
     *            <code>null</code>, a data service will be selected
     *            automatically.
     * 
     * @return The UUID of the registered index.
     * 
     * @deprecated This method and its task on the metadataservice can be
     *             replaced by
     *             {@link #registerIndex(String, byte[][], UUID[])}
     */
    public UUID registerIndex(String name, UUID dataServiceUUID) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerManagedIndex(name,
                    dataServiceUUID);

            return indexUUID;

        } catch (Exception ex) {

            BigdataClient.log.error(ex);

            throw new RuntimeException(ex);

        }

    }

    public UUID registerIndex(String name, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerManagedIndex(name,
                    separatorKeys, dataServiceUUIDs);

            return indexUUID;

        } catch (Exception ex) {

            BigdataClient.log.error(ex);

            throw new RuntimeException(ex);

        }

    }

    /**
     * Drops the named scale-out index (synchronous).
     * 
     * FIXME implement. No new unisolated operation or transaction should be
     * allowed to read or write on the index. Once there are no more users
     * of the index, the index must be dropped from each data service,
     * including both the mutable B+Tree absorbing writes for the index and
     * any read-only index segments. The metadata index must be dropped on
     * the metadata service (and from the client's cache).
     * 
     * @todo A "safe" version of this operation would schedule the restart
     *       safe deletion of the mutable btrees, index segments and the
     *       metadata index so that the operation could be "discarded"
     *       before the data were actually destroyed (assuming an admin tool
     *       that would allow you to recover a dropped index before its
     *       component files were deleted).
     */
    public void dropIndex(String name) {

        assertOpen();

        throw new UnsupportedOperationException();

    }

    /**
     * @todo support isolated views, share cached data service information
     *       between isolated and unisolated views.
     */
    public IIndex getIndex(long tx, String name) {

        assertOpen();

        try {

            if (getMetadataService().getManagedIndexUUID(name) == null) {

                return null;

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        return new ClientIndexView(this, tx, name);

    }

    public void disconnect() {

        if (client == null)
            throw new IllegalStateException();
        
        for(int i=0; i<dataService.length; i++) {
            
            DataService ds = this.dataService[i];
            
            ds.shutdown();
            
        }

        metadataService.shutdown();

    }

    /**
     * A local (in process) metadata service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class EmbeddedMetadataService extends MetadataService {

        final private UUID serviceUUID;
        private int nextDataService;
        private EmbeddedBigdataFederation federation;
        
        public EmbeddedMetadataService(EmbeddedBigdataFederation federation,
                UUID serviceUUID, Properties properties) {
            
            super(properties);
        
            if (serviceUUID == null)
                throw new IllegalArgumentException();
            
            if (federation == null)
                throw new IllegalArgumentException();
            
            this.federation = federation;

            this.serviceUUID = serviceUUID;
            
        }

        public UUID getServiceUUID() throws IOException {

            return serviceUUID;
            
        }

        /**
         * @todo this is just an arbitrary instance and does not consider
         *       utilization.
         */
        public UUID getUnderUtilizedDataService() throws IOException {

            /*
             * Assigns the next data service using a round-robin approach.
             */

            int i = nextDataService;

            nextDataService = (nextDataService + 1) % federation.ndataServices;

            return federation.dataService[i].getServiceUUID();
            
        }

        public IDataService getDataServiceByUUID(UUID dataService) throws IOException {

            return federation.getDataService(dataService);
            
        }
        
    }
    
    /**
     * A local (in process) data service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class EmbeddedDataService extends DataService {
        
        final private UUID serviceUUID;
        
        public EmbeddedDataService(UUID serviceUUID, Properties properties) {
            
            super(properties);
            
            if (serviceUUID == null)
                throw new IllegalArgumentException();
            
            this.serviceUUID = serviceUUID;
            
        }

        public UUID getServiceUUID() throws IOException {

            return serviceUUID;
            
        }
        
    }
    
}
