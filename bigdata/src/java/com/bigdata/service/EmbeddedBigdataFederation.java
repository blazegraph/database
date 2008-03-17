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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.ReadOnlyMetadataIndexView;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.util.InnerCause;

/**
 * An implementation that uses an embedded database rather than a distributed
 * database. An embedded federation runs entirely in process, but uses the same
 * {@link DataService} and {@link MetadataService} implementations as a
 * distributed federation. Unlike a distributed federation, an embedded
 * federation starts and stops with the client. An embedded federation may be
 * used to assess or remove the overhead of network operations, to simplify
 * testing of client code, or to deploy a scale-up (vs scale-out) solution.
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
    private final File dataDir;
    
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
     * There are {@link #ndataServices} data services defined in the federation.
     * This returns the data service with that index.
     * 
     * @param index
     *            The index.
     *            
     * @return The data service at that index.
     */
    public DataService getDataService(int index) {
        
        return dataService[index];
        
    }
    
    /**
     * Options for the embedded (in process) federation. Service instances will
     * share the same configuration properties except for the name of the
     * backing store file.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends MetadataService.Options {

        /**
         * The name of the optional property whose value is the #of data
         * services that will be (re-)started.
         */
        public static String NDATA_SERVICES = "ndataServices";

        /**
         * The default is two (2).
         */
        public static String DEFAULT_NDATA_SERVICES = "2";
        
        /**
         * <code>data.dir</code> - The name of the required property whose
         * value is the name of the directory under which each metadata and data
         * service will store their state (their journals and index segments).
         * <p>
         * Note: A set of subdirectories will be created under the specified
         * data directory. Those subdirectories will be named by the UUID of the
         * corresponding metadata or data service. In addition, the subdirectory
         * corresponding to the metadata service will have a file named ".mds"
         * to differentiate it from the data service directories. The presence
         * of that ".mds" file is used to re-start the service as a metadata
         * service rather than a data service.
         * <p>
         * Since a set of subdirectories will be created for the embedded
         * federation, it is important to give each embedded federation its own
         * data directory. Otherwise a new federation starting up with another
         * federation's data directory will attempt to re-start that federation.
         */
        public static final String DATA_DIR = "data.dir";

//        /**
//         * The default is the current working directory.
//         */
//        public static final String DEFAULT_DATA_DIR = ".";
        
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

        // isolate caller from any changes we may make.
        properties = new Properties(properties);
        
        // true iff the federation is diskless.
        final boolean isTransient = BufferMode.Transient.toString().equals(
                properties.getProperty(Options.BUFFER_MODE));
        
        log.info("federation is "+(isTransient?"not ":"")+"persistent");
        
        // true if temp files are being requested.
        final boolean createTempFile = Boolean.parseBoolean(properties
                .getProperty(Options.CREATE_TEMP_FILE,
                        ""+Options.DEFAULT_CREATE_TEMP_FILE));

        /*
         * The directory in which the data files will reside.
         */
        if (isTransient) {

            dataDir = null;
            
            /*
             * Always do first time startup since there is no persistent state.
             */
            ndataServices = createFederation(properties, isTransient);

        } else {

            if (createTempFile) {

                // files will be created in a temporary directory.
                File tmpDir = new File(properties.getProperty(Options.TMP_DIR, System
                        .getProperty("java.io.tmpdir")));
                
                try {

                    // create temp file.
                    dataDir = File.createTempFile("bigdata", ".fed", tmpDir);
                    
                    // delete temp file.
                    dataDir.delete();
                    
                    // re-create as directory.
                    dataDir.mkdir();
                    
                } catch (IOException e) {

                    throw new RuntimeException(e);
                    
                }

                // unset this property so that it does not propagate to the journal ctor.
                properties.setProperty(Options.CREATE_TEMP_FILE, "false");
                
            } else {

//                dataDir = new File(properties.getProperty(Options.DATA_DIR,
//                        Options.DEFAULT_DATA_DIR));
                
                String val = properties.getProperty(Options.DATA_DIR);
                
                if (val == null) {

                    throw new RuntimeException("Required property: "
                            + Options.DATA_DIR);
                    
                }
                
                dataDir = new File(val);
                
            }

            log.info(Options.DATA_DIR + "=" + dataDir);

            if (!dataDir.exists()) {

                if (!dataDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + dataDir.getAbsolutePath());

                }

            }

            if (!dataDir.isDirectory()) {

                throw new RuntimeException(
                        "Regular file exists with that name: "
                                + dataDir.getAbsolutePath());

            }

            /*
             * Persistent (re-)start.
             * 
             * Look for pre-existing (meta)data services. Each service stores
             * its state in a subdirectory named by the serviceUUID. We
             * recognize these directories by the ability to parse the directory
             * name as a UUID (the embedded services do not store a service.id
             * file - that is just the jini services). In addition a ".mds" file
             * is created in the service directory that corresponds to the
             * metadata service.
             */

            final File[] serviceDirs = dataDir.listFiles(new FileFilter() {

                public boolean accept(File pathname) {
                    
                    if(!pathname.isDirectory()) {
                        
                        log.info("Ignoring normal file: "+pathname);
                        
                        return false;
                        
                    }
                    
                    final String name = pathname.getName();
                    
                    try {
                        
                        UUID.fromString(name);

                        log.info("Found service directory: "+pathname);
                        
                        return true;
                        
                    } catch(IllegalArgumentException ex) {
                        
                        log.info("Ignoring directory: "+pathname);
                        
                        return false;
                        
                    }
                    
                }
                
            });
            
            if (serviceDirs.length == 0) {
                
                /*
                 * First time startup.
                 */
                ndataServices = createFederation(properties,isTransient);

            } else {
                
                /*
                 * Reload services from disk.
                 */

                // expected #of data services.
                dataService = new DataService[serviceDirs.length - 1];

                int ndataServices = 0;
                int nmetadataServices = 0;
                for(File serviceDir : serviceDirs ) {
                    
                    final UUID serviceUUID = UUID.fromString(serviceDir.getName());

                    final Properties p = new Properties(properties);

                    /*
                     * Note: Use DATA_DIR if the metadata service is using a
                     * ResourceManager and FILE if it is using a simple Journal.
                     */
                    p.setProperty(Options.DATA_DIR, serviceDir.toString());
//                    p.setProperty(Options.FILE, new File(serviceDir,"journal"+Options.JNL).toString());

                    if(new File(serviceDir,".mds").exists()) {
                        
                        /*
                         * metadata service.
                         */
                        metadataService = new EmbeddedMetadataService(this,
                                serviceUUID, p);
                        
                        nmetadataServices++;
                        
                        if (nmetadataServices > 1) {

                            throw new RuntimeException(
                                    "Not expecting more than one metadata service");
                            
                        }
                        
                    } else {
                        
                        /*
                         * data service.
                         */
                        
                        final DataService dataService = new EmbeddedMetadataService(
                                this, serviceUUID, p);

                        if (ndataServices == this.dataService.length) {

                            throw new RuntimeException(
                                    "Too many data services?");
                            
                        }

                        this.dataService[ndataServices++] = dataService;
                     
                        dataServiceByUUID.put(serviceUUID, dataService);
                        
                    }
                    
                }

                assert ndataServices == this.dataService.length;
                
                this.ndataServices = ndataServices;
                
            }
            
        }

    }

    /**
     * Create a new federation.
     * 
     * @param properties
     * 
     * @return The #of created data services.
     * 
     * FIXME The embedded federation setup is not correct when transient buffers
     * are requested. We wind up creating the resource manager files in the
     * current working directory when we really want the resource manager to
     * startup with transient journals (and disallow overflow since you can not
     * re-open a store or even write an index segement).
     */
    private int createFederation(Properties properties, boolean isTransient) {
        
        final int ndataServices;
        
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

            final Properties p = new Properties(properties);
            
            final UUID serviceUUID = UUID.randomUUID();

            if (!isTransient) {

                final File serviceDir = new File(dataDir, serviceUUID
                        .toString());

                serviceDir.mkdirs();

                /*
                 * Create ".mds" file to mark this as the metadata service
                 * directory.
                 */
                try {

                    new RandomAccessFile(new File(serviceDir, ".mds"), "rw")
                            .close();

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

                /*
                 * Note: Use DATA_DIR if the metadata service is using a
                 * ResourceManager and FILE if it is using a simple Journal.
                 */
                p.setProperty(Options.DATA_DIR, serviceDir.toString());
//                p.setProperty(Options.FILE, new File(serviceDir,"journal"+Options.JNL).toString());

            }
            
            metadataService = new EmbeddedMetadataService(this, serviceUUID, p);
            
        }
        
        /*
         * Start the data services.
         */
        {

            dataService = new DataService[ndataServices];

            for (int i = 0; i < ndataServices; i++) {

                final Properties p = new Properties(properties);

                final UUID serviceUUID = UUID.randomUUID();

                if (!isTransient) {

                    final File serviceDir = new File(dataDir, serviceUUID
                            .toString());

                    serviceDir.mkdirs();

                    p.setProperty(Options.DATA_DIR, serviceDir.toString());
                    
                }

                dataService[i] = new EmbeddedDataService(serviceUUID, p) {
                  
                    public IMetadataService getMetadataService() {
                        
                        return metadataService;
                        
                    }
                    
                };

                dataServiceByUUID.put(serviceUUID, dataService[i]);

            }
            
        }
        
        return ndataServices;

    }
    
    public IMetadataIndex getMetadataIndex(String name,long timestamp) {

        assertOpen();

        // The name of the metadata index.
        final String metadataName = MetadataService.getMetadataIndexName(name);

        // The metadata service.
        final MetadataService metadataService = (MetadataService) getMetadataService();
        
        // The sources for the view as of that timestamp.
        final AbstractBTree[] sources = metadataService.getResourceManager()
                .getIndexSources(metadataName, timestamp);
        
        if (sources.length != 1) {
            
            throw new UnsupportedOperationException(
                    "Metadata index must not be a view: name=" + name
                            + ", #sources=" + sources);
            
        }

        // Read only view.
        return new ReadOnlyMetadataIndexView( sources[0] );

    }
    
    public UUID registerIndex(IndexMetadata metadata) {

        assertOpen();

        return registerIndex(metadata, null);

    }

    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {

        assertOpen();

        if (dataServiceUUID == null) {
            
            try {
            
                dataServiceUUID = getMetadataService()
                        .getUnderUtilizedDataService();

            } catch (Exception ex) {

                log.error(ex);

                throw new RuntimeException(ex);

            }

        }

        return registerIndex(//
                metadata, //
                new byte[][] { new byte[] {} },//
                new UUID[] { dataServiceUUID } //
            );
        
    }

    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerScaleOutIndex(
                    metadata, separatorKeys, dataServiceUUIDs);

            return indexUUID;

        } catch (Exception ex) {

            log.error(ex);

            throw new RuntimeException(ex);

        }

    }

    public void dropIndex(String name) {

        assertOpen();

        try {
            
            getMetadataService().dropScaleOutIndex(name);
            
        } catch (Exception e) {

            throw new RuntimeException( e );
            
        }

    }

    public IIndex getIndex(String name,long timestamp) {

        assertOpen();

        final MetadataIndexMetadata mdmd;
        try {

            mdmd = (MetadataIndexMetadata) getMetadataService()
                    .getIndexMetadata(
                            MetadataService.getMetadataIndexName(name),
                            timestamp);
            
            assert mdmd != null;
            
        } catch (Exception ex) {

            if(InnerCause.isInnerCause(ex, NoSuchIndexException.class)) {
                
                return null;
                
            }
            
            throw new RuntimeException(ex);

        }

        return new ClientIndexView(this, name, timestamp, mdmd);

    }

    /**
     * Note: the {@link EmbeddedBigdataFederation} is {@link #shutdown()} when
     * the client {@link #disconnect()}s.
     */
    public void disconnect() {

        if (client == null)
            throw new IllegalStateException();
        
        shutdown();
        
    }

    /**
     * Normal shutdown of the services in the federation.
     */
    public void shutdown() {
        
        log.info("begin");
        
        assertOpen();
        
        client = null;
        
        for(int i=0; i<dataService.length; i++) {
            
            DataService ds = this.dataService[i];
            
            ds.shutdown();
            
        }

        metadataService.shutdown();
        
        log.info("done");

    }
    
    /**
     * Immediate shutdown of the services in the embedded federation.
     */
    public void shutdownNow() {
        
        log.info("begin");

        assertOpen();
        
        client = null;
        
        for(int i=0; i<dataService.length; i++) {
            
            DataService ds = this.dataService[i];
            
            ds.shutdownNow();
            
        }

        metadataService.shutdownNow();

        log.info("done");
        
    }
    
    /**
     * A local (in process) metadata service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class EmbeddedMetadataService extends MetadataService {

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

        public UUID[] getUnderUtilizedDataServices(int limit, UUID exclude) throws IOException {

            if(limit==0) {
                
                limit = federation.ndataServices;
                
            } else {
            
                limit = Math.min(limit, federation.ndataServices);
                
            }
            
            final List<UUID> a = new ArrayList<UUID>(limit);
            
            for(int i=0; i<federation.ndataServices; i++) {
                
                int index = _index.incrementAndGet();
                
                UUID uuid = federation.dataService[index % federation.ndataServices].getServiceUUID();
                
                if(exclude != null && exclude.equals(uuid)) {
                    
                    continue;
                    
                }
                
                a.add(uuid);
                
            }
            
            return a.toArray(new UUID[a.size()]);
            
        }
        
        /**
         * Used to make {@link #getUnderUtilizedDataServices(int, UUID)} a
         * thread-safe round-robin policy.
         */
        private AtomicInteger _index = new AtomicInteger(0);

        public IDataService getDataService(UUID dataService) throws IOException {

            return federation.getDataService(dataService);
            
        }

        @Override
        public IMetadataService getMetadataService() {

            return this;
            
        }
        
    }
    
}
