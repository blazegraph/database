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
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.ReadOnlyMetadataIndexView;
import com.bigdata.service.EmbeddedClient.Options;

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
public class EmbeddedFederation extends AbstractFederation {

    public static final Logger log = Logger.getLogger(EmbeddedFederation.class);

    /**
     * The #of data service instances.
     */
    final int ndataServices;
    
    /**
     * True if the federation is not backed by disk.
     * @return
     */
    private final boolean isTransient;
    
    /**
     * The directory in which the data files will reside.
     */
    private final File dataDir;
    
    /**
     * The (in process) {@link TimestampService}.
     */
    private TimestampService timestampService;
    
    /**
     * The (in process) {@link LoadBalancerService}.
     */
    private LoadBalancerService loadBalancerService;
    
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

    /**
     * Return true if the federation is not backed by disk.
     */
    public boolean isTransient() {
    
        return isTransient;
        
    }
    
    public EmbeddedClient getClient() {
        
        return (EmbeddedClient) super.getClient();
        
    }

    /**
     * The (in process) {@link ITimestampService}.
     */
    public ITimestampService getTimestampService() {

        // Note: return null if service not available/discovered.
        
        return timestampService;
        
    }
    
    /**
     * The (in process) {@link LoadBalancerService}.
     */
    public ILoadBalancerService getLoadBalancerService() {

        // Note: return null if service not available/discovered.

        return loadBalancerService;
        
    }
    
    /**
     * The (in process) {@link MetadataService}.
     */
    public IMetadataService getMetadataService() {

        // Note: return null if service not available/discovered.

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

        // Note: return null if service not available/discovered.

        return dataServiceByUUID.get(serviceUUID);
        
    }
    
    /**
     * The #of configured data services in the embedded federation.
     */
    public int getDataServiceCount() {
        
        return ndataServices;
        
    }
    
    /**
     * There are {@link #getDataServiceCount()} data services defined in the
     * federation. This returns the data service with that index.
     * 
     * @param index
     *            The index.
     * 
     * @return The data service at that index.
     */
    public DataService getDataService(int index) {
        
        assertOpen();

        return dataService[index];
        
    }
    
    public UUID[] getDataServiceUUIDs(int maxCount) {

        assertOpen();

        if (maxCount < 0)
            throw new IllegalArgumentException();
        
        final int n = maxCount == 0 ? ndataServices : Math.min(maxCount,
                ndataServices);
        
        final UUID[] uuids = new UUID[ n ];
        
        for(int i=0; i<n; i++) {
            
            uuids[i] = getDataService( i ).getServiceUUID();
            
        }
        
        return uuids;
        
    }

    public IDataService getAnyDataService() {
        
        return getDataService(0);
        
    }

    /**
     * Start or restart an embedded bigdata federation.
     * 
     * @param client
     *            The client.
     */
    protected EmbeddedFederation(EmbeddedClient client) {
        
        super(client);
        
        final Properties properties = client.getProperties();
        
        // true iff the federation is diskless.
        isTransient = BufferMode.Transient.toString().equals(
                properties.getProperty(Options.BUFFER_MODE));
        
        log.info("federation is "+(isTransient?"not ":"")+"persistent");
        
        // true if temp files are being requested.
        final boolean createTempFile = Boolean.parseBoolean(properties
                .getProperty(Options.CREATE_TEMP_FILE,
                        ""+Options.DEFAULT_CREATE_TEMP_FILE));

        /*
         * Start the timestamp service.
         */
        timestampService = new EmbeddedTimestampService( UUID.randomUUID(), properties );
        
        /*
         * Start the load balancer.
         */
        loadBalancerService = new EmbeddedLoadBalancerService(UUID.randomUUID(),
                properties);

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
                                serviceUUID, p).start();
                        
                        nmetadataServices++;
                        
                        if (nmetadataServices > 1) {

                            throw new RuntimeException(
                                    "Not expecting more than one metadata service");
                            
                        }
                        
                    } else {
                        
                        /*
                         * data service.
                         */
                        
                        final DataService dataService = new EmbeddedDataServiceImpl(
                                serviceUUID, p).start();

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
        
        /*
         * Have the data services join the load balancer.
         */
        {

            final String hostname;
            try {
                hostname = Inet4Address.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e1) {
                // should never happen for the local host.
                throw new AssertionError();
            }
            
            for(IDataService ds : this.dataService ) {

                try {

                    loadBalancerService.join(ds.getServiceUUID(), hostname);
                    
                } catch (IOException e) {
                    
                    // Should never be thrown for an embedded service.
                    
                    log.warn(e.getMessage(),e);
                    
                }

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
            
            metadataService = new EmbeddedMetadataService(this, serviceUUID, p).start();
            
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

                dataService[i] = new EmbeddedDataServiceImpl(serviceUUID, p).start();

                dataServiceByUUID.put(serviceUUID, dataService[i]);

            }
            
        }
        
        return ndataServices;

    }
    
    /**
     * Concrete implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class EmbeddedDataServiceImpl extends AbstractEmbeddedDataService {

        /**
         * @param serviceUUID
         * @param properties
         */
        public EmbeddedDataServiceImpl(UUID serviceUUID, Properties properties) {
       
            super(serviceUUID, properties);
            
        }

        @Override
        public EmbeddedFederation getFederation() {

            return EmbeddedFederation.this;
            
        }
        
    }
    
    public IMetadataIndex getMetadataIndex(String name,long timestamp) {

        assertOpen();

        // The name of the metadata index.
        final String metadataName = MetadataService.getMetadataIndexName(name);

        // The metadata service.
        final MetadataService metadataService = (MetadataService) getMetadataService();
        
        /*
         * The sources for the view as of that timestamp.
         * 
         * Note: We force READ-COMMITTED semantics if the request was
         * UNISOLATED.
         */
        final AbstractBTree[] sources = metadataService
                .getResourceManager()
                .getIndexSources(
                        metadataName,
                        TimestampUtility.isUnisolated(timestamp) ? ITx.READ_COMMITTED
                                : timestamp);
        
        if (sources.length != 1) {
            
            throw new UnsupportedOperationException(
                    "Metadata index must not be a view: name=" + name
                            + ", #sources=" + sources);
            
        }

        // Read only view.
        return new ReadOnlyMetadataIndexView( sources[0] );

    }
    
    /**
     * Normal shutdown of the services in the federation.
     */
    synchronized public void shutdown() {
        
        log.info("begin");
        
        super.shutdown();
        
        for(int i=0; i<dataService.length; i++) {
            
            DataService ds = this.dataService[i];
            
            ds.shutdownNow();
            
        }

        metadataService.shutdownNow();
        
        if (loadBalancerService != null) {

            loadBalancerService.shutdown();

            loadBalancerService = null;
            
        }
        
        if (timestampService != null) {

            timestampService.shutdown();

            timestampService = null;

        }
        
        log.info("done");

    }
    
    /**
     * Immediate shutdown of the services in the embedded federation.
     */
    synchronized public void shutdownNow() {
        
        log.info("begin");

        super.shutdownNow();
        
        for(int i=0; i<dataService.length; i++) {
            
            DataService ds = this.dataService[i];
            
            ds.shutdownNow();
            
        }

        metadataService.shutdownNow();

        if (loadBalancerService != null) {

            loadBalancerService.shutdownNow();

            loadBalancerService = null;
            
        }
        
        if (timestampService != null) {

            timestampService.shutdownNow();

            timestampService = null;

        }
        
        log.info("done");
        
    }
    
    public void destroy() {

        log.info("");

        for (int i=0; i<dataService.length; i++) {

            IDataService ds = dataService[i];
            
            try {
                
                ds.destroy();
            
            } catch (IOException e) {
             
                log.error("Could not destroy dataService", e );
                
            }
            
            dataService[i] = null;

        }

        {

            try {

                metadataService.destroy();

            } catch (IOException e) {

                log.error("Could not destroy dataService", e);

            }

            metadataService = null;
            
        }

        loadBalancerService.shutdownNow();
        
        loadBalancerService = null;
        
        timestampService.shutdownNow();
        
        timestampService = null;
        
    }
    
    /**
     * Return <code>true</code>.
     */
    public boolean isScaleOut() {
        
        return true;
        
    }

    /**
     * Return <code>false</code>.
     */
    public boolean isDistributed() {
        
        return false;
        
    }
    
    public boolean isStable() {

        return !isTransient;

    }
    
    /**
     * @todo this scans the {@link DataService}s and reports the most recent
     *       value. The data service initialization should be changed so that
     *       the embedded data services are using a shared
     *       {@link AbstractLocalTransactionManager} and that class should note
     *       the most recent commit time (it will have to query the data
     *       services during start, much like we are doing here). This approach
     *       generalizes towards the distributed systems approach.
     */
    public long lastCommitTime() {

        assertOpen();
        
        long maxValue = 0;
        
        for(int i=0; i<dataService.length; i++) {

            final long commitTime = dataService[i].getResourceManager()
                    .getLiveJournal().getRootBlockView().getLastCommitTime();
            
            if(commitTime>maxValue) {
                
                maxValue = commitTime;
                
            }
            
        }
        
        return maxValue;
        
    }

}
