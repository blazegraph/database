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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ResourceLockService;
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
public class EmbeddedFederation<T> extends AbstractScaleOutFederation<T> {

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
     * The directory in which the data files will reside. Each directory
     * is named for the service {@link UUID} - restart depends on this.
     */
    private final File dataDir;
    
    /**
     * The (in process) {@link AbstractTransactionService}.
     */
    private AbstractTransactionService abstractTransactionService;
    
    /** The (in process) {@link IResourceLockService} */
    private ResourceLockService resourceLockManager;
    
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
    
    public EmbeddedClient<T> getClient() {
        
        return (EmbeddedClient<T>) super.getClient();
        
    }

    /**
     * The (in process) {@link ITransactionService}.
     */
    final public ITransactionService getTransactionService() {

        // Note: return null if service not available/discovered.
        
        return abstractTransactionService;
        
    }
    
    /**
     * The (in process) {@link IResourceLockService}.
     */
    final public IResourceLockService getResourceLockService() {
        
        return resourceLockManager;
        
    }
    
    /**
     * The (in process) {@link LoadBalancerService}.
     */
    final public ILoadBalancerService getLoadBalancerService() {

        // Note: return null if service not available/discovered.

        return loadBalancerService;
        
    }
    
    /**
     * The (in process) {@link MetadataService}.
     */
    final public IMetadataService getMetadataService() {

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
    final public IDataService getDataService(UUID serviceUUID) {

        // Note: return null if service not available/discovered.

        return dataServiceByUUID.get(serviceUUID);
        
    }
    
    /**
     * The #of configured data services in the embedded federation.
     */
    final public int getDataServiceCount() {
        
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
    final public DataService getDataService(final int index) {
        
        assertOpen();

        return dataService[index];
        
    }
    
    final public UUID[] getDataServiceUUIDs(final int maxCount) {

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

    final public IDataService getAnyDataService() {
        
        return getDataService(0);
        
    }

//    /**
//     * There are no preconditions for a service start.
//     */
//    @Override
//    protected boolean awaitPreconditions(long timeout, TimeUnit unit)
//            throws InterruptedException {
//
//        return true;
//
//    }
    
    /**
     * Start or restart an embedded bigdata federation.
     * 
     * @param client
     *            The client.
     */
    protected EmbeddedFederation(final EmbeddedClient<T> client) {
        
        super(client);
        
        final Properties properties = client.getProperties();
        
        // true iff the federation is diskless.
        isTransient = BufferMode.Transient.toString().equals(
                properties.getProperty(Options.BUFFER_MODE));
        
        if (log.isInfoEnabled())
            log.info("federation is "+(isTransient?"not ":"")+"persistent");
        
        // true if temp files are being requested.
        final boolean createTempFile = Boolean.parseBoolean(properties
                .getProperty(Options.CREATE_TEMP_FILE,
                        ""+Options.DEFAULT_CREATE_TEMP_FILE));

        /*
         * The directory in which the data files will reside.
         */
        if (isTransient) {

            // No data directory.
            dataDir = null;

        } else {

            if (createTempFile) {

                // files will be created in a temporary directory.
                final File tmpDir = new File(properties.getProperty(
                        Options.TMP_DIR, System.getProperty("java.io.tmpdir")));
                
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

            if (log.isInfoEnabled())
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
            
        }
        
        /*
         * Start the transaction service.
         */
        {

            final Properties p = new Properties(properties);
            
            if (isTransient) {

                // disable snapshots
                p.setProperty(
                                EmbeddedTransactionServiceImpl.Options.SHAPSHOT_INTERVAL,
                                "0");
                
            } else {
                
                // specify the data directory for the txService.
                p.setProperty(EmbeddedTransactionServiceImpl.Options.DATA_DIR,
                        new File(dataDir, "txService").toString());
                
            }
            
            abstractTransactionService = new EmbeddedTransactionServiceImpl(
                    UUID.randomUUID(), p).start();
            
        }

        /*
         * Start the lock manager.
         */
        resourceLockManager = new ResourceLockService();
        
        /*
         * Start the load balancer.
         */
        loadBalancerService = new EmbeddedLoadBalancerServiceImpl(UUID.randomUUID(),
                properties).start();

        /*
         * The directory in which the data files will reside.
         */
        if (isTransient) {

            /*
             * Always do first time startup since there is no persistent state.
             */
            ndataServices = createFederation(properties, isTransient);

        } else {

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
                        
                        if(log.isInfoEnabled())
                            log.info("Ignoring normal file: "+pathname);
                        
                        return false;
                        
                    }
                    
                    final String name = pathname.getName();
                    
                    try {
                        
                        UUID.fromString(name);

                        if (log.isInfoEnabled())
                            log.info("Found service directory: " + pathname);

                        return true;

                    } catch (IllegalArgumentException ex) {

                        if (log.isInfoEnabled())
                            log.info("Ignoring directory: " + pathname);

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
                    p.setProperty(MetadataService.Options.DATA_DIR, serviceDir.toString());
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

        {
        
            final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

            /*
             * Have the data services join the load balancer.
             */
            for (IDataService ds : this.dataService) {

                try {

                    loadBalancerService.join(ds.getServiceUUID(), ds
                            .getServiceIface(), hostname);

                } catch (IOException e) {

                    // Should never be thrown for an embedded service.

                    log.warn(e.getMessage(), e);

                }

            }

            /*
             * Other service joins.
             */

            loadBalancerService.join(abstractTransactionService.getServiceUUID(),
                    abstractTransactionService.getServiceIface(), hostname);

            loadBalancerService.join(loadBalancerService.getServiceUUID(),
                    loadBalancerService.getServiceIface(), hostname);

            loadBalancerService.join(metadataService.getServiceUUID(),
                    metadataService.getServiceIface(), hostname);

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

                final File serviceDir = new File(dataDir, serviceUUID.toString());

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

                p.setProperty(MetadataService.Options.DATA_DIR, serviceDir.toString());

            }
            
            metadataService = new EmbeddedMetadataService(this, serviceUUID, p)
                    .start();
            
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

                    p.setProperty(DataService.Options.DATA_DIR, serviceDir
                            .toString());

                }

                dataService[i] = new EmbeddedDataServiceImpl(serviceUUID, p)
                        .start();

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
    
    protected class EmbeddedLoadBalancerServiceImpl extends AbstractEmbeddedLoadBalancerService {
        
        /**
         * @param serviceUUID
         * @param properties
         */
        public EmbeddedLoadBalancerServiceImpl(UUID serviceUUID, Properties properties) {
       
            super(serviceUUID, properties);
            
        }

        @Override
        public EmbeddedFederation getFederation() {

            return EmbeddedFederation.this;

        }

    }
    
    protected class EmbeddedTransactionServiceImpl extends AbstractEmbeddedTransactionService {

        /**
         * @param serviceUUID
         * @param properties
         */
        public EmbeddedTransactionServiceImpl(UUID serviceUUID, Properties properties) {
           
            super(serviceUUID, properties);
            
        }

        @Override
        public EmbeddedFederation getFederation() {

            return EmbeddedFederation.this;

        }
        
//        @Override
//        protected void setReleaseTime(final long releaseTime) {
//            
//            for (DataService ds : dataService) {
//
//                ds.setReleaseTime(releaseTime);
//                
//            }
//            
//        }

    }
    
    /**
     * Normal shutdown of the services in the federation.
     */
    synchronized public void shutdown() {
        
        if (log.isInfoEnabled())
            log.info("begin");

        super.shutdown();
        
        if (abstractTransactionService != null) {

            abstractTransactionService.shutdown();

        }

        for (int i = 0; i < dataService.length; i++) {

            DataService ds = this.dataService[i];

            ds.shutdownNow();
            
        }

        metadataService.shutdownNow();
        
        if (loadBalancerService != null) {

            loadBalancerService.shutdown();

            loadBalancerService = null;
            
        }
        
        // Note: don't clear ref until all down since nextTimestamp() still active.
        abstractTransactionService = null;

        if (log.isInfoEnabled())
            log.info("done");

    }

    /**
     * Immediate shutdown of the services in the embedded federation.
     */
    synchronized public void shutdownNow() {

        if (log.isInfoEnabled())
            log.info("begin");

        super.shutdownNow();

        if (abstractTransactionService != null) {

            abstractTransactionService.shutdownNow();

        }

        for (int i = 0; i < dataService.length; i++) {

            DataService ds = this.dataService[i];
            
            ds.shutdownNow();
            
        }

        metadataService.shutdownNow();

        if (loadBalancerService != null) {

            loadBalancerService.shutdownNow();

            loadBalancerService = null;
            
        }
        
        // Note: don't clear ref until all down since nextTimestamp() still active.
        abstractTransactionService = null;

        if (log.isInfoEnabled())
            log.info("done");

    }

    public void destroy() {

        if (log.isInfoEnabled())
            log.info("");

        abstractTransactionService.shutdownNow();
        
        for (int i = 0; i < dataService.length; i++) {

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

        // Note: don't clear ref until all down since nextTimestamp() still active.
        abstractTransactionService = null;
        
    }
    
    /**
     * Return <code>false</code>.
     */
    final public boolean isDistributed() {
        
        return false;
        
    }
    
    final public boolean isStable() {

        return !isTransient;

    }
    
    /**
     * This scans the {@link DataService}s and reports the most recent value.
     */
    public long getLastCommitTime() {

        assertOpen();
        
        long maxValue = 0;

        // check each of the data services.
        for(int i=0; i<dataService.length; i++) {

            final long commitTime = dataService[i].getResourceManager()
                    .getLiveJournal().getRootBlockView().getLastCommitTime();

            if (commitTime > maxValue) {

                maxValue = commitTime;
                
            }
            
        }
        
        // and also check the metadata service
        {

            final long commitTime = metadataService.getResourceManager()
                    .getLiveJournal().getRootBlockView().getLastCommitTime();

            if (commitTime > maxValue) {

                maxValue = commitTime;

            }

        }
        
        return maxValue;
        
    }

    public IDataService getDataServiceByName(final String name) {

        for (IDataService ds : dataService) {

            final String serviceName;
            try {

                serviceName = ds.getServiceName();
                
            } catch (IOException e) {
                
                // note: will not be thrown (local service, no RMI).
                throw new RuntimeException(e);
                
            }
            
            if (name.equals(serviceName)) {

                return ds;

            }

        }
        
        // no match.
        return null;
        
    }

}
