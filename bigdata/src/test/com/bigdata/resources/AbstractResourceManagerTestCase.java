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
 * Created on Feb 22, 2008
 */

package com.bigdata.resources;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ValidationError;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.IBlock;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Base class for {@link ResourceManager} test suites that can use normal
 * startup and shutdown.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractResourceManagerTestCase extends
        AbstractResourceManagerBootstrapTestCase {

    /**
     * 
     */
    public AbstractResourceManagerTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractResourceManagerTestCase(String arg0) {
        super(arg0);
    }

    /**
     * Forces the use of persistent journals so that we can do overflow
     * operations and the like.
     */
    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        // Note: test requires data on disk.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());
        
        // Disable index copy - overflow will always cause an index segment build.
        properties.setProperty(Options.COPY_INDEX_THRESHOLD,"0");
        
        return properties;
        
    }
    
    protected IMetadataService metadataService;
    protected ResourceManager resourceManager;
    protected ConcurrencyManager concurrencyManager;
    protected AbstractLocalTransactionManager localTransactionManager;
    protected static final MillisecondTimestampFactory timestampFactory = new MillisecondTimestampFactory();

    /**
     * Setup test fixtures.
     */
    public void setUp() throws Exception {
        
        super.setUp();

        metadataService = new MockMetadataService();
//        {
//        
//            /*
//             * Note: The metadata service uses a ResourceManager internally.
//             * Therefore we setup a distinct DATA_DIR to avoid conflicts with
//             * the ResourceManager instance whose behavior we are trying to
//             * test.
//             */
//            Properties properties = new Properties(getProperties());
//
//            properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//            
//            metadataService = new MetadataService(properties) {
//
//                private final UUID uuid = UUID.randomUUID();
//
//                @Override
//                protected IMetadataService getMetadataService() {
//                    return this;
//                }
//
//                @Override
//                public UUID getServiceUUID() throws IOException {
//                    return uuid;
//                }
//
//                public UUID getUnderUtilizedDataService() throws IOException {
//                    throw new UnsupportedOperationException();
//                }
//
//                public IDataService getDataServiceByUUID(UUID serviceUUID)
//                        throws IOException {
//                    throw new UnsupportedOperationException();
//                }
//
//            };
//
//        }

        final Properties properties = getProperties();

        resourceManager = new ResourceManager(properties) {

            final private UUID dataServiceUUID = UUID.randomUUID();
            
            public ILoadBalancerService getLoadBalancerService() {

                throw new UnsupportedOperationException();
                
            }
            
            public IMetadataService getMetadataService() {

                return metadataService;

            }

            public IDataService getDataService(UUID dataService) {

                throw new UnsupportedOperationException();

            }

            public UUID getDataServiceUUID() {
                
                return dataServiceUUID;
                
            }

            /** Note: no failover services. */
            public UUID[] getDataServiceUUIDs() {
                
                return new UUID[] { dataServiceUUID };
                
            }

        };

        localTransactionManager = new AbstractLocalTransactionManager(
                resourceManager) {

            public long nextTimestamp() {

                return timestampFactory.nextMillis();
                
            }
            
        };
        
        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

        localTransactionManager.setConcurrencyManager(concurrencyManager);

        resourceManager.setConcurrencyManager(concurrencyManager);
        
        resourceManager.start();
        
    }

    public void tearDown() throws Exception {

        shutdownNow();
        
        if (resourceManager != null)
            resourceManager.deleteResources();

    }

    /**
     * Polite shutdown does not accept new requests and will shutdown once the
     * existing requests have been processed.
     * <p>
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    public void shutdown() {

        concurrencyManager.shutdown();

        localTransactionManager.shutdown();

        resourceManager.shutdown();

//        metadataService.shutdown();
        
    }

    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon as
     * possible.
     * <p>
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    public void shutdownNow() {

        if (concurrencyManager != null)
            concurrencyManager.shutdownNow();

        if (localTransactionManager != null)
            localTransactionManager.shutdownNow();

        if (resourceManager != null)
            resourceManager.shutdownNow();

//        if (metadataService!= null)
//            metadataService.shutdownNow();

    }

    /**
     * A minimal implementation of {@link IMetadataService} - only those methods
     * actually used by the {@link ResourceManager} are implemented.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class MockMetadataService implements IMetadataService {

        private AtomicInteger partitionId = new AtomicInteger(0);
       
        public int nextPartitionId(String name) throws IOException, InterruptedException, ExecutionException {
            return partitionId.incrementAndGet();
        }

        public UUID registerScaleOutIndex(IndexMetadata metadata, byte[][] separatorKeys, UUID[] dataServices) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public void dropScaleOutIndex(String name) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public UUID getServiceUUID() throws IOException {
            throw new UnsupportedOperationException();
        }

        public String getStatistics() throws IOException {
            throw new UnsupportedOperationException();
        }

        public void registerIndex(String name, IndexMetadata metadata) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public IndexMetadata getIndexMetadata(String name,long timestamp) throws IOException {
            throw new UnsupportedOperationException();
        }

        public String getStatistics(String name) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void dropIndex(String name) throws IOException, InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public ResultSet rangeIterator(long tx, String name, byte[] fromKey, byte[] toKey, int capacity, int flags, ITupleFilter filter) throws InterruptedException, ExecutionException, IOException {
            throw new UnsupportedOperationException();
        }

        public Object submit(long tx, String name, IIndexProcedure proc) throws InterruptedException, ExecutionException, IOException {
            throw new UnsupportedOperationException();
        }

        public IBlock readBlock(IResourceMetadata resource, long addr) throws IOException {
            throw new UnsupportedOperationException();
        }

        public long commit(long tx) throws ValidationError, IOException {
            throw new UnsupportedOperationException();
        }

        public void abort(long tx) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void splitIndexPartition(String name,
                PartitionLocator oldLocator, PartitionLocator[] newLocators)
                throws IOException, InterruptedException, ExecutionException {

            log.info("Split index partition: name=" + name + ", oldLocator="
                    + oldLocator + " into " + Arrays.toString(newLocators));

        }

        public void joinIndexPartition(String name,
                PartitionLocator[] oldLocators, PartitionLocator newLocator)
                throws IOException, InterruptedException, ExecutionException {

            log.info("Join index partitions: name=" + name + ", oldLocators="
                    + Arrays.toString(oldLocators) + " into " + newLocator );
            
        }

        public void moveIndexPartition(String name,
                PartitionLocator oldLocator, PartitionLocator newLocator)
                throws IOException, InterruptedException, ExecutionException {

            log.info("Move index partition: name=" + name + ", oldLocator="
                    + oldLocator + " to " + newLocator);
            
        }

        public PartitionLocator get(String name, long timestamp, byte[] key) throws InterruptedException, ExecutionException, IOException {
            // TODO Auto-generated method stub
            return null;
        }

        public PartitionLocator find(String name, long timestamp, byte[] key) throws InterruptedException, ExecutionException, IOException {
            // TODO Auto-generated method stub
            return null;
        }

        public void forceOverflow() throws IOException {
            
            throw new UnsupportedOperationException();
            
        }

        public long getOverflowCounter() throws IOException {

            throw new UnsupportedOperationException();
            
        }

    }
    
}
