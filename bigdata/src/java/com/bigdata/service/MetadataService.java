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
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.jini.core.lookup.ServiceID;

import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.MasterJournal;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionMetadata;

/**
 * Implementation of a metadata service for a named scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support creation and management of scale-out indices, including mapping
 *       their index partitions to data services. Build out this functionality
 *       with a series of test cases that invoke the basic operations
 *       (registerIndex (done), getPartition (done), putPartition,
 *       getPartitions, movePartition, etc.) and handle the load-balancing
 *       later.
 * 
 * @todo Provide a means to reconstruct the metadata index from the journal and
 *       index segment data files. We tag each journal and index segment with a
 *       UUID. Each index is also tagged with a UUID, and that UUID is written
 *       into the metadata record for the index on each journal and index
 *       segment. Based on those UUIDs we are able to work backwards from the
 *       data on disk and identify the indices to which they belong. That
 *       information in combination with the timestamps in the metadata records
 *       and the first/last keys in the index partition is sufficient to
 *       regenerate the metadata indices.
 * 
 * @todo A temporal/immortable database can be realized if we never delete old
 *       journals since they contain the historical committed states of the
 *       database. The use of index segments would still provide fast read
 *       performance on recent data, while a suitable twist on the metadata
 *       index would allow access to those historical states. (E.g., you have to
 *       be able to access the historical state of the metadata index that
 *       corresponds to the commit time of interest for the database.)
 * 
 * @todo support two-tier metadata index and reconcile with
 *       {@link MetadataIndex} and {@link MasterJournal}.
 */
abstract public class MetadataService extends DataService implements
        IMetadataService, IServiceShutdown {

    /**
     * Return the name of the metadata index.
     * 
     * @param indexName
     *            The name of the scale-out index.
     * 
     * @return The name of the corresponding {@link MetadataIndex} that is used
     *         to manage the partitions in the named scale-out index.
     */
    public static String getMetadataName(String indexName) {
        
        return "metadata-"+indexName;
        
    }
    
    protected MetadataService(Properties properties) {

        super(properties);

    }
    
    /**
     * @todo if if exits already? (and has consistent/inconsistent metadata)?
     * 
     * @todo index metadata options (unicode support, per-partition counters,
     *       etc.) i had been passing in the BTree instance, but that does not
     *       work as well in a distributed environment.
     */
    public UUID registerIndex(String name) throws IOException,
            InterruptedException, ExecutionException {
        
        MetadataIndex mdi = (MetadataIndex) journal.serialize(
                new RegisterMetadataIndexTask(name)).get();
        
        UUID managedIndexUUID = mdi.getManagedIndexUUID();
        
        return managedIndexUUID; 
                
    }

    public UUID getManagedIndexUUID(String name) throws IOException {
        
        // the name of the metadata index itself.
        final String metadataName = getMetadataName(name);
        
        // make sure there is no metadata index for that btree.
        MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
        
        if(mdi == null) {
            
            return null;
            
        }
        
        return mdi.getManagedIndexUUID();
        
    }

    public IPartitionMetadata getPartition(String name,byte[] key) throws IOException {
        
        // the name of the metadata index itself.
        final String metadataName = getMetadataName(name);
        
        // make sure there is no metadata index for that btree.
        MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
        
        if(mdi == null) {
            
            throw new IllegalArgumentException("Index not registered: " + name);
            
        }

        /*
         * @todo this winds up deserializing the value into a PartitionMetadata
         * object and then re-serializing it to return to the remote client.
         */
        IPartitionMetadata pmd = mdi.find(key);
        
        if( pmd == null ) {
            
            throw new IllegalStateException("No partitioned in index: "+name);
            
        }
        
        return pmd;
        
    }
    
    /*
     * Tasks.
     */
    
    /**
     * Registers a metadata index for a named scale-out index and creates the
     * initial partition for the scale-out index on a {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class RegisterMetadataIndexTask extends AbstractIndexManagementTask {
        
        public RegisterMetadataIndexTask(String name) {

            super(name);
            
        }
        
        public Object call() throws Exception {

            // the name of the metadata index itself.
            final String metadataName = getMetadataName(name);
            
            // make sure there is no metadata index for that btree.
            if( journal.getIndex(metadataName) != null ) {
                
                throw new IllegalStateException("Already registered: name="
                        + name);
                
            }

            /*
             * Note: there are two UUIDs here - the UUID for the metadata index
             * describing the partitions of the named scale-out index and the UUID
             * of the named scale-out index. The metadata index UUID MUST be used by
             * all B+Tree objects having data for the metadata index (its mutable
             * btrees on journals and its index segments) while the managed named
             * index UUID MUST be used by all B+Tree objects having data for the
             * named index (its mutable btrees on journals and its index segments).
             */
            
            final UUID metadataIndexUUID = UUID.randomUUID();
            
            final UUID managedIndexUUID = UUID.randomUUID();
            
            MetadataIndex mdi = new MetadataIndex(journal, metadataIndexUUID,
                    managedIndexUUID, name);
            
            /*
             * Register the metadata index.
             */
            journal.registerIndex(metadataName, mdi);
            
            /*
             * Setup the initial partition which is able to accept any key.
             */
            
            ServiceID dataServiceID = getUnderUtilizedDataService();
            
            final UUID[] dataServices = new UUID[]{
                    
                    JiniUtil.serviceID2UUID(dataServiceID)
                    
            };
            
            mdi.put(new byte[]{}, new PartitionMetadata(0, dataServices ));

            journal.commit();

            /*
             * Create the initial partition of the scale-out index on the
             * selected data service.
             * 
             * FIXME This must be done using a restart-safe operation such that
             * the partition is either eventually created or the operation is
             * retracted and the partition is created on a different data
             * service. Note that this is a high-latency remote operation and
             * MUST NOT be run inside of the serialized write on the metadata
             * index itself. It is a good question exactly when this operation
             * should be run.... One option would be lazily by the data service
             * when it receives a request for an index / index key range that
             * was not known to be mapped to that data service.
             */
            
            IDataService dataService = getDataServiceByID(dataServiceID);
            
            if(dataService==null) {
                
                throw new RuntimeException("Condition is not supported");
                
            }
            
            /*
             * Register the index on the target data service (remote operation).
             */
            dataService.registerIndex(name, managedIndexUUID);
            
            return mdi;
            
        }
        
    }

}
