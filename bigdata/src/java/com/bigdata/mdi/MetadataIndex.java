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
package com.bigdata.mdi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IResourceManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.MetadataService;

/**
 * A metadata index for the partitions of a distributed index. There is one
 * metadata index for each distributed index. The keys of the metadata index are
 * the first key that would be directed into the corresponding index segment,
 * e.g., a <em>separator key</em> (this is just the standard btree semantics).
 * The values are serialized {@link PartitionLocator} objects.
 * <p>
 * Note: At this time the recommended scale-out approach for the metadata index
 * is to place the metadata indices on a {@link MetadataService} (the same
 * {@link MetadataService} may be used for an arbitrary #of scale-out indices)
 * and to <em>replicate</em> the state for the {@link MetadataService} onto
 * failover {@link MetadataService}s. Since the {@link MetadataIndex} may grow
 * without bound, you simply need to have enough disk on hand for it (the size
 * requirements are quite modest). Further, the {@link MetadataService} MUST NOT
 * be used to hold the data for the scale-out indices themselves since the
 * {@link MetadataIndex} can not undergo {@link IResourceManager#overflow()}.
 * <p>
 * One advantage of this approach is that the {@link MetadataIndex} is
 * guarenteed to hold all historical states of the partition definitions for
 * each index - effectively it is an immortal store for the partition metadata.
 * On the other hand it is not possible to compact the metadata index without
 * taking the database offline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The {@link MetadataIndex} does NOT support either overflow (it may NOT
 *       be a {@link FusedView}) NOR key-range splits. There are several issues
 *       involved:
 *       <p>
 *       (a) How to track the next partition identifier to be assigned to an
 *       index partition for the managed index. Currently this value is written
 *       in the {@link MetadataIndexCheckpoint} record and is propagated to the
 *       new backing store on overflow. However, if the metadata index is split
 *       into partitions then additional care MUST be taken to use only the
 *       value of that field on the 'meta-meta' index.
 *       <p>
 *       (b) how to locate the partitions of the metadata index itself.
 * 
 * @todo one way to locate the metadata-index partitions is to hash partition
 *       the metadata index and range queries can be flooded to all partitions.
 *       the #of metadata service nodes can be changed by a suitable broadcast
 *       event in which clients have to change to the new hash basis. this
 *       feature can be generalized to provide hash partitioned indices as well
 *       as key-range partitioned indices.
 * 
 * @todo A metadata index can be recovered by a distributed process running over
 *       the data services. Each data service reports all index partitions. The
 *       reports are collected and the index is rebuilt from the reports. Much
 *       like a map/reduce job.
 */
public class MetadataIndex extends BTree implements IMetadataIndex {

    /**
     * Used to implement find(byte[] key).
     */
    private transient final MetadataIndexView view;
    
    public MetadataIndexMetadata getIndexMetadata() {
        
        return (MetadataIndexMetadata) super.getIndexMetadata();
        
    }
    
    public IndexMetadata getScaleOutIndexMetadata() {
        
        return getIndexMetadata().getManagedIndexMetadata();
        
    }
    
    /**
     * Returns the value to be assigned to the next partition created on this
     * {@link MetadataIndex} and then increments the counter. The counter will
     * be made restart-safe iff the index is dirty, the index is registered as
     * an {@link ICommitter}, and the store on which the index is stored is
     * committed.
     * <p>
     * Note: The metadata index uses a 32-bit partition identifier rather than
     * the {@link #getCounter()}. The reason is that the {@link Counter} uses
     * the partition identifier in the high word and a partition local counter
     * in the low word. Therefore we have to centralize the assignment of the
     * partition identifier, even when the metadata index is itself split into
     * partitions. Requests for partition identifiers need to be directed to the
     * root partition (L0) for the {@link MetadataIndex}.
     */
    public int incrementAndGetNextPartitionId() {
        
        final int tmp = nextPartitionId++;

        /*
         * Notify listener that the index is dirty.
         */
        
        fireDirtyEvent();
        
        return tmp;
        
    }

    private int nextPartitionId;
    
    /**
     * Create a new {@link MetadataIndex}.
     * 
     * @param store
     *            The backing store.
     * @param indexUUID
     *            The unique identifier for the metadata index.
     * @param managedIndexMetadata
     *            The metadata template for the managed scale-out index.
     */
    public static MetadataIndex create(IRawStore store, UUID indexUUID,
            IndexMetadata managedIndexMetadata) {

        final MetadataIndexMetadata metadata = new MetadataIndexMetadata(
                managedIndexMetadata.getName(), indexUUID, managedIndexMetadata);

        /*
         * Note: the metadata index should probably delete markers so that we
         * can do compacting merges on it.
         */
        
        metadata.setDeleteMarkers(true);

        /*
         * Override the implementation class.
         */
        
        metadata.setClassName(MetadataIndex.class.getName());

        /*
         * Override the checkpoint record implementation class.
         */
        metadata.setCheckpointClassName(MetadataIndexCheckpoint.class.getName());

        /*
         * Tuple serializer.
         */
        metadata.setTupleSerializer(PartitionLocatorTupleSerializer.newInstance());
        
        return (MetadataIndex) BTree.create(store, metadata);
        
    }

    /**
     * Required ctor.
     * 
     * @param store
     * @param checkpoint
     * @param metadata
     */
    public MetadataIndex(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {
        
        super(store, checkpoint, metadata);

        /*
         * copy the initial value from the checkpoint record.
         */

        nextPartitionId = ((MetadataIndexCheckpoint)checkpoint).getNextPartitionId();
        
        view = new MetadataIndexView(this);
        
    }
    
    /**
     * Extended to require a checkpoint if {@link #incrementAndGetNextPartitionId()} has been
     * invoked.
     */
    public boolean needsCheckpoint() {

        if(nextPartitionId != ((MetadataIndexCheckpoint)getCheckpoint()).getNextPartitionId()) {
            
            return true;
            
        }
        
        return super.needsCheckpoint();
        
    }
    
    /**
     * Extends the {@link Checkpoint} record to store the next partition
     * identifier to be assigned by the metadata index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MetadataIndexCheckpoint extends Checkpoint {

        /**
         * 
         */
        private static final long serialVersionUID = 6482587101150014793L;

        private int nextPartitionId;

        /**
         * The immutable value of the <code>nextPartitionId</code> counter
         * stored in the metadata record.
         */
        public int getNextPartitionId() {

            return nextPartitionId;

        }
        
        /**
         * De-serialization constructor.
         */
        public MetadataIndexCheckpoint() {
            
        }
        
        /**
         * @param btree
         */
        public MetadataIndexCheckpoint(BTree btree) {
            
            super(btree);
            
            nextPartitionId = ((MetadataIndex)btree).nextPartitionId;
            
        }

        /**
         * Create the initial checkpoint record for the initial metadata index.
         * 
         * @param metadata
         */
        public MetadataIndexCheckpoint(IndexMetadata metadata) {

            super(metadata);

            // The first partitionId is zero(0).
            nextPartitionId = 0;
            
        }
        
        /**
         * Create the initial checkpoint record when the metadata index
         * overflows onto a new backing store.
         * 
         * @param metadata
         */
        public MetadataIndexCheckpoint(IndexMetadata metadata, Checkpoint oldCheckpoint) {

            super(metadata, oldCheckpoint);

            // propagate the value of this field onto the new backing store.
            nextPartitionId = ((MetadataIndexCheckpoint)oldCheckpoint).nextPartitionId;
            
        }
        
        final transient private static int VERSION0 = 0x0;
        
        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            super.readExternal(in);
            
            final int version = (int) LongPacker.unpackLong(in);

            if (version != 0)
                throw new IOException("Unknown version: " + version);

            nextPartitionId = in.readInt();
            
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {

            super.writeExternal(out);

            LongPacker.packLong(out, VERSION0);

            out.writeInt(nextPartitionId);
            
        }

    }

    /**
     * Extends the {@link IndexMetadata} record to hold the metadata template
     * for the managed scale-out index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MetadataIndexMetadata extends IndexMetadata implements Externalizable {

        private static final long serialVersionUID = -7309267778881420043L;
        
        private IndexMetadata scaleOutIndexMetadata;
        
        /**
         * The managed index metadata
         */
        public final IndexMetadata getManagedIndexMetadata() {
            
            return scaleOutIndexMetadata;
            
        }
        
        /**
         * De-serialization constructor.
         */
        public MetadataIndexMetadata() {
            
        }

        /**
         * First time constructor.
         * 
         * @param name
         *            The name of the managed index. The name of the metadata
         *            index is given by
         *            {@link MetadataService#getMetadataIndexName(String)}
         * @param indexUUID
         *            The UUID of the metadata index.
         * @param managedIndexMetadata
         *            The metadata template for the managed index.
         */
        public MetadataIndexMetadata(String managedIndexName, UUID indexUUID, IndexMetadata managedIndexMetadata) {

            super(MetadataService.getMetadataIndexName(managedIndexName), indexUUID);
            
            if(managedIndexMetadata == null) {
                
                throw new IllegalArgumentException();
                
            }
            
            this.scaleOutIndexMetadata = managedIndexMetadata;
            
        }

        private static final transient int VERSION0 = 0x0;

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            super.readExternal(in);

            final int version = (int) LongPacker.unpackLong(in);

            if (version != VERSION0) {

                throw new IOException("Unknown version: version=" + version);

            }

            scaleOutIndexMetadata = (IndexMetadata) in.readObject();

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            super.writeExternal(out);

            LongPacker.packLong(out, VERSION0);

            out.writeObject(scaleOutIndexMetadata);

        }

    }

    public PartitionLocator get(byte[] key) {
        
//        return (PartitionLocator)SerializerUtil.deserialize(lookup(key));
        
        // automatic de-serialization using the ITupleSerializer.
        return (PartitionLocator)super.lookup((Object)key);
        
    }

    public PartitionLocator find(byte[] key) {

        return view.find(key);
        
    }

    /**
     * Used to (de-)serialize {@link PartitionLocator}s in the
     * {@link MetadataIndex}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class PartitionLocatorTupleSerializer extends
            DefaultTupleSerializer<byte[]/*key*/, PartitionLocator/*val*/> {

        /**
         * 
         */
        private static final long serialVersionUID = -4178430896409893596L;

        public static PartitionLocatorTupleSerializer newInstance() {
         
            return new PartitionLocatorTupleSerializer(getDefaultKeyBuilderFactory());
            
        }
        
        /**
         * De-serialization ctor <em>only</em>.
         */
        public PartitionLocatorTupleSerializer() {
            
        }
        
        /**
         * @param keyBuilderFactory
         */
        public PartitionLocatorTupleSerializer(
                IKeyBuilderFactory keyBuilderFactory) {

            super(keyBuilderFactory);
            
        }

    }

    /**
     * Passes the notice along to the {@link #view}. It caches de-serialized
     * locators and needs to drop them from its cache if they become stale.
     */
    public void staleLocator(PartitionLocator locator) {
        
        view.staleLocator(locator);
        
    }

}
