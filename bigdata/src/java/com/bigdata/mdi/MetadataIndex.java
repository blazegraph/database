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
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IndexSegment;
import com.bigdata.io.SerializerUtil;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.MetadataService;

/**
 * A metadata index for the partitions of a distributed index. There is one
 * metadata index for each distributed index. The keys of the metadata index are
 * the first key that would be directed into the corresponding index segment,
 * e.g., a <em>separator key</em> (this is just the standard btree semantics).
 * The values are serialized {@link PartitionMetadata} objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo consider changing the values to
 *       {@link PartitionMetadataWithSeparatorKeys} objects. the metadata index
 *       does not to store either separator key since they are redundent with
 *       the key for an index partition's metadata and the key for its
 *       successor, but it is handy for clients to get it along with the rest of
 *       the partition metadata.
 * 
 * @todo Track which {@link IndexSegment}s and {@link Journal}s are required
 *       to support the {@link IsolatedFusedView}s in use by a {@link Tx}.
 *       Deletes of old journals and index segments MUST be deferred until no
 *       transaction remains which can read those data. This metadata must be
 *       restart-safe so that resources are eventually deleted.
 * 
 * @todo the {@link MetadataIndex} does not support overflow or splits. There
 *       are several issues to be solved
 *       <p>
 *       (a) How to track the next partition identifier to be assigned to an
 *       index partition for the managed index. Currently this value is written
 *       in the {@link MetadataIndexCheckpoint} record. Care MUST be applied to
 *       ensure that this value is not lost during overflow or split. Further,
 *       if the metadata index is split into partitions then additional care
 *       MUST be taken to use only the value of that field on the 'meta-meta'
 *       index.
 *       <p>
 *       (b) how to locate the partitions of the metadata index itself.
 */
public class MetadataIndex extends BTree implements IMetadataIndex {

    public IndexMetadata getScaleOutIndexMetadata() {
        
        return ((MetadataIndexMetadata) metadata).scaleOutIndexMetadata;
        
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
    public int nextPartitionId() {
        
        return nextPartitionId++;
        
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
        public MetadataIndexCheckpoint(IndexMetadata metadata, long counter) {

            super(metadata, counter);

            // The first partitionId is zero(0).
            nextPartitionId = 0;
            
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

    /**
     * Find the index of the partition spanning the given key.
     * 
     * @return The index of the partition spanning the given key or
     *         <code>-1</code> iff there are no partitions defined.
     * 
     * @exception IllegalStateException
     *                if there are partitions defined but no partition spans the
     *                key. In this case the {@link MetadataIndex} lacks an entry
     *                for the key <code>new byte[]{}</code>.
     */
    public int findIndexOf(byte[] key) {
        
        int pos = super.indexOf(key);
        
        if (pos < 0) {

            /*
             * the key lies between the partition separators and represents the
             * insert position.  we convert it to an index and subtract one to
             * get the index of the partition that spans this key.
             */
            
            pos = -(pos+1);

            if(pos == 0) {

                if(nentries != 0) {
                
                    throw new IllegalStateException(
                            "Partition not defined for empty key.");
                    
                }
                
                return -1;
                
            }
                
            pos--;

            return pos;
            
        } else {
            
            /*
             * exact hit on a partition separator, so we choose the entry with
             * that key.
             */
            
            return pos;
            
        }

    }
    
    /**
     * Find and return the partition spanning the given key.
     * 
     * @return The partition spanning the given key or <code>null</code> if
     *         there are no partitions defined.
     */
    public PartitionMetadata find(byte[] key) {
        
        final int index = findIndexOf(key);
        
        if(index == -1) return null;
        
        byte[] val = (byte[]) valueAt(index);
        
        return (PartitionMetadata) SerializerUtil.deserialize(val);
        
    }
    
    /**
     * The partition with that separator key or <code>null</code> (exact
     * match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The partition with that separator key or <code>null</code>.
     */
    public PartitionMetadata get(byte[] key) {
        
        byte[] val = (byte[]) lookup(key);
        
        if(val==null) return null;
        
        return (PartitionMetadata) SerializerUtil.deserialize(val);
        
    }
    
    /**
     * Create or update a partition (exact match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * @param val
     *            The parition metadata.
     * 
     * @return The previous partition metadata for that separator key or
     *         <code>null</code> if there was no partition for that separator
     *         key.
     * 
     * @exception IllegalArgumentException
     *                if the key identifies an existing partition but the
     *                partition identifers do not agree.
     */
    public PartitionMetadata put(byte[] key,PartitionMetadata val) {
        
        if (val == null) {

            throw new IllegalArgumentException();

        }
        
        byte[] newval = SerializerUtil.serialize(val);
        
        byte[] oldval2 = (byte[])insert(key, newval);

        PartitionMetadata oldval = oldval2 == null ? null
                : (PartitionMetadata) SerializerUtil.deserialize(oldval2);
        
        if (oldval != null && oldval.getPartitionId() != val.getPartitionId()) {

            throw new IllegalArgumentException("Expecting: partId="
                    + oldval.getPartitionId() + ", but have partId="
                    + val.getPartitionId());

        }

        return oldval;
    
    }

//    /**
//     * Remove a partition (exact match on the separator key).
//     * 
//     * @param key
//     *            The separator key (the first key that would go into that
//     *            partition).
//     * 
//     * @return The existing partition for that separator key or
//     *         <code>null</code> if there was no entry for that separator key.
//     */
//    public PartitionMetadata remove(byte[] key) {
//        
//        byte[] oldval2 = (byte[])super.remove(key);
//
//        PartitionMetadata oldval = oldval2 == null ? null
//                : (PartitionMetadata) SerializerUtil.deserialize(oldval2);
//
//        return oldval;
//        
//    }

}
