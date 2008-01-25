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
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.io.SerializerUtil;
import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.scaleup.PartitionedIndexView;

/**
 * A metadata index for the partitions of a distributed index. There is one
 * metadata index for each distributed index. The keys of the metadata index are
 * the first key that would be directed into the corresponding index segment,
 * e.g., a <em>separator key</em> (this is just the standard btree semantics).
 * The values are {@link PartitionMetadata} objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo the partition identifier counter should be broken down into each
 *       partition of the metadata index so that we can split L0 (the root
 *       metadata index) into a set of L1 metadata index partitions. The int32
 *       partition identifiers may be used to form int64 unique identifiers
 *       within the various data partitions.
 * 
 * @todo mutation operations need to be synchronized.
 * 
 * @todo Track which {@link IndexSegment}s and {@link Journal}s are required
 *       to support the {@link IsolatedBTree}s in use by a {@link Tx}. Deletes
 *       of old journals and index segments MUST be deferred until no
 *       transaction remains which can read those data. This metadata must be
 *       restart-safe so that resources are eventually deleted.
 */
public class MetadataIndex extends UnisolatedBTree {

    /**
     * The name of the managed index.
     */
    private final String managedIndexName;
    
    /**
     * The name of the managed index.
     */
    final public String getManagedIndexName() {
        
        return managedIndexName;
        
    }
    
    private final UnisolatedBTreePartitionConstructor ctor;
    
    /**
     * The object used to create mutable {@link BTree}s for to absorb writes on
     * the index partitions of the scale-out index managed by this
     * {@link MetadataIndex}.
     */
    final public UnisolatedBTreePartitionConstructor getIndexConstructor() {
        
        return ctor;
        
    }
    
    /**
     * The unique identifier for the index whose data metadata is managed by
     * this {@link MetadataIndex}.
     * <p>
     * When using a scale-out index the same <i>indexUUID</i> MUST be assigned
     * to each mutable and immutable B+Tree having data for any partition of
     * that scale-out index. This makes it possible to work backwards from the
     * B+Tree data structures and identify the index to which they belong. This
     * field is that UUID. Note that the inherited {@link #getIndexUUID()}
     * method provides the UUID of the metadata index NOT the managed index!
     * 
     * @see IIndex#getIndexUUID()
     */
    protected final UUID managedIndexUUID;

    /**
     * The unique identifier for the index whose data metadata is managed by
     * this {@link MetadataIndex}.
     * <p>
     * When using a scale-out index the same <i>indexUUID</i> MUST be assigned
     * to each mutable and immutable B+Tree having data for any partition of
     * that scale-out index. This makes it possible to work backwards from the
     * B+Tree data structures and identify the index to which they belong. This
     * field is that UUID. Note that the inherited {@link #getIndexUUID()}
     * method provides the UUID of the metadata index NOT the managed index!
     * 
     * @see IIndex#getIndexUUID()
     */
    public UUID getManagedIndexUUID() {
        
        return managedIndexUUID;
        
    }

    /**
     * Returns the value to be assigned to the next partition created on this
     * {@link MetadataIndex} and then increments the counter. The counter will
     * be made restart-safe iff the index is dirty, the index is registered as
     * an {@link ICommitter}, and the store on which the index is stored is
     * committed.
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
     * @param managedIndexUUID
     *            The unique identifier for the managed scale-out index.
     * @param managedIndexName
     *            The managedIndexName of the managed scale out index.
     * @param ctor
     *            The constructor used to create instances of the mutable btree
     *            indices for the scale-out index.
     */
    public MetadataIndex(IRawStore store, UUID indexUUID,
            UUID managedIndexUUID, String managedIndexName,
            UnisolatedBTreePartitionConstructor ctor) {

        super(store, indexUUID );
        
        //
        this.managedIndexUUID = managedIndexUUID;

        this.managedIndexName = managedIndexName;

        this.ctor = ctor;
        
        // The first partitionId is zero(0).
        this.nextPartitionId = 0;
        
    }

    public MetadataIndex(IRawStore store, BTreeMetadata metadata) {
        
        super(store, metadata);
        
        managedIndexUUID = ((MetadataIndexMetadata)metadata).getManagedIndexUUID();

        managedIndexName = ((MetadataIndexMetadata)metadata).getName();

        ctor = ((MetadataIndexMetadata)metadata).getIndexConstructor();
        
        nextPartitionId = ((MetadataIndexMetadata)metadata).getNextPartitionId();
        
    }

    protected BTreeMetadata newMetadata() {
        
        return new MetadataIndexMetadata(this);
        
    }

    /**
     * Extends the {@link BTreeMetadata} record to also hold the managedIndexName of the
     * partitioned index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MetadataIndexMetadata extends UnisolatedBTreeMetadata implements Externalizable {

        private static final long serialVersionUID = -7309267778881420043L;
        
        private String name;
        private UnisolatedBTreePartitionConstructor ctor;
        private UUID managedIndexUUID;
        private int nextPartitionId;
        
        /**
         * The managedIndexName of the metadata index, which is the always the same as the managedIndexName
         * under which the corresponding {@link PartitionedIndexView} was registered.
         */
        public final String getName() {
            
            return name;
            
        }
        
        /**
         * The object used to create mutable {@link BTree}s for to absorb writes on
         * the index partitions of the scale-out index managed by this
         * {@link MetadataIndex}.
         */
        final public UnisolatedBTreePartitionConstructor getIndexConstructor() {
            
            return ctor;
            
        }

        /**
         * The unique identifier for the index whose data metadata is managed by
         * this {@link MetadataIndex}.
         * <p>
         * When using a scale-out index the same <i>indexUUID</i> MUST be assigned
         * to each mutable and immutable B+Tree having data for any partition of
         * that scale-out index. This makes it possible to work backwards from the
         * B+Tree data structures and identify the index to which they belong. This
         * field is that UUID. Note that the inherited {@link #getIndexUUID()}
         * method provides the UUID of the metadata index NOT the managed index!
         */
        public final UUID getManagedIndexUUID() {
            
            return managedIndexUUID;
            
        }

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
        public MetadataIndexMetadata() {
            
        }
        
        /**
         * @param mdi
         */
        protected MetadataIndexMetadata(MetadataIndex mdi) {

            super(mdi);
            
            this.name = mdi.getManagedIndexName();
            
            this.ctor = mdi.getIndexConstructor();
            
            this.managedIndexUUID = mdi.getManagedIndexUUID();
            
            this.nextPartitionId = mdi.nextPartitionId;
            
        }

        private static final transient int VERSION0 = 0x0;
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
            super.readExternal(in);
            
            final int version = (int)LongPacker.unpackLong(in);
            
            if (version != VERSION0) {

                throw new IOException("Unknown version: version=" + version);
                
            }
            
            name = in.readUTF();

            ctor = (UnisolatedBTreePartitionConstructor)in.readObject();
            
            managedIndexUUID = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);

            nextPartitionId = (int)LongPacker.unpackLong(in);
            
        }
        
        public void writeExternal(ObjectOutput out) throws IOException {

            super.writeExternal(out);
            
            LongPacker.packLong(out,VERSION0);

            out.writeUTF(name);
            
            out.writeObject(ctor);
            
            out.writeLong(managedIndexUUID.getMostSignificantBits());
            
            out.writeLong(managedIndexUUID.getLeastSignificantBits());
            
            LongPacker.packLong(out, nextPartitionId);
            
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

    /**
     * Remove a partition (exact match on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The existing partition for that separator key or
     *         <code>null</code> if there was no entry for that separator key.
     */
    public PartitionMetadata remove(byte[] key) {
        
        byte[] oldval2 = (byte[])super.remove(key);

        PartitionMetadata oldval = oldval2 == null ? null
                : (PartitionMetadata) SerializerUtil.deserialize(oldval2);

        return oldval;
        
    }

}
