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
package com.bigdata.scaleup;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.io.SerializerUtil;
import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.rawstore.IRawStore;

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
     */
    public MetadataIndex(IRawStore store, UUID indexUUID,
            UUID managedIndexUUID, String managedIndexName) {

        super(store, indexUUID );
        
        this.managedIndexName = managedIndexName;

        //
        this.managedIndexUUID = managedIndexUUID;
        
    }

    public MetadataIndex(IRawStore store, BTreeMetadata metadata) {
        
        super(store, metadata);
        
        managedIndexName = ((MetadataIndexMetadata)metadata).getName();

        managedIndexUUID = ((MetadataIndexMetadata)metadata).getManagedIndexUUID();
        
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
        private UUID managedIndexUUID;
        
        /**
         * The managedIndexName of the metadata index, which is the always the same as the managedIndexName
         * under which the corresponding {@link PartitionedIndexView} was registered.
         */
        public final String getName() {
            
            return name;
            
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
            
            this.managedIndexUUID = mdi.getManagedIndexUUID();
            
        }

        private static final transient int VERSION0 = 0x0;
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
            super.readExternal(in);
            
            final int version = (int)LongPacker.unpackLong(in);
            
            if (version != VERSION0) {

                throw new IOException("Unknown version: version=" + version);
                
            }
            
            name = in.readUTF();
            
            managedIndexUUID = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);
            
        }
        
        public void writeExternal(ObjectOutput out) throws IOException {

            super.writeExternal(out);
            
            LongPacker.packLong(out,VERSION0);

            out.writeUTF(name);
            
            out.writeLong(managedIndexUUID.getMostSignificantBits());
            
            out.writeLong(managedIndexUUID.getLeastSignificantBits());
            
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
