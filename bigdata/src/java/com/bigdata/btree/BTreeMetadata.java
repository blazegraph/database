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
package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.IStoreObjectInputStream;

/**
 * <p>
 * Used to persist metadata for a {@link BTree} so that a historical state may
 * be re-loaded from the store.
 * </p>
 * <p>
 * Note: Derived classes SHOULD extend the {@link Externalizable} interface and
 * explicitly manage serialization versions so that their metadata may evolve in
 * a backward compatible manner.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BTree#newMetadata(), which you must override if you subclass this class.
 */
public class BTreeMetadata implements Serializable, Externalizable {

    private static final long serialVersionUID = 4370669592664382720L;

    private transient IRawStore store;
    
    private long addrRoot;
    
    private int branchingFactor;

    private int height;
    
    private int nnodes;
    
    private int nleaves;
    
    private int nentries;
    
    private IValueSerializer valueSer;
    
    private String className;
    
    private RecordCompressor recordCompressor;

    private boolean useChecksum;

    private UUID indexUUID;
    
    private long counter;
    
    /**
     * The backing store.
     */
    public final IRawStore getStore() {
       
        return store;
        
    }
    
    /**
     * The address of the root node or leaf.
     */
    public final long getRootAddr() {
        
        return addrRoot;
        
    }
    
    public final int getBranchingFactor() {return branchingFactor;}
    
    public final int getHeight() {return height;}

    public final int getNodeCount() {return nnodes;}

    public final int getLeafCount() {return nleaves;}

    public final int getEntryCount() {return nentries;}
    
    public final IValueSerializer getValueSerializer() {return valueSer;}
    
    /**
     * The name of a class derived from {@link BTree} that will be used to
     * re-load the index.
     */
    public final String getClassName() {return className;}

    /**
     * The object that will handle record (de-)compressor -or- <code>null</code>
     * iff records are not compressed.
     */
    public final RecordCompressor getRecordCompressor() {return recordCompressor;}

    /**
     * True iff node/leaf checksums are in use.
     */
    public final boolean getUseChecksum() {return useChecksum;}
    
    /**
     * The unique identifier for the index whose data is accessible from this
     * metadata record.
     * <p>
     * All {@link AbstractBTree}s having data for the same index will have the
     * same {@link #indexUUID}. A partitioned index is comprised of mutable
     * {@link BTree}s and historical read-only {@link IndexSegment}s, all of
     * which will have the same {@link #indexUUID} if they have data for the
     * same scale-out index.
     */
    public final UUID getIndexUUID() {
        
        return indexUUID;
        
    }
   
    /**
     * Return the value of the counter stored in the metadata record.
     */
    public final long getCounter() {
        
        return counter;
        
    }
    
    /**
     * Address that can be used to read this metadata record from the store.
     * <p>
     * Note: This is not persisted since we do not have the address until after
     * we have written out the state of this record.
     */
    protected transient /*final*/ long addrMetadata;
    
    /**
     * The address that can be used to read this metadata record from the store.
     */
    final public long getMetadataAddr() {
        
        return addrMetadata;
        
    }

    /**
     * De-serialization constructor.
     */
    public BTreeMetadata() {
        
    }
    
    /**
     * Constructor used to write out a metadata record.
     * 
     * @param btree
     *            The btree.
     */
    protected BTreeMetadata(BTree btree) {
        
        assert btree.isOpen();
        
        this.store = btree.store;
        
        this.addrRoot = btree.root.getIdentity();
        
        this.branchingFactor = btree.branchingFactor;
        
        this.height = btree.height;
        
        this.nnodes = btree.nnodes;
        
        this.nleaves = btree.nleaves;

        this.nentries = btree.nentries;

        this.valueSer = btree.nodeSer.valueSerializer;
        
        this.className = btree.getClass().getName();
        
        this.recordCompressor = btree.nodeSer.recordCompressor;

        this.useChecksum = btree.nodeSer.useChecksum;
        
        this.indexUUID = btree.indexUUID;
        
        this.counter = btree.counter;
        
        /*
         * Note: This can not be invoked here since a derived class will not 
         * have initialized its fields yet.  Therefore the write() is done by
         * the BTree.
         */
//        this.addrMetadata = write(btree.store);
        
    }
    
    /**
     * Write out the metadata record for the btree on the store and return the
     * address.
     * 
     * @param btree
     *            The btree whose metadata record is being written.
     * 
     * @param store
     *            The store on which the metadata record is being written.
     * 
     * @return The address of the metadata record.
     */
    protected long write(BTree btree, IRawStore store) {

        return store.write(ByteBuffer.wrap(store.serialize(this)));

    }

    /**
     * Read the metadata record from the store.
     * <p>
     * See {@link BTree#load(IRawStore, long)}, which will load the
     * {@link BTree} or derived subclass not just the {@link BTreeMetadata}.
     * 
     * @param store
     *            the store.
     * @param addr
     *            the address of the metadata record.
     * 
     * @return the metadata record.
     */
    public static BTreeMetadata read(IRawStore store, long addr) {
        
        BTreeMetadata metadata = (BTreeMetadata) store.deserialize(store.read(addr));
        
        // save the address from which the metadata record was loaded.
        metadata.addrMetadata = addr;
        
        return metadata;
        
    }

    /**
     * A human readable representation of the metadata record.
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder();

        sb.append("addrRoot=" + store.toString(addrRoot));
        sb.append(", branchingFactor=" + branchingFactor);
        sb.append(", height=" + height);
        sb.append(", nnodes=" + nnodes);
        sb.append(", nleaves=" + nleaves);
        sb.append(", nentries=" + nentries);
        sb.append(", addrMetadata=" + store.toString(addrMetadata));
        sb.append(", valueSerializer=" + valueSer.getClass().getName());
        sb.append(", recordCompressor="
                + (recordCompressor == null ? null : recordCompressor
                        .getClass().getName()));
        sb.append(", useChecksum=" + useChecksum);
        sb.append(", indexUUID="+indexUUID);
        
        return sb.toString();
        
    }

    private static transient final int VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        final IStoreObjectInputStream is = (IStoreObjectInputStream)in;
        
        final int version = (int)LongPacker.unpackLong(in);
        
        if (version != VERSION0) {

            throw new IOException("Unknown version: version=" + version);
            
        }

        store = is.getStore();
        
        addrRoot = store.unpackAddr(in);
        
        branchingFactor = (int)LongPacker.unpackLong(in);

        height = (int)LongPacker.unpackLong(in);
        
        nnodes = (int)LongPacker.unpackLong(in);

        nleaves = (int)LongPacker.unpackLong(in);

        nentries = (int)LongPacker.unpackLong(in);
        
        valueSer = (IValueSerializer)in.readObject();
        
        className = in.readUTF();
        
        recordCompressor = (RecordCompressor)in.readObject();
        
        useChecksum = in.readBoolean();
        
        indexUUID = new UUID(in.readLong()/*MSB*/,in.readLong()/*LSB*/);
        
        counter = LongPacker.unpackLong(in);

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        LongPacker.packLong(out,VERSION0);
        
        store.packAddr(out, addrRoot);
        
        LongPacker.packLong(out, branchingFactor);
        
        LongPacker.packLong(out, height);
        
        LongPacker.packLong(out, nnodes);
        
        LongPacker.packLong(out, nleaves);
        
        LongPacker.packLong(out, nentries);
        
        out.writeObject(valueSer);
        
        out.writeUTF(className);
        
        out.writeObject( recordCompressor );
        
        out.writeBoolean(useChecksum);
        
        out.writeLong(indexUUID.getMostSignificantBits());
        
        out.writeLong(indexUUID.getLeastSignificantBits());
        
        LongPacker.packLong(out, counter);
        
    }
    
}
