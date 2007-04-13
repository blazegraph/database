package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

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
     * Address that can be used to read this metadata record from the store.
     * <p>
     * Note: This is not persisted since we do not have the address until after
     * we have written out the state of this record.
     */
    protected transient /*final*/ long addrMetadata;
    
    /**
     * The {@link Addr address} that can be used to read this metadata record
     * from the store.
     */
    public long getMetadataAddr() {
        
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
        
        /*
         * Note: This can not be invoked here since a derived class will not 
         * have initialized its fields yet.  Therefore the write() is done by
         * the BTree.
         */
//        this.addrMetadata = write(btree.store);
        
    }
    
    /**
     * Write out the metadata record for the btree on the store and return the
     * {@link Addr address}.
     * 
     * @return The address of the metadata record.
     */
    protected long write(IRawStore store) {

        return store.write(ByteBuffer.wrap(SerializerUtil.serialize(this)));

    }

    /**
     * Read the metadata record from the store.
     * 
     * @param store
     *            the store.
     * @param addr
     *            the address of the metadata record.
     * 
     * @return the metadata record.
     * 
     * @see #load(IRawStore, long), which will load the {@link BTree} or derived
     *      subclass not just the {@link BTreeMetadata}.
     */
    public static BTreeMetadata read(IRawStore store, long addr) {
        
        return (BTreeMetadata) SerializerUtil.deserialize(store.read(addr));
        
    }

    /**
     * A human readable representation of the metadata record.
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder();

        sb.append("addrRoot=" + Addr.toString(addrRoot));
        sb.append(", branchingFactor=" + branchingFactor);
        sb.append(", height=" + height);
        sb.append(", nnodes=" + nnodes);
        sb.append(", nleaves=" + nleaves);
        sb.append(", nentries=" + nentries);
        sb.append(", addrMetadata=" + Addr.toString(addrMetadata));
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

        final int version = (int)LongPacker.unpackLong(in);
        
        if (version != VERSION0) {

            throw new IOException("Unknown version: version=" + version);
            
        }

        addrRoot = Addr.unpack(in);
        
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

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        LongPacker.packLong(out,VERSION0);
        
        Addr.pack(out, addrRoot);
        
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
        
    }
    
}
