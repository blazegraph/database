package com.bigdata.objndx;

import java.io.Externalizable;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * Used to persist metadata for a {@link BTree} so that a historical state may
 * be re-loaded from the store.
 * </p>
 * 
 * @todo The metadata record is extensible since it uses default java
 *       serialization. That makes it a bit fat, which we could address by
 *       implementing {@link Externalizable} but this is probably not much of an
 *       issue.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BTree#newMetadata(), which you must override if you subclass this class.
 */
public class BTreeMetadata implements Serializable {

    private static final long serialVersionUID = 4370669592664382720L;

    /**
     * The address of the root node or leaf.
     */
    public final long addrRoot;
    
    public final int branchingFactor;

    public final int height;
    
    public final int nnodes;
    
    public final int nleaves;
    
    public final int nentries;
    
    public final IValueSerializer valueSer;
    
    /**
     * The name of the class that will be used to re-load the index.
     */
    public final String className;
    
    public final RecordCompressor recordCompressor;

    public final boolean useChecksum;
   
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

//    /**
//     * De-serialization constructor.
//     */
//    public BTreeMetadata() {
//        
//    }
    
    /**
     * Constructor used to write out a metadata record.
     * 
     * @param btree
     *            The btree.
     */
    protected BTreeMetadata(BTree btree) {
        
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
        
        return sb.toString();
        
    }
    
}
