package com.bigdata.objndx;

import java.io.Externalizable;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import com.bigdata.io.SerializerUtil;
import com.bigdata.isolation.IConflictResolver;
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
     * @see #load(IRawStore, long), which will load the btree not just the
     *      {@link BTreeMetadata}.
     * 
     * @todo review remaining uses of this method vs
     *       {@link #load(IRawStore, long)}.
     */
    public static BTreeMetadata read(IRawStore store, long addr) {
        
        return (BTreeMetadata) SerializerUtil.deserialize(store.read(addr));
        
    }

    /**
     * Re-load the {@link BTree} or derived class from the store. The
     * {@link BTree} or derived class MUST provide a public construct with the
     * following signature: <code>
     * 
     * <i>className</i>(IRawStore store, BTreeMetadata metadata)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * @param addr
     *            The address of the {@link BTreeMetadata} record for that
     *            class.
     * 
     * @return The {@link BTree} or derived class loaded from that
     *         {@link BTreeMetadata} record.
     * 
     * @see BTree#newMetadata(), which MUST be overloaded if you subclass extend
     *      {@link BTreeMetadata}.
     */
    public static BTree load(IRawStore store, long addr) {
        
        BTreeMetadata metadata = read(store, addr);
        
        try {
            
            Class cl = Class.forName(metadata.className);
            
            Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class, BTreeMetadata.class });
            
            BTree btree = (BTree) ctor.newInstance(new Object[] { store,
                    metadata });
            
            return btree;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
//    /**
//     * Write out the metadata record for the btree on the store and return the
//     * {@link Addr address}.
//     * 
//     * @return The address of the metadata record.
//     */
//    protected long write(IRawStore store) {
//
//        ByteBuffer buf = ByteBuffer.allocate(SIZEOF_METADATA);
//
//        buf.putLong(addrRoot);
//        buf.putInt(branchingFactor);
//        buf.putInt(height);
//        buf.putInt(nnodes);
//        buf.putInt(nleaves);
//        buf.putInt(nentries);
//        buf.putInt(keyType.intValue());
//        
//        buf.flip(); // prepare for writing.
//
//    }

//    /**
//     * Read the persistent metadata record for the btree.
//     * 
//     * @param addrMetadta
//     *            The address from which the metadata record will be read.
//     * 
//     * @return The persistent identifier of the root of the btree.
//     */
//    public BTreeMetadata(IRawStore store,long addrMetadata) {
//
//        assert store != null;
//        
//        assert addrMetadata != 0L;
//        
//        this.addrMetadata = addrMetadata;
//        
//        ByteBuffer buf = store.read(addrMetadata,null);
//        assert buf.position() == 0;
//        assert buf.limit() == Addr.getByteCount(addrMetadata);
//        
//        addrRoot = buf.getLong();
//        
//        branchingFactor = buf.getInt();
//        
//        height = buf.getInt();
//
//        nnodes = buf.getInt();
//        
//        nleaves = buf.getInt();
//        
//        nentries = buf.getInt();
//        
//        keyType = ArrayType.parseInt( buf.getInt() );
//        
//        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
//        assert height >= 0;
//        assert nnodes >= 0;
//        assert nleaves >= 0;
//        assert nentries >= 0;
//
//        BTree.log.info(toString());
//
//    }

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
        
        return sb.toString();
        
    }
    
}
