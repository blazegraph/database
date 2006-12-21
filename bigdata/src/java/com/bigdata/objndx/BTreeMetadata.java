package com.bigdata.objndx;

import java.nio.ByteBuffer;

import com.bigdata.journal.Bytes;

/**
 * Used to pass multiple values out of {@link BTree#readMetadata} so that
 * various final fields can be set in the constructor form that loads an
 * existing tree from a store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo refactor to make the metadata record extensible.
 */
public class BTreeMetadata {

    /**
     * The address of the root node or leaf.
     */
    public final long addrRoot;
    
    public final int branchingFactor;

    public final int height;
    
    public final int nnodes;
    
    public final int nleaves;
    
    public final int nentries;
    
    public final ArrayType keyType;

    /**
     * Address that can be used to read this metadata record from the store.
     */
    public final long addrMetadata;
    
    /**
     * The #of bytes in the metadata record written by {@link #writeMetadata()}.
     */
    public static final int SIZEOF_METADATA = Bytes.SIZEOF_LONG
            + Bytes.SIZEOF_INT * 6;

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
        
        this.keyType = btree.keyType;
    
        this.addrMetadata = write(btree.store);
        
    }
    
    /**
     * Write out the persistent metadata for the btree on the store and
     * return the persistent identifier for that metadata. The metadata
     * include the persistent identifier of the root of the btree and the
     * height, #of nodes, #of leaves, and #of entries in the btree.
     * 
     * @return The persistent identifier for the metadata.
     */
    protected long write(IRawStore2 store) {

        ByteBuffer buf = ByteBuffer.allocate(SIZEOF_METADATA);

        buf.putLong(addrRoot);
        buf.putInt(branchingFactor);
        buf.putInt(height);
        buf.putInt(nnodes);
        buf.putInt(nleaves);
        buf.putInt(nentries);
        buf.putInt(keyType.intValue());
        
        buf.flip(); // prepare for writing.

        return store.write(buf);

    }

    /**
     * Read the persistent metadata record for the btree.
     * 
     * @param addrMetadta
     *            The address from which the metadata record will be read.
     *            
     * @return The persistent identifier of the root of the btree.
     */
    public BTreeMetadata(IRawStore2 store,long addrMetadata) {

        assert store != null;
        
        assert addrMetadata != 0L;
        
        this.addrMetadata = addrMetadata;
        
        ByteBuffer buf = store.read(addrMetadata,null);

        addrRoot = buf.getLong();
        
        branchingFactor = buf.getInt();
        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        height = buf.getInt();
        assert height >= 0;
        
        nnodes = buf.getInt();
        assert nnodes >= 0;
        
        nleaves = buf.getInt();
        assert nleaves >= 0;
        
        nentries = buf.getInt();
        assert nentries >= 0;
        
        keyType = ArrayType.parseInt( buf.getInt() );
        
        BTree.log.info(toString());

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
        sb.append(", keyType=" + keyType);
        sb.append(", addrMetadata=" + Addr.toString(addrMetadata));
        
        return sb.toString();
        
    }
    
}
