package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IRawStore;

/**
 * A checkpoint record is written each time the btree is flushed to the
 * store.
 * <p>
 * Note: In order to create a btree use
 * {@link BTree#create(IRawStore, IndexMetadata)} to write the initial
 * {@link IndexMetadata} record and the initial check point on the store. It
 * will then load the {@link BTree} from the {@link Checkpoint} record and
 * you can start using the index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Checkpoint implements Externalizable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -4308251060627051232L;

    // transient and set on write or read-back.
    transient long addrCheckpoint;

    // persistent and immutable.
    private long addrMetadata;
    private long addrRoot;
    private int height;
    private int nnodes;
    private int nleaves;
    private int nentries;
    private long counter;

    /** Note: added in {@link #VERSION1} and presumed 0L in earlier versions. */
    private long addrBloomFilter;

    /**
     * The address used to read this {@link Checkpoint} record from the
     * store.
     * <p>
     * Note: This is set as a side-effect by {@link #write(IRawStore)}.
     * 
     * @throws IllegalStateException
     *             if the {@link Checkpoint} record has not been written on
     *             a store.
     */
    final public long getCheckpointAddr() {
        
        if (addrCheckpoint == 0L) {
            
            throw new IllegalStateException();
            
        }
        
        return addrCheckpoint;
        
    }

    /**
     * Address that can be used to read the {@link IndexMetadata} record for
     * the index from the store.
     */
    final public long getMetadataAddr() {
        
        return addrMetadata;
        
    }
    
    /**
     * Address of the root node or leaf of the {@link BTree}.
     * 
     * @return The address of the root -or- <code>0L</code> iff the btree
     *         does not have a root.
     */
    final public long getRootAddr() {
        
        return addrRoot;
        
    }
    
    /**
     * Address of the {@link IBloomFilter}.
     * 
     * @return The address of the bloom filter -or- <code>0L</code> iff the
     *         btree does not have a bloom filter.
     */
    final public long getBloomFilterAddr() {
        
        return addrBloomFilter;
        
    }
    
    /**
     * The height of the tree - ZERO(0) means just a root leaf. Values
     * greater than zero give the #of levels of abstract nodes. There is
     * always one layer of leaves which is not included in this value.
     */
    public final int getHeight() {

        return height;
        
    }

    /**
     * The #of non-leaf nodes.
     */
    public final int getNodeCount() {
        
        return nnodes;
        
    }

    /**
     * The #of leaves.
     */
    public final int getLeafCount() {
        
        return nleaves;
        
    }

    /**
     * The #of index entries.
     */
    public final int getEntryCount() {
        
        return nentries;
        
    }

    /**
     * Return the value of the counter stored in the {@link Checkpoint}
     * record.
     */
    public final long getCounter() {
        
        return counter;
        
    }

    /**
     * A human readable representation of the state of the {@link Checkpoint}
     * record.
     */
    public final String toString() {

        return "Checkpoint" + //
                "{height=" + height + //
                ",nnodes=" + nnodes + //
                ",nleaves=" + nleaves + //
                ",nentries=" + nentries + //
                ",counter=" + counter + //
                ",addrRoot=" + addrRoot + //
                ",addrMetadata=" + addrMetadata + //
                ",addrBloomFilter=" + addrBloomFilter + //
                ",addrCheckpoint=" + addrCheckpoint + //
                "}";
        
    }
    
    /**
     * De-serialization ctor.
     */
    public Checkpoint() {
        
    }
    
    /**
     * Create the first checkpoint record for a new {@link BTree} from a
     * {@link IndexMetadata} record. The root of the {@link BTree} will NOT
     * exist (its address will be <code>0L</code>). Once written on the store
     * the {@link Checkpoint} record may be used to obtain a corresponding
     * instance of a {@link BTree} object.
     * 
     * @param metadata
     *            The index metadata record.
     */
    public Checkpoint(final IndexMetadata metadata ) {

        this( //
                metadata.getMetadataAddr(), //
                0L,// No root yet.
                0L,// No bloom filter yet.
                0, // height 
                0, // nnodes
                0, // nleaves
                0, // nentries
                0L // counter
        );
        
    }

    /**
     * Create the first checkpoint record for an existing {@link BTree} when it
     * is propagated on overflow onto a new backing {@link IRawStore}. The
     * {@link #counter} is propagated to the new {@link Checkpoint} but
     * otherwise the initialization is as if for an empty {@link BTree}.
     * 
     * @param metadata
     *            The index metadata record.
     * @param oldCheckpoint
     *            The last {@link Checkpoint} for the index on the old backing
     *            store. The {@link Checkpoint#counter} is propagated to the new
     *            {@link Checkpoint} record.
     */
    public Checkpoint(final IndexMetadata metadata, final Checkpoint oldCheckpoint ) {

        this( //
                metadata.getMetadataAddr(), //
                0L,// No root yet.
                0L,// No bloom filter yet.
                0, // height 
                0, // nnodes
                0, // nleaves
                0, // nentries
                oldCheckpoint.counter
        );
        
    }

    /**
     * Creates a {@link Checkpoint} record from a {@link BTree}.
     * <p>
     * Pre-conditions:
     * <ul>
     * <li>The root is clean.</li>
     * <li>The metadata record is clean.</li>
     * <li>The optional bloom filter is clean if it is defined.</li>
     * </ul>
     * Note: if the root is <code>null</code> then the root is assumed to be
     * clean and the root address from the last {@link Checkpoint} record is
     * used. Otherwise the address of the root is used (in which case it MUST be
     * defined).
     * 
     * @param btree
     *            The btree.
     */
    public Checkpoint(final BTree btree) {
        
        this(btree.metadata.getMetadataAddr(),//
                /*
                 * root node or leaf.
                 * 
                 * Note: if the [root] reference is not defined then we use the
                 * address in the last checkpoint record. if that is 0L then
                 * there is no root and a new root leaf will be created on
                 * demand.
                 */
                (btree.root == null ? btree.getCheckpoint().getRootAddr()
                        : btree.root.getIdentity()),//
                /*
                 * optional bloom filter.
                 * 
                 * Note: if the [bloomFilter] reference is not defined then we
                 * use the address in the last checkpoint record. if that is 0L
                 * then there is no bloom filter. If the [bloomFilter] reference
                 * is defined but the bloom filter has been disabled, then we
                 * also write a 0L so that the bloom filter is no longer
                 * reachable from the new checkpoint.
                 */
                (btree.bloomFilter == null ? btree.getCheckpoint()
                        .getBloomFilterAddr()
                        : btree.bloomFilter.isEnabled() ? btree.bloomFilter
                                .getAddr() : 0L),//
                btree.height,//
                btree.nnodes,//
                btree.nleaves,//
                btree.nentries,//
                btree.counter.get()//
                );
           
    }

    private Checkpoint(final long addrMetadata, final long addrRoot,
            final long addrBloomFilter, final int height, final int nnodes,
            final int nleaves, final int nentries, final long counter) {

        /*
         * Note: The constraint on [addrMetadata] is relaxed in order to permit
         * a transient BTree (no backing store).
         */
//        assert addrMetadata != 0L;
        // MUST be valid addr.
        this.addrMetadata = addrMetadata;

        // MAY be 0L (tree initially has no root)
        this.addrRoot = addrRoot;

        /*
         * MAY be 0L (bloom filter is optional and an new bloom filter is clear,
         * so it will not be written out until something is written on the
         * index).
         */
        this.addrBloomFilter = addrBloomFilter;
        
        this.height = height;

        this.nnodes = nnodes;

        this.nleaves = nleaves;

        this.nentries = nentries;

        this.counter = counter;
        
    }

    /**
     * Initial serialization version.
     * <p>
     * Note: The fields of the {@link Checkpoint} record use fixed length
     * representations in order to support the possibility that we might do an
     * in place update of a {@link Checkpoint} record as part of a data
     * migration strategy. For the same reason, the {@link Checkpoint} record
     * includes some unused fields. Those fields are available for future
     * version changes without requiring us to change the length of the
     * {@link Checkpoint} record.
     */
    private static transient final int VERSION0 = 0x0;
    
    /**
     * The current version.
     */
    private static transient final int VERSION = VERSION0;

    /**
     * Write the {@link Checkpoint} record on the store, setting
     * {@link #addrCheckpoint} as a side effect.
     * 
     * @param store
     * 
     * @throws IllegalStateException
     *             if the {@link Checkpoint} record has already been
     *             written.
     */
    final public void write(final IRawStore store) {

        if (addrCheckpoint != 0L) {
            
            throw new IllegalStateException();
            
        }
        
        final byte[] data = SerializerUtil.serialize(this);

        addrCheckpoint = store.write(ByteBuffer.wrap(data));
        
    }

    /**
     * Read a {@link Checkpoint} record from a store.
     * 
     * @param store
     *            The store.
     * @param addrCheckpoint
     *            The address from which to read the {@link Checkpoint}
     *            record. This address is set on the {@link Checkpoint}
     *            record as a side-effect.
     */
    public static Checkpoint load(final IRawStore store, final long addrCheckpoint) {

        if (store == null)
            throw new IllegalArgumentException();
        
        final ByteBuffer buf = store.read(addrCheckpoint);

        final Checkpoint checkpoint = (Checkpoint) SerializerUtil.deserialize(buf);
        
        checkpoint.addrCheckpoint = addrCheckpoint;
        
        return checkpoint;
        
    }

    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        
        final int version = in.readInt();

        if (version != VERSION0)
            throw new IOException("Unknown version: " + version);

        this.addrMetadata = in.readLong();

        this.addrRoot = in.readLong();

        this.addrBloomFilter = in.readLong();
        
        this.height = in.readInt();

        this.nnodes = in.readInt();

        this.nleaves = in.readInt();

        this.nentries = in.readInt();

        this.counter = in.readLong();

        in.readLong(); // unused.
        
        in.readLong(); // unused.
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.writeInt(VERSION);

        out.writeLong(addrMetadata);

        out.writeLong(addrRoot);
        
        out.writeLong(addrBloomFilter);

        out.writeInt(height);

        out.writeInt(nnodes);

        out.writeInt(nleaves);

        out.writeInt(nentries);

        out.writeLong(counter);

        out.writeLong(0L/*unused*/);
        
        out.writeLong(0L/*unused*/);
        
    }

}
