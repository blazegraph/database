/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.nio.ByteBuffer;

import com.bigdata.bop.solutions.SolutionSetStream;
import com.bigdata.htree.HTree;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.stream.Stream;
import com.bigdata.stream.Stream.StreamIndexMetadata;

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
 */
public class Checkpoint implements ICheckpoint, Externalizable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -4308251060627051232L;

    // transient and set on write or read-back.
    transient long addrCheckpoint;

    // persistent and immutable.
    private long addrMetadata;
    private long addrRoot; // of root node/leaf for BTree; rootDir for HTree.
    private int height; // height for BTree; globalDepth for HTree.
    private long nnodes; // #of directories for HTree
    private long nleaves; // #of buckets for HTree.
    private long nentries; // #of tuples in the index.
    private long counter; // B+Tree local counter.

    private long addrBloomFilter;

    private long recordVersion; // #of node or leaf records written to date.

	/**
	 * Added in {@link #VERSION1}. This is a short field allowing for 65536
	 * different possible index types.
	 */
    private IndexTypeEnum indexType;

    @Override
    final public long getCheckpointAddr() {
        
        if (addrCheckpoint == 0L) {
            
            throw new IllegalStateException();
            
        }
        
        return addrCheckpoint;
        
    }

    @Override
    final public boolean hasCheckpointAddr() {
        
        return addrCheckpoint != 0L;
        
    }

    @Override
    final public long getMetadataAddr() {
        
        return addrMetadata;
        
    }
    
    @Override
    final public long getRootAddr() {
        
        return addrRoot;
        
    }
    
    @Override
    final public long getBloomFilterAddr() {
        
        return addrBloomFilter;
        
    }

   /**
    * {@inheritDoc}
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1229" > DumpJournal fails on
    *      non-BTree classes </a>
    */
    @Override
    public final int getHeight() {

//		switch (indexType) {
//		case BTree:
//			return height;
//		default:
//			throw new UnsupportedOperationException();
//		}
       return height;
        
    }

    /**
     * {@inheritDoc}
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1229" > DumpJournal fails on
     *      non-BTree classes </a>
     */
    @Override
    public final int getGlobalDepth() {
    	
//		switch (indexType) {
//		case HTree:
//			return height;
//		default:
//			throw new UnsupportedOperationException();
//		}
       return height;

    }

    @Override
    public final long getNodeCount() {
        
        return nnodes;
        
    }

    @Override
    public final long getLeafCount() {
        
        return nleaves;
        
    }

    @Override
    public final long getEntryCount() {
        
        return nentries;
        
    }

    @Override
    public final long getCounter() {
        
        return counter;
        
    }

    @Override
    public final long getRecordVersion() {
        
        return counter;
        
    }
    
    @Override
    public final IndexTypeEnum getIndexType() {
        
        return indexType;
        
    }

    /**
     * A human readable representation of the state of the {@link Checkpoint}
     * record.
     */
    @Override
    public final String toString() {

        return "Checkpoint" + //
        		"{indexType=" + indexType + //
				(indexType == IndexTypeEnum.BTree ? ",height=" + height
						: (indexType == IndexTypeEnum.HTree ? ",globalDepth="
								+ height : "")) +
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
                0L, // nnodes
                0L, // nleaves
                0L, // nentries
                0L, // counter
                0L, // recordVersion
                metadata.getIndexType() // indexType
                
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
                0L, // nnodes
                0L, // nleaves
                0L, // nentries
                oldCheckpoint.counter,//
                0L, // recordVersion 
                metadata.getIndexType()//
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
     * <p>
     * Note: <strong>This method is invoked by reflection.</strong>
     * 
     * @param btree
     *            The btree.
     */
    public Checkpoint(final BTree btree) {
        
        this(btree.getMetadataAddr(),//
                /*
                 * root node or leaf.
                 * 
                 * Note: if the [root] reference is not defined then we use the
                 * address in the last checkpoint record. if that is 0L then
                 * there is no root and a new root leaf will be created on
                 * demand.
                 */
        		btree.getRootAddr(),//
//                (btree.root == null ? btree.getCheckpoint().getRootAddr()
//                        : btree.root.getIdentity()),//
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
				/*
				 * Note: This MUST access the raw counter in scale-out or the
				 * logic in PartitionedCounter.wrap(long) will observe the
				 * partitionId in the high word and throw an exception. The
				 * whole point of that check in PartitionedCounter.wrap(long) is
				 * to verify that the underlying index local counter has not
				 * overflowed, which is what we are saving out here.
				 */
                btree.counter.get(),//
                btree.getRecordVersion(),//
                IndexTypeEnum.BTree // IndexTypeEnum
                );
           
    }

    /**
     * Creates a {@link Checkpoint} record from an {@link HTree}.
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
     * <p>
     * Note: <strong>This method is invoked by reflection.</strong>
     * 
     * @param htree
     *            The {@link HTree}.
     */
    public Checkpoint(final HTree htree) {
        
        this(htree.getMetadataAddr(),//
                /*
                 * root node or leaf.
                 * 
                 * Note: if the [root] reference is not defined then we use the
                 * address in the last checkpoint record. if that is 0L then
                 * there is no root and a new root leaf will be created on
                 * demand.
                 */
        		htree.getRootAddr(),//
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
//                (htree.bloomFilter == null ? htree.getCheckpoint()
//                        .getBloomFilterAddr()
//                        : htree.bloomFilter.isEnabled() ? htree.bloomFilter
//                                .getAddr() : 0L),//
                0L, // TODO No bloom filter yet. Do we want to support this?
                0, // htree.height,// Note: HTree is not balanced (height not uniform)
                htree.getNodeCount(),//
                htree.getLeafCount(),//
                htree.getEntryCount(),//
                htree.getCounter().get(),//
                htree.getRecordVersion(),//
                IndexTypeEnum.HTree // IndexTypeEnum
                );
           
    }

    /**
     * Creates a {@link Checkpoint} record from an {@link HTree}.
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
     * <p>
     * Note: <strong>This method is invoked by reflection.</strong>
     * 
     * @param stream
     *            The {@link HTree}.
     */
    public Checkpoint(final Stream stream) {
        
        this(stream.getMetadataAddr(),//
                /*
                 * root node or leaf.
                 * 
                 * Note: if the [root] reference is not defined then we use the
                 * address in the last checkpoint record. if that is 0L then
                 * there is no root and a new root leaf will be created on
                 * demand.
                 */
                stream.getRootAddr(),//
                /*
                 * optional bloom filter.
                 * 
                 * Note: if the [bloomFilter] reference is not defined then we
                 * use the address in the last checkpoint record. if that is 0L
                 * then there is no bloom filter. If the [bloomFilter] reference
                 * is defined but the bloom filter has been disabled, then we
                 * also write a 0L so that the bloom filter is no longer
                 * reachable from the new checkpoint.
                 * 
                 * FIXME GIST : The SolutionSetStats are hacked into the
                 * bloom filter addr for the Stream.
                 */
                ((SolutionSetStream)stream).getStatsAddr(),//
                // 
                0, // htree.height,// Note: HTree is not balanced (height not uniform)
                0L,//stream.getNodeCount(),//
                0L,//stream.getLeafCount(),//
                stream.rangeCount(),//
                0L,//stream.getCounter().get(),//
                stream.getRecordVersion(),//
                IndexTypeEnum.Stream // IndexTypeEnum
                );
           
    }

	private Checkpoint(final long addrMetadata, final long addrRoot,
			final long addrBloomFilter, final int height, final long nnodes,
			final long nleaves, final long nentries, final long counter,
			final long recordVersion,
			final IndexTypeEnum indexType) {

		assert indexType != null;
		
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

		this.recordVersion = recordVersion;

        this.indexType = indexType;
        
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
	 * Adds the {@link #indexType} field and the {@link #globalDepth} field,
	 * which is present only for {@link IndexTypeEnum#HTree}.
	 */
    private static transient final int VERSION1 = 0x1;

	/**
	 * Adds and/or modifies the following fields.
	 * <dl>
	 * <dt>nodeCount</dt>
	 * <dd>Changed from int32 to int64.</dd>
	 * <dt>leafCount</dt>
	 * <dd>Changed from int32 to int64.</dd>
	 * <dt>entryCount</dt>
	 * <dd>Changed from int32 to int64.</dd>
	 * <dt>recordVersion</dt>
	 * <dd>Added a new field which record the <em>next</em> record version
	 * identifier to be used when the next node or leaf data record is written
	 * onto the backing store. The field provides sequential numbering of those
	 * data record which can facilitate certain kinds of forensics. For example,
	 * all updated records falling between two checkpoints may be identified by
	 * a file scan filtering for the index UUID and a record version number GT
	 * the last record version written for the first checkpoint and LTE the last
	 * record version number written for the second checkpoint. The initial
	 * value is ZERO (0).</dd>
	 * </dl>
	 * In addition, the <strong>size</strong> of the checkpoint record has been
	 * increased in order to provide room for future expansions.
	 */
    private static transient final int VERSION2 = 0x2;
    
    /**
     * The current version.
     */
    private static transient final int currentVersion = VERSION2;

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

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        
        final int version = in.readInt();

		switch (version) {
		case VERSION0:
		case VERSION1:
		case VERSION2:
			break;
		default:
			throw new IOException("Unknown version: " + version);
		}

		this.addrMetadata = in.readLong();

		this.addrRoot = in.readLong();

		this.addrBloomFilter = in.readLong();

		this.height = in.readInt();

		if (version <= VERSION1) {

			this.nnodes = in.readInt();

			this.nleaves = in.readInt();

			this.nentries = in.readInt();

			this.recordVersion = 0L;
			
		} else {

			this.nnodes = in.readLong();

			this.nleaves = in.readLong();

			this.nentries = in.readLong();

			this.recordVersion = in.readLong();

		}

        this.counter = in.readLong();

		switch (version) {
		case VERSION0:
			in.readLong(); // unused
			indexType = IndexTypeEnum.BTree;
			break;
		case VERSION1:
		case VERSION2:
			this.indexType = IndexTypeEnum.valueOf(in.readShort());
			in.readShort();// ignored.
			in.readInt();// ignored.
			break;
		default:
			throw new AssertionError();
        }
        
        in.readLong(); // unused.

		if (version >= VERSION2) {

			// Read some additional padding added to the record in VERSION2.
			for (int i = 0; i < 10; i++) {

				in.readLong();
				
			}

        }
        
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {

        out.writeInt(currentVersion);

        out.writeLong(addrMetadata);

        out.writeLong(addrRoot);
        
        out.writeLong(addrBloomFilter);

        out.writeInt(height);

		if (currentVersion <= VERSION1) {

			if (nnodes > Integer.MAX_VALUE)
				throw new RuntimeException();
			if (nleaves > Integer.MAX_VALUE)
				throw new RuntimeException();
			if (nentries > Integer.MAX_VALUE)
				throw new RuntimeException();
			
			out.writeInt((int)nnodes);

			out.writeInt((int)nleaves);

			out.writeInt((int)nentries);

		} else {

			out.writeLong(nnodes);

			out.writeLong(nleaves);

			out.writeLong(nentries);

			out.writeLong(recordVersion);

		}

        out.writeLong(counter);

        /*
         * 8 bytes follow. 
         */
        
        out.writeShort(indexType.getCode());
		out.writeShort(0/* unused */);
		out.writeInt(0/* unused */);

		/*
		 * 8 bytes follow.
		 */

		out.writeLong(0L/* unused */);

		/*
		 * Additional space added in VERSION2.
		 */
		for (int i = 0; i < 10; i++) {

			out.writeLong(0L/* unused */);

		}
		
	}

    /**
     * Utility method reads the {@link Checkpoint} record and then loads and
     * returns a view of the associated read-only persistence capable data
     * structure.
     * <p>
     * <strong>Note: The caller is responsible for marking the returned object
     * as read-only or not depending on the context. This method should be used
     * from trusted code such as {@link Name2Addr} and {@link AbstractJournal}
     * which can make this decision.</strong>
     * 
     * @param store
     *            The backing store.
     * @param checkpointAddr
     *            The address of the checkpoint record.
     * @param readOnly
     *            <code>true</code> if the object will be read-only.
     * 
     * @return The persistence capable data structure loaded from that
     *         checkpoint.
     */
    public static ICheckpointProtocol loadFromCheckpoint(final IRawStore store,
            final long checkpointAddr, final boolean readOnly) {

        /*
         * Read checkpoint record from store.
         */
        final Checkpoint checkpoint;
        try {
            checkpoint = Checkpoint.load(store, checkpointAddr);
        } catch (Throwable t) {
            throw new RuntimeException("Could not load Checkpoint: store="
                    + store + ", addrCheckpoint="
                    + store.toString(checkpointAddr), t);
        }

        // re-load from the store.
        final ICheckpointProtocol ndx;
        switch (checkpoint.getIndexType()) {
        case BTree:
            ndx = BTree.load(store, checkpointAddr, readOnly);
            break;
        case HTree:
            ndx = HTree.load(store, checkpointAddr, readOnly);
            break;
        case Stream:
            ndx = Stream.load(store, checkpointAddr, readOnly);
            break;
        default:
            throw new AssertionError("Unknown: " + checkpoint.getIndexType());
        }

        // // set the lastCommitTime on the index.
        // btree.setLastCommitTime(lastCommitTime);

        return ndx;

    }

    /**
     * Generic method to create a persistence capable data structure (GIST
     * compatible, core implementation).
     * 
     * @param store
     *            The backing store.
     * @param metadata
     *            The metadata that describes the data structure to be created.
     * 
     * @return The persistence capable data structure.
     */
    public static ICheckpointProtocol create(final IRawStore store,
            final IndexMetadata metadata) {

        final ICheckpointProtocol ndx;
        switch (metadata.getIndexType()) {
        case BTree:
            ndx = BTree.create(store, metadata);
            break;
        case HTree:
            ndx = HTree.create(store, (HTreeIndexMetadata) metadata);
            break;
        case Stream:
            /*
             * FIXME GIST : This is not setting the SolutionSetStream class
             * since Stream.create() is being invoked rather than
             * SolutionSetStream.create()
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/585 (GIST)
             */
            ndx = Stream.create(store, (StreamIndexMetadata) metadata);
            break;
        default:
            throw new AssertionError("Unknown: " + metadata.getIndexType());
        }

        return ndx;

    }
    
}
