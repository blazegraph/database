/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
/*
 * Created on Apr 9, 2012
 */

package com.bigdata.rdf.sparql.ast.cache;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.Checkpoint;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.encoder.IVSolutionSetDecoder;
import com.bigdata.rdf.internal.encoder.IVSolutionSetEncoder;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.SolutionSetStatserator;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rwstore.IPSOutputStream;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Metadata for a solution set declaration (sort of like a checkpoint record).
 * 
 * TODO Work out the relationship to {@link Checkpoint} records. Everything
 * about this solution set which is updatable should be organized into a
 * checkpoint. The metadata declaration for the solution set should also be
 * organized into the checkpoint, perhaps as an ISPO[], perhaps using a rigid
 * schema. The size of the solution set should be visible when its metadata is
 * looked at as a graph.
 */
public final class SolutionSetMetadata { //implements ICheckpointProtocol {

    private static final Logger log = Logger
            .getLogger(SolutionSetMetadata.class);

    /**
     * The index is already closed.
     */
    protected static final String ERROR_CLOSED = "Closed";

    // /**
    // * A parameter was less than zero.
    // */
    // protected static final String ERROR_LESS_THAN_ZERO = "Less than zero";
    //
    // /**
    // * A parameter was too large.
    // */
    // protected static final String ERROR_TOO_LARGE = "Too large";
    //
    /**
     * The index is read-only but a mutation operation was requested.
     */
    protected static final String ERROR_READ_ONLY = "Read-only";

    /**
     * The name of the solution set.
     */
    public final String name;

    /**
     * The metadata describing the solution set.
     */
    private final ISPO[] metadata;
    
    /**
     * The #of solutions in this solution set.
     * 
     * TODO This is redundant with
     * {@link ISolutionSetStats#getSolutionSetSize()}
     */
    private long solutionCount;
    
    /**
     * The {@link ISolutionSetStats} are collected when the solution are
     * written by {@link #put(ICloseableIterator)}.
     */
    private ISolutionSetStats solutionSetStats;
    
    /**
     * The address from which the solution set may be read.
     */
    private long solutionSetAddr;

    /**
     * <code>true</code> iff the view is read-only.
     */
    private final boolean readOnly;
    
    /**
     * The backing store.
     */
    private final IRWStrategy store;

    /**
     * 
     * @param name
     *            The name of the solution set.
     * @param store
     *            The {@link IMemoryManager} on which the solution set will be
     *            written and from which it will be read.
     * @param metadata
     *            The metadata describing the solution set.
     */
    public SolutionSetMetadata(final String name,
            final IRWStrategy store, final ISPO[] metadata, final boolean readOnly) {

        if (name == null)
            throw new IllegalArgumentException();

        if (store == null)
            throw new IllegalArgumentException();

        this.name = name;

        this.store = store;

        this.metadata = metadata;
        
        this.readOnly = readOnly;

    }

    public void clear() {

        assertNotReadOnly();

        if (solutionSetAddr != IRawStore.NULL) {
        
            recycle(solutionSetAddr);
//            store.clear();

            solutionSetAddr = IRawStore.NULL;
            
            solutionSetStats = null;
            
            fireDirtyEvent();
            
        }

    }

    /**
	 * Return the {@link ISolutionSetStats} for the saved solution set.
	 * 
	 * @return The {@link ISolutionSetStats}.
	 */
    public ISolutionSetStats getStats() {
    	
    		return solutionSetStats;
    	
    }
    
    public ICloseableIterator<IBindingSet[]> get() {

		final long solutionSetAddr = this.solutionSetAddr;

		if (solutionSetAddr == IRawStore.NULL)
			throw new IllegalStateException();

		try {

			return new SolutionSetStreamDecoder(solutionSetAddr, solutionCount);

		} catch (IOException e) {

			throw new RuntimeException(e);

		}

    }

    /**
     * The per-chunk header is currently two int32 fields. The first field is
     * the #of solutions in that chunk. The second field is the #of bytes to
     * follow in the payload for that chunk.
     * 
     * TODO Should there be an overall header for the solution set or are we
     * going to handle that through the solution set metadata?
     */
    static private int CHUNK_HEADER_SIZE = Bytes.SIZEOF_INT + Bytes.SIZEOF_INT;

    /**
     * Stream decoder for solution sets.
     */
    private class SolutionSetStreamDecoder implements
            ICloseableIterator<IBindingSet[]> {

        private final long solutionSetCount;
        private final DataInputStream in;
        private final IVSolutionSetDecoder decoder;

        private boolean open = false;
        
        /** The next chunk of solutions to be visited. */
        private IBindingSet[] bsets = null;
        
        /** The #of solution sets which have been decoded so far. */
        private long nsolutions = 0;

        public SolutionSetStreamDecoder(final long solutionSetAddr,
                final long solutionSetCount) throws IOException {

            this.solutionSetCount = solutionSetCount;

            this.in = new DataInputStream(
                    wrapInputStream(store
                            .getInputStream(solutionSetAddr)));

            this.open = true;

            this.decoder = new IVSolutionSetDecoder();

        }

        @Override
        public void close() {
            if(open) {
                open = false;
                try {
                    in.close();
                } catch (IOException e) {
                    // Unexpected exception.
                    log.error(e, e);
                }
            }
        }

        @Override
        public boolean hasNext() {

            if (open && bsets == null) {
            
                /*
                 * Read ahead and extract a chunk of solutions.
                 */

                try {

                    if ((bsets = decodeNextChunk()) == null) {

                        // Nothing more to be read.
                        close();

                    }

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

            }

            return open && bsets != null;

        }

        /**
         * Read ahead and decode the next chunk of solutions.
         * 
         * @return The decoded solutions.
         */
        private IBindingSet[] decodeNextChunk() throws IOException {

            if (nsolutions == solutionSetCount) {

                // Nothing more to be read.

				if (log.isDebugEnabled())
					log.debug("Read solutionSet: name=" + name
							+ ", solutionSetSize=" + nsolutions);

                return null;

            }
            
            // #of solutions in this chunk.
            final int chunkSize = in.readInt();

            // #of bytes in this chunk.
            final int byteLength = in.readInt();

            // Read in all the bytes in the next chunk.
            final byte[] a = new byte[byteLength];

            // read data
            in.readFully(a);

            // Wrap byte[] as stream.
            final DataInputBuffer buf = new DataInputBuffer(a);

            // Allocate array for the decoded solutions.
            final IBindingSet[] t = new IBindingSet[chunkSize];

            // Decode the solutions into the array.
            for (int i = 0; i < chunkSize; i++) {

                t[i] = decoder
                        .decodeSolution(buf, true/* resolveCachedValues */);

				if (log.isTraceEnabled())
					log.trace("Read: name=" + name + ", solution=" + t[i]);

            }

            // Update the #of solution sets which have been decoded.
            nsolutions += chunkSize;

            if (log.isTraceEnabled())
				log.trace("Read chunk: name=" + name + ", chunkSize="
						+ chunkSize + ", bytesRead="
						+ (CHUNK_HEADER_SIZE + byteLength)
						+ ", solutionSetSize=" + nsolutions);

            // Return the decoded solutions.
            return t;

        }

        @Override
        public IBindingSet[] next() {
            
            if (!hasNext())
                throw new NoSuchElementException();
            
            final IBindingSet[] t = bsets;
            
            bsets = null;
            
            return t;
        }

        @Override
        public void remove() {
           
            throw new UnsupportedOperationException();
            
        }

    }

    public void put(final ICloseableIterator<IBindingSet[]> src2) {

        if (src2 == null)
            throw new IllegalArgumentException();

        assertNotReadOnly();

        // wrap with class to compute statistics over the observed solutions.
        final SolutionSetStatserator src = new SolutionSetStatserator(src2);
        
        final IVSolutionSetEncoder encoder = new IVSolutionSetEncoder();

        // Stream writing onto the backing store.
        final IPSOutputStream out = store.getOutputStream();
        // Address from which the solutions may be read.
        final long newAddr;
        // #of solutions written.
        long nsolutions = 0;
        // #of bytes for the encoded solutions (before compression).
        long nbytes = 0;
        // The #of chunks written.
        long chunkCount = 0;
        try {

            final DataOutputBuffer buf = new DataOutputBuffer();
            
            final DataOutputStream os = new DataOutputStream(
                    wrapOutputStream(out));

            try {

                while (src.hasNext()) {

                    // Discard the data in the buffer.
                    buf.reset();

                    // Chunk of solutions to be written.
                    final IBindingSet[] chunk = src.next();

                    // Write solutions.
                    for (int i = 0; i < chunk.length; i++) {

                        encoder.encodeSolution(buf, chunk[i]);

						if (log.isTraceEnabled())
							log.trace("Wrote name=" + name + ", solution="
									+ chunk[i]);

                    }

                    // #of bytes written onto the buffer.
                    final int bytesBuffered = buf.limit();

                    // Write header (#of solutions in this chunk).
                    os.writeInt(chunk.length);

                    // Write header (#of bytes buffered).
                    os.writeInt(bytesBuffered);

                    // transfer buffer data to output stream.
                    os.write(buf.array(), 0/* off */, bytesBuffered);

                    // += headerSize + bytesBuffered.
                    nbytes += CHUNK_HEADER_SIZE + bytesBuffered;

                    nsolutions += chunk.length;

                    chunkCount++;

                    if (log.isDebugEnabled())
						log.debug("Wrote chunk: name=" + name + ", chunkSize="
								+ chunk.length + ", chunkCount=" + chunkCount
								+ ", bytesBuffered=" + bytesBuffered
								+ ", solutionSetSize=" + nsolutions);

                }

                os.flush();
            
            } finally {
                
                os.close();
                
            }

            out.flush();

            newAddr = out.getAddr();

            if (log.isDebugEnabled())
				log.debug("Wrote solutionSet: name=" + name
						+ ", solutionSetSize=" + nsolutions + ", chunkCount="
						+ chunkCount + ", encodedBytes=" + nbytes
					  //	+ ", bytesWritten=" + out.getBytesWritten()
					  );

        } catch (IOException e) {

            throw new RuntimeException(e);

        } finally {

            try {
                out.close();
            } catch (IOException e) {
                // Unexpected exception.
                log.error(e, e);
            }

        }

        if (solutionSetAddr != IRawStore.NULL) {

            /*
             * Release the old solution set.
             */

            recycle(solutionSetAddr);

        }

        // TODO This is not atomic (needs lock).
        solutionSetAddr = newAddr;
        solutionCount = nsolutions;
        solutionSetStats = src.getStats();
        fireDirtyEvent();

    }

    /**
	 * TODO Test performance with and without gzip. Extract into the CREATE
	 * schema so we can do this declaratively.
	 */
    private static final boolean zip = true;
    
    private OutputStream wrapOutputStream(final OutputStream out)
            throws IOException {

        if (zip)
            return new DeflaterOutputStream(out);

        return out;

    }

    private InputStream wrapInputStream(final InputStream in)
            throws IOException {

        if (zip)
            return new InflaterInputStream(in);

        return in;

    }

    /*
     * ICheckpointProtocol
     * 
     * FIXME Finish the ICheckpointProtocol implementation.
     */
    
    /**
     * Return <code>true</code> iff this B+Tree is read-only.
     */
    final public boolean isReadOnly() {

        return readOnly;

    }

    /**
     * 
     * @throws UnsupportedOperationException
     *             if the B+Tree is read-only.
     * 
     * @see #isReadOnly()
     */
    final protected void assertNotReadOnly() {

        if (isReadOnly()) {

            throw new UnsupportedOperationException(ERROR_READ_ONLY);

        }

    }

    /**
     * NOP. Implemented for compatibility with the interior APIs of the HTree
     * and BTree.
     */
    final protected void assertNotTransient() {

        // NOP

    }
    
//    // FIXME Report ISolutionSetStats and the name of the solution set.
//    @Override
//    public CounterSet getCounters() {
//        
//        final CounterSet counterSet = new CounterSet();
//        {
//            
//            counterSet.addCounter("index UUID", new OneShotInstrument<String>(
//                    getIndexMetadata().getIndexUUID().toString()));
//
//            counterSet.addCounter("class", new OneShotInstrument<String>(
//                    getClass().getName()));
//
//        }
//
////        /*
////         * Note: These statistics are reported using a snapshot mechanism which
////         * prevents a hard reference to the AbstractBTree from being attached to
////         * the CounterSet object!
////         */
////        {
////
////            final CounterSet tmp = counterSet
////                    .makePath(IBTreeCounters.WriteRetentionQueue);
////
////            tmp.addCounter("Capacity", new OneShotInstrument<Integer>(
////                    writeRetentionQueue.capacity()));
////
////            tmp.addCounter("Size", new OneShotInstrument<Integer>(
////                    writeRetentionQueue.size()));
////
////            tmp.addCounter("Distinct", new OneShotInstrument<Integer>(
////                    ndistinctOnWriteRetentionQueue));
////
////        }
////        
////        /*
////         * Note: These statistics are reported using a snapshot mechanism which
////         * prevents a hard reference to the AbstractBTree from being attached to
////         * the CounterSet object!
////         */
////        {
////
////            final CounterSet tmp = counterSet
////                    .makePath(IBTreeCounters.Statistics);
////
////            tmp.addCounter("addressBits", new OneShotInstrument<Integer>(
////                    addressBits));
////
//////          tmp.addCounter("height",
//////                  new OneShotInstrument<Integer>(getHeight()));
////
////            tmp.addCounter("nodeCount", new OneShotInstrument<Long>(
////                    getNodeCount()));
////
////            tmp.addCounter("leafCount", new OneShotInstrument<Long>(
////                    getLeafCount()));
////
////            tmp.addCounter("tupleCount", new OneShotInstrument<Long>(
////                    getEntryCount()));
////
//////          /*
//////           * Note: The utilization numbers reported here are a bit misleading.
//////           * They only consider the #of index positions in the node or leaf
//////           * which is full, but do not take into account the manner in which
//////           * the persistence store allocates space to the node or leaf. For
//////           * example, for the WORM we do perfect allocations but retain many
//////           * versions. For the RWStore, we do best-fit allocations but recycle
//////           * old versions. The space efficiency of the persistence store is
//////           * typically the main driver, not the utilization rate as reported
//////           * here.
//////           */
//////          final IBTreeUtilizationReport r = getUtilization();
//////          
//////          // % utilization in [0:100] for nodes
//////          tmp.addCounter("%nodeUtilization", new OneShotInstrument<Integer>(r
//////                  .getNodeUtilization()));
//////          
//////          // % utilization in [0:100] for leaves
//////          tmp.addCounter("%leafUtilization", new OneShotInstrument<Integer>(r
//////                  .getLeafUtilization()));
//////
//////          // % utilization in [0:100] for the whole tree (nodes + leaves).
//////          tmp.addCounter("%totalUtilization", new OneShotInstrument<Integer>(r
//////                  .getTotalUtilization())); // / 100d
////
////            /*
////             * Compute the average bytes per tuple. This requires access to the
////             * current entry count, so we have to do this as a OneShot counter
////             * to avoid dragging in the B+Tree reference.
////             */
////
////            final long entryCount = getEntryCount();
////
////            final long bytes = btreeCounters.bytesOnStore_nodesAndLeaves.get()
////                    + btreeCounters.bytesOnStore_rawRecords.get();
////
////            final long bytesPerTuple = (long) (entryCount == 0 ? 0d
////                    : (bytes / entryCount));
////
////            tmp.addCounter("bytesPerTuple", new OneShotInstrument<Long>(
////                    bytesPerTuple));
////
////        }
////
////        /*
////         * Attach detailed performance counters.
////         * 
////         * Note: The BTreeCounters object does not have a reference to the
////         * AbstractBTree. Its counters will update "live" since we do not
////         * need to snapshot them.
////         */
////        counterSet.attach(btreeCounters.getCounters());
//
//        return counterSet;
//
//    }
//
//    @Override
//    public long handleCommit(final long commitTime) {
//
//        return writeCheckpoint2().getCheckpointAddr();
//        
//    }
//
//    @Override
//    public long getRecordVersion() {
//        return recordVersion;
//    }
//    /**
//     * The value of the record version number that will be assigned to the next
//     * node or leaf written onto the backing store. This number is incremented
//     * each time a node or leaf is written onto the backing store. The initial
//     * value is ZERO (0). The first value assigned to a node or leaf will be
//     * ZERO (0).
//     */
//    private long recordVersion;
//
//    /**
//     * The constructor sets this field initially based on a {@link Checkpoint}
//     * record containing the only address of the {@link IndexMetadata} for the
//     * index. Thereafter this reference is maintained as the {@link Checkpoint}
//     * record last written by {@link #writeCheckpoint()} or read by
//     * {@link #load(IRawStore, long)}.
//     */
//    private Checkpoint checkpoint = null;    
//
//    @Override
//    final public Checkpoint getCheckpoint() {
//
//        if (checkpoint == null)
//            throw new AssertionError();
//        
//        return checkpoint;
//        
//    }
//    
//    @Override
//    final public long writeCheckpoint() {
//        
//        // write checkpoint and return address of that checkpoint record.
//        return writeCheckpoint2().getCheckpointAddr();
//        
//    }
//
//    /**
//     * {@inheritDoc}
//     * 
//     * @see #load(IRawStore, long)
//     */
//    final public Checkpoint writeCheckpoint2() {
//        
//        assertNotTransient();
//        assertNotReadOnly();
//
//        /*
//         * Note: Acquiring this lock provides for atomicity of the checkpoint of
//         * the BTree during the commit protocol. Without this lock, users of the
//         * UnisolatedReadWriteIndex could be concurrently modifying the BTree
//         * while we are attempting to snapshot it for the commit.
//         * 
//         * Note: An alternative design would declare a global read/write lock
//         * for mutation of the indices in addition to the per-BTree read/write
//         * lock provided by UnisolatedReadWriteIndex. Rather than taking the
//         * per-BTree write lock here, we would take the global write lock in the
//         * AbstractJournal's commit protocol, e.g., commitNow(). The global read
//         * lock would be taken by UnisolatedReadWriteIndex before taking the
//         * per-BTree write lock. This is effectively a hierarchical locking
//         * scheme and could provide a workaround if deadlocks are found to occur
//         * due to lock ordering problems with the acquisition of the
//         * UnisolatedReadWriteIndex lock (the absence of lock ordering problems
//         * really hinges around UnisolatedReadWriteLocks not being taken for
//         * more than one index at a time.)
//         * 
//         * @see https://sourceforge.net/apps/trac/bigdata/ticket/278
//         * @see https://sourceforge.net/apps/trac/bigdata/ticket/284
//         * @see https://sourceforge.net/apps/trac/bigdata/ticket/288
//         * @see https://sourceforge.net/apps/trac/bigdata/ticket/343
//         * @see https://sourceforge.net/apps/trac/bigdata/ticket/440
//         */
//        final Lock lock = UnisolatedReadWriteIndex.getReadWriteLock(this).writeLock();
////      final Lock lock = new UnisolatedReadWriteIndex(this).writeLock();
//        lock.lock();
//        try {
//
//            if (/* autoCommit && */needsCheckpoint()) {
//
//                /*
//                 * Flush the btree, write a checkpoint record, and return the
//                 * address of that checkpoint record. The [checkpoint] reference
//                 * is also updated.
//                 */
//
//                return _writeCheckpoint2();
//
//            }
//
//            /*
//             * There have not been any writes on this btree or auto-commit is
//             * disabled.
//             * 
//             * Note: if the application has explicitly invoked writeCheckpoint()
//             * then the returned address will be the address of that checkpoint
//             * record and the BTree will have a new checkpoint address made
//             * restart safe on the backing store.
//             */
//
//            return checkpoint;
//
//        } finally {
//
//            lock.unlock();
//
//        }
//
//    }
//    
//    /**
//     * Return true iff changes would be lost unless the B+Tree is flushed to the
//     * backing store using {@link #writeCheckpoint()}.
//     * <p>
//     * Note: In order to avoid needless checkpoints this method will return
//     * <code>false</code> if:
//     * <ul>
//     * <li> the metadata record is persistent -AND-
//     * <ul>
//     * <li> EITHER the root is <code>null</code>, indicating that the index
//     * is closed (in which case there are no buffered writes);</li>
//     * <li> OR the root of the btree is NOT dirty, the persistent address of the
//     * root of the btree agrees with {@link Checkpoint#getRootAddr()}, and the
//     * {@link #counter} value agrees {@link Checkpoint#getCounter()}.</li>
//     * </ul>
//     * </li>
//     * </ul>
//     * 
//     * @return <code>true</code> true iff changes would be lost unless the
//     *         B+Tree was flushed to the backing store using
//     *         {@link #writeCheckpoint()}.
//     */
//    public boolean needsCheckpoint() {
//
//        if(checkpoint.getCheckpointAddr() == 0L) {
//            
//            /*
//             * The checkpoint record needs to be written.
//             */
//            
//            return true;
//            
//        }
//        
//        if(metadata.getMetadataAddr() == 0L) {
//            
//            /*
//             * The index metadata record was replaced and has not yet been
//             * written onto the store.
//             */
//            
//            return true;
//            
//        }
//        
//        if(metadata.getMetadataAddr() != checkpoint.getMetadataAddr()) {
//            
//            /*
//             * The index metadata record was replaced and has been written on
//             * the store but the checkpoint record does not reflect the new
//             * address yet.
//             */
//            
//            return true;
//            
//        }
//        
//        if(checkpoint.getCounter() != counter.get()) {
//            
//            // The counter has been modified.
//            
//            return true;
//            
//        }
//        
//        if(root != null ) {
//            
//            if (root.isDirty()) {
//
//                // The root node is dirty.
//
//                return true;
//
//            }
//            
//            if(checkpoint.getRootAddr() != root.identity) {
//        
//                // The root node has a different persistent identity.
//                
//                return true;
//                
//            }
//            
//        }
//
//        /*
//         * No apparent change in persistent state so we do NOT need to do a
//         * checkpoint.
//         */
//        
//        return false;
//        
////        if (metadata.getMetadataAddr() != 0L && //
////                (root == null || //
////                        ( !root.dirty //
////                        && checkpoint.getRootAddr() == root.identity //
////                        && checkpoint.getCounter() == counter.get())
////                )
////        ) {
////            
////            return false;
////            
////        }
////
////        return true;
//     
//    }
//    
//    /**
//     * Core implementation invoked by {@link #writeCheckpoint2()} while holding
//     * the lock - <strong>DO NOT INVOKE THIS METHOD DIRECTLY</strong>.
//     * 
//     * @return the checkpoint.
//     */
//    final private Checkpoint _writeCheckpoint2() {
//        
//        assertNotTransient();
//        assertNotReadOnly();
//        
////        assert root != null : "root is null"; // i.e., isOpen().
//
//        // flush any dirty nodes.
//        flush();
//        
//        // pre-condition: all nodes in the tree are clean.
//        assert root == null || !root.isDirty();
//
////        { // TODO support bloom filter?
////            /*
////             * Note: Use the [AbstractBtree#bloomFilter] reference here!!!
////             * 
////             * If that reference is [null] then the bloom filter is either
////             * clean, disabled, or was not configured. For any of those (3)
////             * conditions we will use the address of the bloom filter from the
////             * last checkpoint record. If the bloom filter is clean, then we
////             * will just carry forward its old address. Otherwise the address in
////             * the last checkpoint record will be 0L and that will be carried
////             * forward.
////             */
////            final BloomFilter filter = this.bloomFilter;
////
////            if (filter != null && filter.isDirty() && filter.isEnabled()) {
////
////                /*
////                 * The bloom filter is enabled, is loaded and is dirty, so write
////                 * it on the store now.
////                 */
////
////                filter.write(store);
////
////            }
////            
////        }
//        
//        if (metadata.getMetadataAddr() == 0L) {
//            
//            /*
//             * Is there an old metadata addr in need of recyling?
//             */
//            if (checkpoint != null) {
//                final long addr = checkpoint.getMetadataAddr();
//                if (addr != IRawStore.NULL)
//                    store.delete(addr);
//            }
//            
//            /*
//             * The index metadata has been modified so we write out a new
//             * metadata record on the store.
//             */
//            
//            metadata.write(store);
//            
//         }
//        
//        if (checkpoint != null && getRoot() != null
//                && checkpoint.getRootAddr() != getRoot().getIdentity()) {
//            recycle(checkpoint.getRootAddr());
//        }
//
//        if (checkpoint != null) {
//            recycle(checkpoint.getCheckpointAddr());
//        }
//        
//        // create new checkpoint record.
//        checkpoint = metadata.newCheckpoint(this);
//        
//        // write it on the store.
//        checkpoint.write(store);
//        
//        if (BigdataStatics.debug||log.isInfoEnabled()) {
//            final String msg = "name=" + metadata.getName()
//                    + ", writeQueue{size=" + writeRetentionQueue.size()
//                    + ",distinct=" + ndistinctOnWriteRetentionQueue + "} : "
//                    + checkpoint;
//            if (BigdataStatics.debug)
//                System.err.println(msg);
//            if (log.isInfoEnabled())
//                log.info(msg);
//        }
//        
//        // return the checkpoint record.
//        return checkpoint;
//        
//    }

    /**
     * Recycle (aka delete) the allocation. This method also adjusts the #of
     * bytes released in the {@link BTreeCounters}.
     * 
     * @param addr
     *            The address to be recycled.
     * 
     * @return The #of bytes which were recycled and ZERO (0) if the address is
     *         {@link IRawStore#NULL}.
     */
    protected int recycle(final long addr) {

         if (addr == IRawStore.NULL)
         return 0;
        
         final int nbytes = store.getByteCount(addr);
        
         // getBtreeCounters().bytesReleased += nbytes;
        
         store.delete(addr);
        
         return nbytes;

    }
//
//    @Override
//    final public long getMetadataAddr() {
//
//        return metadata.getMetadataAddr();
//
//    }
//        
//    @Override
//    final public long getRootAddr() {
//        
//        return (root == null ? getCheckpoint().getRootAddr() : root
//                .getIdentity());
//        
//    }
//
//  @Override
//  public IndexMetadata getIndexMetadata() {
//
//      if (isReadOnly()) {
//
//          if (metadata2 == null) {
//
//              synchronized (this) {
//
//                  if (metadata2 == null) {
//
//                      metadata2 = metadata.clone();
//
//                  }
//
//              }
//
//          }
//
//          return metadata2;
//          
//      }
//      
//      return metadata;
//      
//    }
//    private volatile IndexMetadata metadata2;
//
//    /**
//     * The metadata record for the index. This data rarely changes during the
//     * life of the solution set, but it CAN be changed.
//     */
//    protected IndexMetadata metadata;
//
//    final public IDirtyListener getDirtyListener() {
//        
//        return listener;
//        
//    }
//
//    final public long getLastCommitTime() {
//        
//        return lastCommitTime;
//        
//    }
//
//    final public void setLastCommitTime(final long lastCommitTime) {
//        
//        if (lastCommitTime == 0L)
//            throw new IllegalArgumentException();
//        
//        if (this.lastCommitTime == lastCommitTime) {
//
//            // No change.
//            
//            return;
//            
//        }
//        
//        if (log.isInfoEnabled())
//            log.info("old=" + this.lastCommitTime + ", new=" + lastCommitTime);
//        // Note: Commented out to allow replay of historical transactions.
//        /*if (this.lastCommitTime != 0L && this.lastCommitTime > lastCommitTime) {
//
//            throw new IllegalStateException("Updated lastCommitTime: old="
//                    + this.lastCommitTime + ", new=" + lastCommitTime);
//            
//        }*/
//
//        this.lastCommitTime = lastCommitTime;
//        
//    }
//
//    /**
//     * The lastCommitTime of the {@link Checkpoint} record from which the
//     * {@link BTree} was loaded.
//     * <p>
//     * Note: Made volatile on 8/2/2010 since it is not otherwise obvious what
//     * would guarantee visibility of this field, through I do seem to remember
//     * that visibility might be guaranteed by how the BTree class is discovered
//     * and returned to the class. Still, it does no harm to make this a volatile
//     * read.
//     */
//    volatile private long lastCommitTime = 0L;// Until the first commit.
//    
//    final public void setDirtyListener(final IDirtyListener listener) {
//
//        assertNotReadOnly();
//        
//        this.listener = listener;
//        
//    }
//    
//    private IDirtyListener listener;
//
//    /**
//     * Fire an event to the listener (iff set).
//     */
//    final protected void fireDirtyEvent() {
//
//        assertNotReadOnly();
//
//        final IDirtyListener l = this.listener;
//
//        if (l == null)
//            return;
//
//        if (Thread.interrupted()) {
//
//            throw new RuntimeException(new InterruptedException());
//
//        }
//
//        l.dirtyEvent(this);
//        
//    }
//
//    @Override
//    public long rangeCount() {
//        return getStats().getSolutionSetSize();
//    }
//
//    @Override
//    public ICloseableIterator<IBindingSet[]> rangeIterator() {
//        return get();
//    }
//
//    @Override
//    public void removeAll() {
//        clear();
//    }

    /*
     * FIXME This needs to be replaced by the proper method in the comment block
     * above.
     */
    final protected void fireDirtyEvent() {
        // NOP
    }
    
}
