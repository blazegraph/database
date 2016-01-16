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
/*
 * Created on Apr 9, 2012
 */

package com.bigdata.bop.solutions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.btree.BaseIndexStats;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.internal.encoder.SolutionSetStreamDecoder;
import com.bigdata.rdf.internal.encoder.SolutionSetStreamEncoder;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.stream.Stream;
import com.bigdata.striterator.Chunkerator;

import cutthecrap.utils.striterators.ArrayIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * A persistence capable solution set stored using a stream oriented API. The
 * order of the solutions on playback is their write order. This data structure
 * provides fast read/write performance, but does not provide key-based access
 * into the solution sets.
 * 
 * TODO Test performance with and without gzip. Extract into the CREATE schema /
 * IndexMetadata so we can do this declaratively.
 */
public final class SolutionSetStream extends Stream implements
        ISolutionSet {

    private static final Logger log = Logger.getLogger(SolutionSetStream.class);

    /**
     * Encapsulates the address and the data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private final class MySolutionSetStats implements ISolutionSetStats {
        
        private ISolutionSetStats delegate;
        private long addr;

        public MySolutionSetStats(final ISolutionSetStats stats) {

            this.delegate = stats;
            
        }
        
        @Override
        public long getSolutionSetSize() {
            return delegate.getSolutionSetSize();
        }

        @Override
        public Set<IVariable<?>> getUsedVars() {
            return delegate.getUsedVars();
        }

        @Override
        public Set<IVariable<?>> getAlwaysBound() {
            return delegate.getAlwaysBound();
        }

        @Override
        public Set<IVariable<?>> getNotAlwaysBound() {
            return delegate.getNotAlwaysBound();
        }

        @Override
        public Set<IVariable<?>> getMaterialized() {
            return delegate.getMaterialized();
        }

        @Override
        public Map<IVariable<?>, IConstant<?>> getConstants() {
            return delegate.getConstants();
        }
        
    }
    
    /**
     * The {@link ISolutionSetStats} are collected when the solution are written
     * by {@link #put(ICloseableIterator)}.
     * 
     * @see #needsCheckpoint()
     * @see #flush()
     * 
     *      FIXME GIST : This is hacked into the bloomFilterAddr. It should have
     *      its own address. The Checkpoint class needs a protocol for
     *      populating and reporting fields which are specific to derived
     *      classes, not just BTree, HTree, and Stream. Or we need to add a
     *      general concept of a "summary statistics object" for a persistent
     *      data structure.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
     *      </a>
     */
    private MySolutionSetStats solutionSetStats;
    
    /**
     * Required constructor. This constructor is used both to create a new named
     * solution set, and to load an existing named solution set from the store
     * using a {@link Checkpoint} record.
     * 
     * @param store
     *            The store.
     * @param checkpoint
     *            The {@link Checkpoint} record.
     * @param metadata
     *            The metadata record.
     * @param readOnly
     *            When <code>true</code> the view will be immutable.
     * 
     * @see #create(IRawStore, StreamIndexMetadata)
     * @see #load(IRawStore, long, boolean)
     */
    public SolutionSetStream(final IRawStore store,
            final Checkpoint checkpoint, final IndexMetadata metadata,
            final boolean readOnly) {

        super(store, checkpoint, metadata, readOnly);
        
        /*
         * Note: The SolutionSetStats will be loaded by setCheckpoint().
         */

    }

    /**
     * Create a stream for an ordered solution set.
     * <p>
     * {@inheritDoc}
     * 
     * FIXME GIST : This is not setting the SolutionSetStream class when invoked
     * by {@link Checkpoint#create(IRawStore, IndexMetadata)} since
     * Stream.create() is being invoked rather than SolutionSetStream.create().
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585" > GIST </a>
     */
    public static SolutionSetStream create(final IRawStore store,
            final StreamIndexMetadata metadata) {

        /*
         * Must override the implementation class name:
         */
        
        metadata.setStreamClassName(SolutionSetStream.class.getName());

        return Stream.create(store, metadata);

    }

    /**
	 * Return the {@link ISolutionSetStats} for the saved solution set.
	 * 
	 * @return The {@link ISolutionSetStats}.
	 */
    @Override
    public ISolutionSetStats getStats() {

        /*
         * Note: This field is set by setCheckpoint().
         */
        
    		return solutionSetStats;
    	
    }

    /**
     * Return the address of the {@link ISolutionSetStats} to be written into the
     * next {@link Checkpoint} record. The caller must have {@link #flush()} the
     * {@link SolutionSetStream} as a pre-condition (to ensure that the stats
     * have been written out). If the {@link ISolutionSetStats} are not loaded,
     * then the address from the last {@link Checkpoint} record is returned.
     */
    public long getStatsAddr() {

        if (solutionSetStats != null) {

            return solutionSetStats.addr;

        }

        return getCheckpoint().getBloomFilterAddr();

    }

    @Override
    public ICloseableIterator<IBindingSet[]> get() {

        if (rootAddr == IRawStore.NULL)
            throw new IllegalStateException();

        // Open input stream reading decompressed data from store.
        final DataInputStream in = new DataInputStream(
                wrapInputStream(getStore().getInputStream(rootAddr)));

        // Wrap with iterator pattern that will decode solutions.
        final SolutionSetStreamDecoder decoder = new SolutionSetStreamDecoder(
                metadata.getName(), in, rangeCount());
        
        // Return the iterator to the caller.
        return decoder;

    }

    @Override
    public void put(final ICloseableIterator<IBindingSet[]> src2) {

        if (src2 == null)
            throw new IllegalArgumentException();

        assertNotReadOnly();

        final String name = metadata.getName();

        // Address from which the solutions may be read.
        final long newAddr;
        
        // Used to encode the solutions on the stream.
        final SolutionSetStreamEncoder encoder = new SolutionSetStreamEncoder(
                name);

        // Stream writes onto the backing store.
        final IPSOutputStream out = getStore().getOutputStream();

        try {

            // Wrap with data output stream and compression.
            final DataOutputStream os = new DataOutputStream(
                    wrapOutputStream(out));
            
            try {
                
                // Encode the solutions onto the stream.
                encoder.encode(os, src2);

                // Flush the stream.
                os.flush();

            } finally {

                try {
                    os.close();
                } catch (IOException e) {
                    // Unexpected exception.
                    log.error(e, e);
                }

            }
            
            // Flush
            out.flush();

            // Note address of the stream.
            newAddr = out.getAddr();

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

        if (rootAddr != IRawStore.NULL) {

            /*
             * Release the old solution set.
             */

            recycle(rootAddr);

        }

        if (solutionSetStats != null && solutionSetStats.addr != IRawStore.NULL) {

            recycle(solutionSetStats.addr);

        }

        rootAddr = newAddr;
        entryCount = encoder.getSolutionCount();
        solutionSetStats = new MySolutionSetStats(encoder.getStats());
        fireDirtyEvent();

    }

    /*
     * ICheckpointProtocol
     */
    
    @SuppressWarnings("unchecked")
    @Override
    public ICloseableIterator<IBindingSet> scan() {
        
        return (ICloseableIterator<IBindingSet>) new Striterator(get())
                .addFilter(new Expander() {

                    /**
                     * 
                     */
                    private static final long serialVersionUID = 1L;

                    @SuppressWarnings("rawtypes")
                    @Override
                    protected Iterator expand(final Object obj) {

                        return new ArrayIterator((IBindingSet[]) obj);

                    }
                });

    }

    @Override
    public void clear() {
        
        super.clear();
        
        solutionSetStats = null;
        
    }

    @Override
    protected boolean needsCheckpoint() {

        if (super.needsCheckpoint())
            return true;

        if (solutionSetStats != null
                && solutionSetStats.addr != getCheckpoint()
                        .getBloomFilterAddr()) {

            // The statistics field was updated.
            return true;
            
        }
        
        if (solutionSetStats == null
                && getCheckpoint().getBloomFilterAddr() != IRawStore.NULL) {

            // The statistics field was cleared.
            return true;
            
        }
        
        return false;
        
    }
    
    @Override
    protected void flush() {
        
        super.flush();

        /*
         * If the solutionSetStats are dirty, then write them out and set the
         * addr so it will be propagated to the checkpoint record.
         */

        if (solutionSetStats != null
                && (solutionSetStats.addr == IRawStore.NULL //
                 || solutionSetStats.addr != getCheckpoint().getBloomFilterAddr())//
                 ) {

            solutionSetStats.addr = getStore()
                    .write(ByteBuffer.wrap(SerializerUtil
                            .serialize(solutionSetStats.delegate)));

        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to persist the {@link ISolutionSetStats}.
     */
    @Override
    protected void setCheckpoint(final Checkpoint checkpoint) {

        super.setCheckpoint(checkpoint);

        {
 
            final long addr = checkpoint.getBloomFilterAddr();
            
            if(addr != IRawStore.NULL) {

                this.solutionSetStats = new MySolutionSetStats(
                        (ISolutionSetStats) SerializerUtil
                                .deserialize(getStore().read(addr)));
                
                this.solutionSetStats.addr = addr;
                
            }

        }
        
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void write(final ICloseableIterator<?> src) {
        
        try {

            /*
             * Chunk up the solutions and write them onto the stream.
             */

            put(new Chunkerator(src));
            
        } finally {
            
            src.close();
            
        }

    }

    @Override
    public BaseIndexStats dumpPages(final boolean recursive,
         final boolean visitLeaves) {

       return new BaseIndexStats(this);
       
    }

    /*
     * I've commented out the AccessPath and Predicate abstractions for now.
     * They were not required to implement the SPARQL CACHE mechanism, but
     * this code might be useful in the future.
     */
    
//    /**
//     * Return an access path that can be used to scan the solutions.
//     * 
//     * @param pred
//     *            ignored.
//     * 
//     * @return The access path.
//     * 
//     *         TODO FILTERS: (A) Any filter attached to the predicate must be
//     *         applied by the AP to remove solutions which do not satisfy the
//     *         filter;
//     *         <p>
//     *         (B) When the AP has a filter, then an exact range count must scan
//     *         the solutions to decide how many will match.
//     */
//    public SolutionSetAP getAccessPath(final IPredicate<IBindingSet> pred) {
//
//        return new SolutionSetAP(this, pred);
//
//    }
//
//    /**
//     * Class provides basic access path suitable for full scans.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     */
//    public static class SolutionSetAP implements
//            IBindingSetAccessPath<IBindingSet> {
//
//        private final SolutionSetStream stream;
//        private final IPredicate<IBindingSet> predicate;
//
//        public SolutionSetAP(final SolutionSetStream stream,
//                final IPredicate<IBindingSet> pred) {
//
//            this.stream = stream;
//
//            this.predicate = pred;
//
//        }
//
//        public SolutionSetStream getStream() {
//            
//            return stream;
//            
//        }
//        
//        @Override
//        public IPredicate<IBindingSet> getPredicate() {
//
//            return predicate;
//
//        }
//
//        @Override
//        public boolean isEmpty() {
//
//            /*
//             * Note: If we develop streams which can have non-exact range counts
//             * (due to deleted tuples) then this would have to be revisited.
//             */
//
//            return stream.entryCount == 0;
//
//        }
//
//        @Override
//        public long rangeCount(boolean exact) {
//
//            return stream.entryCount;
//
//        }
//
//        @Override
//        public ICloseableIterator<IBindingSet> solutions(final long limit,
//                final BaseJoinStats stats) {
//
//            if (limit != 0L && limit != Long.MAX_VALUE)
//                throw new UnsupportedOperationException();
//
//            final IChunkedIterator<IBindingSet> itr = new ChunkConsumerIterator<IBindingSet>(
//                    stream.get());
//
//            return BOpContext.solutions(itr, predicate, stats);
//
//        }
//
//        @Override
//        public long removeAll() {
//            
//            final long n = stream.rangeCount();
//            
//            stream.removeAll();
//            
//            return n;
//            
//        }
//
//    }
//
//    /**
//     * A predicate that can be used with an {@link ISolutionSet} without having
//     * to resolve the {@link ISolutionSet} as an {@link IRelation}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     * @param <E>
//     */
//    public static class SolutionSetStreamPredicate<E extends IBindingSet> extends
//            Predicate<E> {
//
//        /**
//         * 
//         */
//        private static final long serialVersionUID = 1L;
//
//        public SolutionSetStreamPredicate(BOp[] args, Map<String, Object> annotations) {
//            super(args, annotations);
//        }
//
//        public SolutionSetStreamPredicate(BOp[] args, NV... annotations) {
//            super(args, annotations);
//        }
//
//        /** Deep copy constructor. */
//        public SolutionSetStreamPredicate(final SolutionSetStreamPredicate<E> op) {
//            super(op);
//        }
//
//        /**
//         * 
//         * @param attributeName
//         *            The name of the query attribute that will be used to
//         *            resolve this solution set.
//         * @param timestamp
//         *            The timestamp associated with the view.
//         */
//        public SolutionSetStreamPredicate(/*IVariableOrConstant<?>[] values,*/
//                final String attributeName,/* int partitionId, boolean optional, */
//                /*
//                 * IElementFilter<E> constraint, IAccessPathExpander<E>
//                 * expander,
//                 */
//                final long timestamp) {
//
//            super(EMPTY, attributeName/* relationName */, -1/* partitionId */,
//                    false/* optional */, null/* constraint */,
//                    null/* expander */, timestamp);
//
//        }
//
//    }
//
//    private static transient final IVariableOrConstant<?>[] EMPTY = new IVariableOrConstant[0];
    
}
