package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryContext;
import com.bigdata.bop.fed.FederatedRunningQuery;
import com.bigdata.bop.solutions.SolutionSetStream;
import com.bigdata.btree.Checkpoint;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;
import com.bigdata.relation.accesspath.ThickCloseableIterator;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.stream.Stream.StreamIndexMetadata;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A chunk of intermediate results stored on the native heap which are ready to
 * be consumed by some {@link BOp} in a specific query.
 * <p>
 * Note: This class is only used in query evaluation for the standalone
 * database.
 * 
 * @see BLZG-533 Vector query engine on native heap.
 */
public class LocalNativeChunkMessage implements IChunkMessage<IBindingSet> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** The query controller. */
    private final IQueryClient queryController;

    /** The service {@link UUID} for the {@link IQueryClient query controller}. */
    private final UUID queryControllerId;
    
    /**
     * The query identifier.
     */
    private final UUID queryId;
    
    /**
     * The target {@link BOp}.
     */
    private final int bopId;

    /**
     * The index partition which is being targeted for that {@link BOp}.
     */
    private final int partitionId;

    /**
     * The solutions (or null if the chunk was empty).
     */
    private final SolutionSetStream ssstr; 
    
    /**
     * The memory manager on which the solutions were stored (this is an
     * allocation context and can be destroyed without destroying the parent
     * memory manager).
     */
    private final MemStore mmgr;

    /**
     * The #of bytes of data written onto the {@link SolutionSetStream} for
     * the {@link IChunkMessage}.
     */
    private final long byteCount;
    
    /**
     * false unless the chunk has been released.
     */
    private boolean released;
    
    @Override
    public IQueryClient getQueryController() {
        return queryController;
    }

    @Override
    public UUID getQueryControllerId() {
        return queryControllerId;
    }
    
    @Override
    public UUID getQueryId() {
        return queryId;
    }

    @Override
    public int getBOpId() {
        return bopId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean isLastInvocation() {
        return false; // Never.
    }

    @Override
    public boolean isMaterialized() {
        return true;
    }

    @Override
    public int getSolutionCount() {

        final SolutionSetStream ssstr = this.ssstr;
        
        if (ssstr == null)
            return 0;
        
        final long nsolutions = ssstr.getStats().getSolutionSetSize();

        if (nsolutions > Integer.MAX_VALUE)
            throw new UnsupportedOperationException(
                    "Too many solutions for interface to report correctly: solutionCount=" + nsolutions);

        return (int) nsolutions;

    }

    /**
     * Return the byte count of the solutions on the native heap for this chunk
     * message.
     */
    public long getByteCount() {
        
        return byteCount;
        
    }

    public LocalNativeChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int bopId, final int partitionId,
            final IQueryContext queryContext,
            final IBindingSet[] bsets) {

        this(queryController, queryId, bopId, partitionId,
                queryContext,
                new IBindingSet[][] { bsets });

    }
    
    public LocalNativeChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int bopId, final int partitionId,
            final IQueryContext queryContext,
            final IBindingSet[][] bindingSetChunks) {

        if (queryController == null)
            throw new IllegalArgumentException();
        
        if (queryId == null)
            throw new IllegalArgumentException();

        if (queryContext == null)
            throw new IllegalArgumentException();

        if (bindingSetChunks == null)
            throw new IllegalArgumentException();
        
        this.queryController = queryController;
        try {
            this.queryControllerId = queryController.getServiceUUID();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
        
        this.queryId = queryId;
        
        this.bopId = bopId;
        
        this.partitionId = partitionId;
        
        this.released = false;
        
        if (bindingSetChunks.length == 0 || (bindingSetChunks.length == 1 && bindingSetChunks[0].length == 0)) {

            /*
             * Empty chunk.
             */
            
            this.mmgr = null;
            this.ssstr = null;
            this.byteCount = 0L;
            
        } else {
            
            /*
             * Write the chunk onto a SolutionSetStream.
             */
            
            final StreamIndexMetadata metadata = new StreamIndexMetadata(UUID.randomUUID()); 
            
            final Checkpoint checkpoint = new Checkpoint(metadata);

            final MemStore store = new MemStore(queryContext.getMemoryManager().createAllocationContext());

            final SolutionSetStream ssstr = new SolutionSetStream(store, checkpoint, metadata, false /* readOnly */);

            // play the result into a native memory backed solution set stream
            final ThickCloseableIterator<IBindingSet[]> itr = new ThickCloseableIterator<IBindingSet[]>(
                    bindingSetChunks);
            try {
                ssstr.put(itr);
            } finally {
                itr.close();
            }

            this.ssstr = ssstr;
            this.mmgr = store;
            
//            // Note: This reports the size in bytes on sector boundaries (1M blocks).
//            this.byteCount = store.size();
            
            // This reports the actual size of the allocation for the PSOutputStream.
            this.byteCount = store.getByteCount(ssstr.getRootAddr());

        }
        
        
    }

    @Override
    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId + ",partitionId=" + partitionId
                + ", ssstr=" + (ssstr == null ? "null" : ssstr.toString() + ":" + ssstr.getStats().toString()) + "}";

    }

    @Override
    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }

    @Override
    public void release() {
        if (!released) {
            if (mmgr != null) {
                mmgr.destroy();
            }
            final ChunkAccessor tmp = chunkAccessor;
            if (tmp != null) {
                // Close the iterator.
                tmp.close();
            }
            released = true;
        }
    }
    
    @Override
    public IChunkAccessor<IBindingSet> getChunkAccessor() {
        if (chunkAccessor == null) {
            chunkAccessor = new ChunkAccessor();
        }
        return chunkAccessor;
    }
    
    private volatile transient ChunkAccessor chunkAccessor = null;
    
    private class ChunkAccessor implements IChunkAccessor<IBindingSet> {

        private final ICloseableIterator<IBindingSet[]> source;

        public ChunkAccessor() {
            if (released)
                throw new IllegalStateException();
            if (ssstr == null) {
                source = new EmptyCloseableIterator<IBindingSet[]>();
            } else {
                source = ssstr.get();
            }
        }

        @Override
        public ICloseableIterator<IBindingSet[]> iterator() {
            return source;
        }

        public void close() {
            source.close();
            if (ssstr != null) {
                ssstr.close();
            }
        }

    }

}
