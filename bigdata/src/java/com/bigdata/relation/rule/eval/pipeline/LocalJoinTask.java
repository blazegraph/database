package com.bigdata.relation.rule.eval.pipeline;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.journal.Journal;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.service.LocalDataServiceFederation;

/**
 * {@link JoinTask} implementation for a {@link Journal} or
 * {@link LocalDataServiceFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalJoinTask extends JoinTask {
    
    /**
     * The asynchronous iterator that is the source for this
     * {@link LocalJoinTask}.
     */
    final private IAsynchronousIterator<IBindingSet[]> source;

    /**
     * <code>null</code> unless this is the last join dimension.
     */
    final private IBuffer<ISolution[]> solutionBuffer;
    
    /**
     * @param concurrencyManager
     * @param indexName
     * @param rule
     * @param joinNexusFactory
     * @param order
     * @param orderIndex
     * @param masterProxy
     * @param masterUUID
     * @param source
     * @param solutionBuffer
     */
    public LocalJoinTask(
            final String indexName, final IRule rule,
            final IJoinNexus joinNexus, final int[] order,
            final int orderIndex, 
            final IJoinMaster masterProxy,
            final UUID masterUUID,
            final IAsynchronousIterator<IBindingSet[]> source,
            final IBuffer<ISolution[]> solutionBuffer) {

        super(indexName, rule, joinNexus, order, orderIndex,
                -1/* partitionId */, masterProxy, masterUUID);

        if (source == null)
            throw new IllegalArgumentException();
        
        if (lastJoin && solutionBuffer == null)
            throw new IllegalArgumentException();

        this.source = source;
        
        /*
         * The fanIn is always one.
         * 
         * Note: There is one source for the first LocalJoinTask. It is the
         * async iterator containing the initial bindingSet from the
         * LocalJoinMaster.
         */
        stats.fanIn = 1;
        
        if (lastJoin) {

            /*
             * Accepted binding sets are flushed to the solution buffer.
             */

            // not used
            this.syncBuffer = null;
            
            // solutionBuffer.
            this.solutionBuffer = solutionBuffer;
            
        } else {

            /*
             * The index is not key-range partitioned. This means that there
             * is ONE (1) JoinTask per predicate in the rule. Chunks of
             * bindingSets are written pre-Thread buffers by ChunkTasks.
             * Those unsynchronized buffers overflow onto the per-JoinTask
             * [syncBuffer], which is a BlockingBuffer. The sink
             * LocalJoinTask drains that BlockingBuffer using its
             * iterator(). When the BlockingBuffer is closed and everything
             * in the buffer has been drained, then the sink LocalJoinTask
             * will conclude that no more bindingSets are available and it
             * will terminate.
             */

            this.syncBuffer = new BlockingBuffer<IBindingSet[]>(joinNexus
                    .getChunkOfChunksCapacity());
            
            // not used.
            this.solutionBuffer = null;

            stats.fanOut = 1;
            
        }

    }

    @Override
    protected IBuffer<ISolution[]> getSolutionBuffer() {
        
        if (!lastJoin)
            throw new IllegalStateException();
        
        return solutionBuffer;
        
    }

    /**
     * Closes the {@link #source} specified to the ctor.
     */
    protected void closeSources() {
        
        if(INFO)
            log.info(toString());
        
        source.close();
        
    }
    
    /**
     * Note: The target buffer on which the unsynchronized buffer writes
     * depends on whether or not there is a downstream sink for this
     * {@link LocalJoinTask}. When this is the {@link JoinTask#lastJoin},
     * the unsynchronized buffer returned by this method will write on the
     * solution buffer. Otherwise it will write on {@link #syncBuffer},
     * which is drained by the sink {@link LocalJoinTask}.
     */
    final protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer() {

        if (lastJoin) {

            /*
             * Accepted binding sets are flushed to the solution buffer.
             */

            // flushes to the solution buffer.
            return new UnsynchronizedSolutionBuffer<IBindingSet>(
                    this, joinNexus, joinNexus.getChunkCapacity());
            
        } else {

            /*
             * The index is not key-range partitioned. This means that there
             * is ONE (1) JoinTask per predicate in the rule. The
             * bindingSets are aggregated into chunks by this buffer. On
             * overflow, the buffer writes onto a BlockingBuffer. The sink
             * JoinTask reads from that BlockingBuffer's iterator.
             */

            // flushes to the syncBuffer.
            return new UnsyncLocalOutputBuffer<IBindingSet>(
                    this, joinNexus.getChunkCapacity(), syncBuffer);

        }

    }

    /**
     * The {@link BlockingBuffer} whose queue will be drained by the
     * downstream {@link LocalJoinTask} -or- <code>null</code> IFF
     * [lastJoin == true].
     */
    protected final BlockingBuffer<IBindingSet[]> syncBuffer;

    /**
     * The {@link Future} for the sink for this {@link LocalJoinTask} and
     * <code>null</code> iff this is {@link JoinTask#lastJoin}. This
     * field is set by the {@link LocalJoinMasterTask} so it can be
     * <code>null</code> if things error out before it gets set or 
     * perhaps if they complete too quickly.
     */
    protected Future<? extends Object> sinkFuture;
    
    @Override
    protected void flushAndCloseBuffersAndAwaitSinks()
            throws InterruptedException, ExecutionException {

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId);
        
        if (halt)
            throw new RuntimeException(firstCause.get());

        if(lastJoin) {

            /*
             * Flush the solutionBuffer.
             * 
             * Note: For the last JOIN, the buffer is either the query
             * solution buffer or the mutation buffer.
             * 
             * DO NOT close() the solutionBuffer for the last join since
             * (for query) the buffer is shared by all rules in the program.
             * 
             * Closing the solutionBuffer on the last join is BAD BAD BAD.
             */

            final long counter = getSolutionBuffer().flush();

            if (joinNexus.getAction().isMutation()) {

                /*
                 * For mutation operations, the solutionBuffer for the last
                 * join dimension writes solutions onto the target relation.
                 * When that buffer is flushed it returns the #of solutions
                 * that resulted in a state change in the target relation.
                 * This is the mutationCount. We report it here to the
                 * JoinStats and it will be aggregated by the
                 * JoinMasterTask.
                 */

                stats.mutationCount.addAndGet(counter);
                
            }
            
        } else {

            /*
             * Close the thread-safe output buffer. For any JOIN except the
             * last, this buffer will be the source for one or more sink
             * JoinTasks for the next join dimension. Once this buffer is
             * closed, the asynchronous iterator draining the buffer will
             * eventually report that there is nothing left for it to
             * process.
             * 
             * Note: This is a BlockingBuffer. BlockingBuffer#flush() is a
             * NOP.
             */

            syncBuffer.close();
            
            assert !syncBuffer.isOpen();

            if (halt)
                throw new RuntimeException(firstCause.get());

            if (sinkFuture == null) {

                // @todo should we wait for the Future to be assigned?
                log.warn("sinkFuture not assigned yet: orderIndex="
                        + orderIndex);
                
            } else {
                
                try {

                    sinkFuture.get();

                } catch (Throwable t) {

                    halt(t);

                }
            
            }
            
        }
        
    }

    @Override
    protected void cancelSinks() {

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId );

        if (!lastJoin) {

            syncBuffer.reset();

            if (sinkFuture != null) {

                sinkFuture.cancel(true/* mayInterruptIfRunning */);

            }

        }

    }
    
    /**
     * Return the next chunk of {@link IBindingSet}s the source
     * {@link JoinTask}.
     * 
     * @return The next chunk -or- <code>null</code> iff the source is
     *         exhausted.
     */
    protected IBindingSet[] nextChunk() throws InterruptedException {

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex);

        while (!source.isExhausted()) {

            if (halt)
                throw new RuntimeException(firstCause.get());

            // note: uses timeout to avoid blocking w/o testing [halt].
            if (source.hasNext(10, TimeUnit.MILLISECONDS)) {

                // read the chunk.
                final IBindingSet[] chunk = source.next();

                stats.bindingSetChunksIn++;
                stats.bindingSetsIn += chunk.length;

                if (DEBUG)
                    log.debug("Read chunk from source: chunkSize="
                            + chunk.length + ", orderIndex=" + orderIndex);

                return chunk;

            }

        }

        /*
         * Termination condition: the source is exhausted.
         */

        if (DEBUG)
            log.debug("Source exhausted: orderIndex=" + orderIndex);

        return null;

    }

}