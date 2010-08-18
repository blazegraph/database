package com.bigdata.relation.rule.eval.pipeline;

import java.util.Iterator;

import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.IBigdataFederation;

/**
 * Unsynchronized buffer maps the {@link IBindingSet}s across the index
 * partition(s) for the target scale-out index when it overflows.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements in the buffer.
 */
class UnsyncDistributedOutputBuffer<E extends IBindingSet> extends
        AbstractUnsynchronizedArrayBuffer<E> {
    
    private final DistributedJoinTask joinTask;
    
    /** The evaluation order of the next predicate. */
    private final int nextOrderIndex;
    
    /** The tailIndex of the next predicate to be evaluated. */
    final int nextTailIndex;
    
    final IBigdataFederation<?> fed;

    /**
     * 
     * @param fed
     * @param joinTask
     * @param capacity
     */
    public UnsyncDistributedOutputBuffer(final AbstractScaleOutFederation<?> fed,
            final DistributedJoinTask joinTask, final int capacity) {

        super(capacity);

        if (fed == null)
            throw new IllegalArgumentException();

        if (joinTask == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.joinTask = joinTask;
        
        this.nextOrderIndex = joinTask.orderIndex + 1;
        
        this.nextTailIndex = joinTask.getTailIndex(nextOrderIndex);

    }
    
    /**
     * Maps the chunk of {@link IBindingSet}s across the index partition(s) for
     * the sink join dimension.
     * 
     * @param a
     *            A chunk of {@link IBindingSet}s.
     * 
     * FIXME optimize locator lookup.
     * <p>
     * Note: We always use a read-consistent view for the join evaluation so we
     * are permitted to cache the locators just as much as we like.
     * <p>
     * When the buffer overflow()s, we generate the asBound() predicates, SORT
     * them by their [fromKey] (or its predicate level equivalence), and process
     * the sorted asBound() predicates. Since they are sorted and since they are
     * all for the same predicate pattern (optionals will leave some variables
     * unbound - does that cause a problem?) we know that the first partitionId
     * is GTE to the last partitionId of the last asBound predicate. We can test
     * the rightSeparatorKey on the PartitionLocator and immediately determine
     * whether the asBound predicate in fact starts and (and possibly ends)
     * within the same index partition. We only need to do a locatorScan when
     * the asBound predicate actually crosses into the next index partition,
     * which could also be handled by an MDI#find(key).
     */
    protected void handleChunk(final E[] chunk) {

        if (DEBUG)
            log.debug("chunkSize=" + chunk.length);
        
        int bindingSetsOut = 0;

        // the next predicate to be evaluated.
        final IPredicate<?> nextPred = joinTask.rule.getTail(nextTailIndex);

        final IJoinNexus joinNexus = joinTask.joinNexus;

        final JoinStats stats = joinTask.stats;

        final int naccepted = chunk.length;

        for (int i = 0; i < naccepted; i++) {

            // an accepted binding set.
            final IBindingSet bindingSet = chunk[i];

            /*
             * Locator scan for the index partitions for that predicate as
             * bound.
             */
            final Iterator<PartitionLocator> itr = joinNexus.locatorScan(
                    joinTask.fed, nextPred.asBound(bindingSet));

            while (itr.hasNext()) {

                final PartitionLocator locator = itr.next();

                if (DEBUG)
                    log
                            .debug("adding bindingSet to buffer: nextOrderIndex="
                                    + nextOrderIndex
                                    + ", partitionId="
                                    + locator.getPartitionId()
                                    + ", bindingSet=" + bindingSet);

                // obtain sink JoinTask from cache or dataService.
                final JoinTaskSink sink;
                try {
                    sink = joinTask.getSink(locator);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

                // add binding set to the sink.
                if (sink.unsyncBuffer.add2(bindingSet)) {

                    // another chunk out to this sink.
                    stats.bindingSetChunksOut++;

                }

                // #of bindingSets out across all sinks for this join task.
                bindingSetsOut++;

            }

        }

        stats.bindingSetsOut += bindingSetsOut;

    }

}
