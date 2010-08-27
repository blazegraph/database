/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.QuoteOp;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask;
import com.bigdata.relation.rule.eval.pipeline.JoinTask;
import com.bigdata.relation.rule.eval.pipeline.JoinTaskFactoryTask;
import com.bigdata.relation.rule.eval.pipeline.JoinTaskSink;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.IDataService;
import com.bigdata.util.concurrent.Computable;
import com.bigdata.util.concurrent.Memoizer;
import com.ibm.icu.impl.ByteBuffer;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Maps binding sets against the shards on which the specified predicate will
 * read for each source binding set (the <i>as bound</i> predicate).
 * <p>
 * Mapping shards is used as part of a JOIN operation against a scale-out index.
 * Consider:
 * 
 * <pre>
 * JOIN( SCAN(A, loves, B), SCAN(B, loves, A) )
 * </pre>
 * 
 * and assume that the input to the join is a single empty binding set. When
 * this join is executed against a sharded relation, the left hand side of the
 * JOIN will use the POS index:
 * 
 * <pre>
 * relation.spo.POS(loves, B, A)
 * </pre>
 * 
 * and will output binding sets in which {B,A} take on values bound from the
 * visited tuples on the POS index.
 * <p>
 * The right hand side of the JOIN will use the SPO index (this is the primary
 * statement index for a triple store and also carries a bloom filter which
 * makes point tests more efficient). In order to read locally on the shards of
 * the SPO index, we must bind SCAN(B, loves, A) using each binding set obtained
 * from the left hand side of the JOIN and then route that binding set to the
 * node having the SPO shard spanning that tuple.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MapBindingSetsOverShards extends AbstractPipelineOp<IBindingSet>
        implements BindingSetPipelineOp {

    static private final transient Logger log = Logger
            .getLogger(MapBindingSetsOverShards.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param sourceOp
     * @param targetPred
     * @param annotations
     */
    protected MapBindingSetsOverShards(final BindingSetPipelineOp sourceOp,
            final IPredicate<?> targetPred,
            final Map<String, Object> annotations) {

        /*
         * Note: This quotes the target predicate to prevent its direct
         * evaluation.
         */

        super(new BOp[] { sourceOp, new QuoteOp(targetPred) }, annotations);

    }

    /**
     * Return the operator which is producing the binding sets.
     */
    protected BindingSetPipelineOp sourceOp() {

        return (BindingSetPipelineOp) get(0);

    }

    /**
     * Return the quoted target predicate.
     */
    protected IPredicate<?> targetPred() {

        // This unquote's the target predicate.
        return (IPredicate<?>) get(1).get(0);

    }

    /**
     * Note: This method should not be invoked since either we will be using a
     * native {@link ByteBuffer} and NIO to move the binding sets around (when
     * evaluating against a federation) or we will take the operator or of line
     * (when evaluating against scaleup).
     * 
     * @todo make sure that MapNodes also does not permit allocation.
     */
    public IBlockingBuffer<IBindingSet[]> newBuffer() {
        
        throw new UnsupportedOperationException();
        
    }
    
    public Future<Void> eval(final BOpContext<IBindingSet> context) {

        if (context.getFederation() == null) {

            /*
             * When not running against a federation, delegate evaluation to the
             * sourceOp. Together with a similar delegation pattern for the
             * receive operator, this effectively takes the mapShards operator
             * out of line.
             */

            return sourceOp().eval(context);

        }

        /*
         * Note: The caller's BlockingBuffer is ignored.
         */
        final FutureTask<Void> ft = new FutureTask<Void>(new MapShardsTask(
                context, sourceOp(), targetPred()));

        context.getIndexManager().getExecutorService().execute(ft);

        return ft;

    }

    /**
     * Setups up pipelined evaluation of the source operand and map binding sets
     * generated by that source operand against the asBound target predicate,
     * forwarding each binding set to the node on which we can read the data
     * locally from the shard spanning the asBound target predicate.
     */
    static private class MapShardsTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The federation is used to obtain locator scans for the access paths.
         */
        private final AbstractScaleOutFederation<?> fed;

        private final BindingSetPipelineOp sourceOp;

        /** The {@link IPredicate} target. */
        private final IPredicate<?> targetPred;

        /**
         * Used to materialize {@link JoinTaskSink}s without causing concurrent
         * requests for different sinks to block.
         */
        final private SinkMemoizer memo;

        /**
         * The name of the scale-out index associated with the next
         * {@link IPredicate} in the evaluation order.
         */
        final private String nextScaleOutIndexName;

        MapShardsTask(final BOpContext<IBindingSet> context,
                final BindingSetPipelineOp sourceOp,
                final IPredicate<?> targetPred) {

            this.fed = (AbstractScaleOutFederation<?>) context.getFederation();

            this.context = context;

            this.sourceOp = sourceOp;

            this.targetPred = targetPred;

            this.memo = new SinkMemoizer(getSink);
            
            this.nextScaleOutIndexName = targetPred.getOnlyRelationName() + "."
                    + targetPred.getKeyOrder()
            // + keyOrders[order[orderIndex + 1]]
            ;

        }

        public Void call() throws Exception {

//            final IBlockingBuffer<IBindingSet[]> sourceBuffer = sourceOp
//                    .newBuffer();
//
//            final Future<Void> sourceFuture = sourceOp.eval(fed, context,
//                    sourceBuffer);
//
//            try {
//
//                /*
//                 * Map the binding sets from the source against the asBound
//                 * target predicate.
//                 */
//
//                final IAsynchronousIterator<IBindingSet[]> itr = sourceBuffer
//                        .iterator();
//                
//                while (itr.hasNext()) {
//
//                    // FIXME handleChunk().
////                    handleChunk(itr.next());
//                    throw new UnsupportedOperationException();
//
//                }
//
                return null;
//                
//            } finally {
//
//                sourceFuture.cancel(true/* mayInterruptIfRunning */);
//
//            }

        }

//        /**
//         * Maps the chunk of {@link IBindingSet}s across the index partition(s) 
//         * for the sink join dimension.
//         * 
//         * @param a
//         *            A chunk of {@link IBindingSet}s.
//         * 
//         * FIXME optimize locator lookup.
//         * <p>
//         * Note: We always use a read-consistent view for the join evaluation so we
//         * are permitted to cache the locators just as much as we like.
//         * <p>
//         * When the buffer overflow()s, we generate the asBound() predicates, SORT
//         * them by their [fromKey] (or its predicate level equivalence), and process
//         * the sorted asBound() predicates. Since they are sorted and since they are
//         * all for the same predicate pattern (optionals will leave some variables
//         * unbound - does that cause a problem?) we know that the first partitionId
//         * is GTE to the last partitionId of the last asBound predicate. We can test
//         * the rightSeparatorKey on the PartitionLocator and immediately determine
//         * whether the asBound predicate in fact starts and (and possibly ends)
//         * within the same index partition. We only need to do a locatorScan when
//         * the asBound predicate actually crosses into the next index partition,
//         * which could also be handled by an MDI#find(key).
//         */
//        protected void handleChunk(final IBindingSet[] chunk) {
//
//            if (log.isDebugEnabled())
//                log.debug("chunkSize=" + chunk.length);
//            
//            int bindingSetsOut = 0;
//
//            // FIXME JoinStat's object is not initialized.
//            final JoinStats stats = null; 
////            final JoinStats stats = joinTask.stats;
//
//            final int naccepted = chunk.length;
//
//            for (int i = 0; i < naccepted; i++) {
//
//                // an accepted binding set.
//                final IBindingSet bindingSet = chunk[i];
//
//                /*
//                 * Locator scan for the index partitions for that predicate as
//                 * bound.
//                 */
//                final Iterator<PartitionLocator> itr = joinNexus.locatorScan(
//                        fed, targetPred.asBound(bindingSet));
//
//                while (itr.hasNext()) {
//
//                    final PartitionLocator locator = itr.next();
//
//                    if (log.isDebugEnabled())
//                        log.debug("adding bindingSet to buffer: targetPred="
//                                + targetPred + ", partitionId="
//                                + locator.getPartitionId() + ", bindingSet="
//                                + bindingSet);
//
//                    // obtain sink JoinTask from cache or dataService.
//                    final JoinTaskSink sink;
//                    try {
//                        sink = getSink(locator);
//                    } catch (InterruptedException ex) {
//                        throw new RuntimeException(ex);
//                    }
//
//                    // add binding set to the sink.
//                    if (sink.unsyncBuffer.add2(bindingSet)) {
//
//                        // another chunk out to this sink.
//                        stats.bindingSetChunksOut++;
//
//                    }
//
//                    // #of bindingSets out across all sinks for this join task.
//                    bindingSetsOut++;
//
//                }
//
//            }
//
//            stats.bindingSetsOut += bindingSetsOut;
//
//        }

        /**
         * Return the sink on which we will write {@link IBindingSet} for the
         * index partition associated with the specified locator. The sink will
         * be backed by a {@link DistributedJoinTask} running on the
         * {@link IDataService} that is host to that index partition. The
         * scale-out index will be the scale-out index for the next
         * {@link IPredicate} in the evaluation order.
         * 
         * @param locator
         *            The locator for the index partition.
         * 
         * @return The sink.
         * 
         * @throws RuntimeException
         *             If the {@link JoinTaskFactoryTask} fails.
         * @throws InterruptedException
         *             If the {@link JoinTaskFactoryTask} is interrupted.
         */
        protected JoinTaskSink getSink(final PartitionLocator locator)
                throws InterruptedException, RuntimeException {

            return memo.compute(new SinkRequest(this, locator));

        }

        /**
         * Inner implementation invoked from the {@link Memoizer}.
         * 
         * @param locator
         *            The shard locator.
         * 
         * @return The sink which will write on the downstream {@link JoinTask}
         *         running on the node for that shard.
         * 
         * @throws ExecutionException
         * @throws InterruptedException
         */
        private JoinTaskSink _getSink(final PartitionLocator locator)
                throws InterruptedException, ExecutionException {

            /*
             * Allocate/discover JoinTask on the target data service and
             * obtain a sink reference for its future and buffers.
             * 
             * Note: The JoinMasterTask uses very similar logic to setup the
             * first join dimension. Of course, it gets to assume that there
             * is no such JoinTask in existence at the time.
             */

//            final int nextOrderIndex = orderIndex + 1;

            if (log.isDebugEnabled())
                log.debug("Creating join task: targetPred="
                        + targetPred+ ", indexName="
                        + nextScaleOutIndexName + ", partitionId="
                        + locator.getPartitionId());

            final UUID sinkUUID = locator.getDataServiceUUID();

            final IDataService dataService;
            if (sinkUUID.equals(fed.getServiceUUID())) {

            /*
             * As an optimization, special case when the downstream
             * data service is _this_ data service.
             */
                dataService = (IDataService)fed.getService();
                
            } else {
            
                dataService = fed.getDataService(sinkUUID);
                
            }

//            final JoinTaskSink sink = new JoinTaskSink(fed, locator, this);
//
//            /*
//             * Export async iterator proxy.
//             * 
//             * Note: This proxy is used by the sink to draw chunks from the
//             * source JoinTask(s).
//             */
//            final IAsynchronousIterator<IBindingSet[]> sourceItrProxy;
//            if (fed.isDistributed()) {
//
//                sourceItrProxy = ((AbstractDistributedFederation<?>) fed)
//                        .getProxy(sink.blockingBuffer.iterator(), joinNexus
//                                .getBindingSetSerializer(), joinNexus
//                                .getChunkOfChunksCapacity());
//
//            } else {
//
//                sourceItrProxy = sink.blockingBuffer.iterator();
//
//            }
//
//            // the future for the factory task (not the JoinTask).
//            final Future<?> factoryFuture;
//            try {
//
//                final JoinTaskFactoryTask factoryTask = new JoinTaskFactoryTask(
//                    nextScaleOutIndexName, rule, joinNexus
//                            .getJoinNexusFactory(), order, nextOrderIndex,
//                    locator.getPartitionId(), masterProxy, masterUUID,
//                    sourceItrProxy, keyOrders, requiredVars);
//
//                // submit the factory task, obtain its future.
//                factoryFuture = dataService.submit(factoryTask);
//
//            } catch (IOException ex) {
//
//                // RMI problem.
//                throw new RuntimeException(ex);
//
//            }
//
//            /*
//             * Obtain the future for the JoinTask from the factory task's
//             * Future.
//             */
//
//            sink.setFuture((Future<?>) factoryFuture.get());
//
//            stats.fanOut++;
//            
//            return sink;

            // FIXME _getSink() not ported to new usage.
            throw new UnsupportedOperationException();

        }

        /**
         * Helper establishes a {@link JoinTaskSink} on the target
         * {@link IDataService}.
         */
        final private static Computable<SinkRequest, JoinTaskSink> getSink = new Computable<SinkRequest, JoinTaskSink>() {

            public JoinTaskSink compute(final SinkRequest req)
                    throws InterruptedException {

                try {
                    return req.mapShardsTask._getSink(req.locator);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }

            }

        };

    } // class MapShardsTask

    /**
     * Helper class models a request to obtain a sink for a given join task and
     * locator.
     * <p>
     * Note: This class must implement equals() and hashCode() since it is used
     * within the {@link Memoizer} pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class SinkRequest {

        final MapShardsTask mapShardsTask;

        final PartitionLocator locator;

        /**
         * 
         * @param locator
         *            The locator for the target shard.
         */
        public SinkRequest(final MapShardsTask mapShardsTask,
                final PartitionLocator locator) {

            this.mapShardsTask = mapShardsTask;

            this.locator = locator;
            
        }

        /**
         * Equals returns true iff parent == o.parent and index == o.index.
         */
        public boolean equals(final Object o) {

            if (this == o)
                return true;
            
            if (!(o instanceof SinkRequest))
                return false;

            final SinkRequest r = (SinkRequest) o;

            return mapShardsTask == r.mapShardsTask
                    && locator.equals(r.locator);

        }

        /**
         * The hashCode() is based directly on the hash code of the
         * {@link PartitionLocator}. All requests against a given
         * {@link Memoizer} will have the same {@link DistributedJoinTask} so
         * that field can be factored out of the hash code.
         */
        public int hashCode() {
            
            return locator.hashCode();
            
        }
        
    } // class SinkRequest

    /**
     * FIXME javadoc : A {@link Memoizer} subclass which exposes an additional
     * method to remove a {@link FutureTask} from the internal cache. This is
     * used as part of an explicit protocol to clear out cache entries once the
     * sink reference has been set on
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class SinkMemoizer extends
            Memoizer<SinkRequest/* request */, JoinTaskSink/* sink */> {

        /**
         * @param c
         */
        public SinkMemoizer(final Computable<SinkRequest, JoinTaskSink> c) {

            super(c);

        }

        int size() {
            return cache.size();
        }

        /**
         * @todo deal with breaking / awaiting a distributed query (that is what
         *       this method was used for).
         * 
         * @todo There are two distinct semantics available here. One is the set
         *       of current sinks (there is a join task fully up and running on
         *       a DS somewhere and we have a proxy for that DS). The other is
         *       the set of sinks which have been requested but may or may not
         *       have been fully realized yet. When we are breaking a join, we
         *       probably want to cancel all of the requests to obtain sinks in
         *       addition to canceling any running sinks. A similar problem may
         *       exist if we implement native SLICE since we could break the
         *       join while there are requests out to create sinks.
         * 
         *       One way to handle this is to pull the cancelSinks() method into
         *       this memoizer.
         * 
         *       However, if we broad cast the rule to the nodes and move away
         *       from this sinks model to using NIO buffers then we will just
         *       broadcast the close of each tail in turn or broadcast the break
         *       of the join.
         */
        @SuppressWarnings("unchecked")
        Iterator<JoinTaskSink> getSinks() {
            return new Striterator(cache.values().iterator()).addFilter(new Filter(){
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean isValid(final Object e) {
                    /*
                     * Filter out any tasks which are not done or which had an
                     * error.
                     */
                    final Future<JoinTaskSink> f = (Future<JoinTaskSink>)e;
                    if(!f.isDone()) {
                        return false;
                    }
                    try {f.get();}
                    catch(final ExecutionException ex) {
                        return false;
                    } catch (final InterruptedException ex) {
                        return false;
                    }
                    return true;
                }
            }).addFilter(new Resolver(){
                private static final long serialVersionUID = 1L;
                @Override
                protected Object resolve(final Object arg0) {
                    /*
                     * We filtered out any tasks which were not done and any
                     * tasks which had errors.  The future should be immediately
                     * available and Future.get() should not throw an error. 
                     */
                    final Future<JoinTaskSink> f = (Future<JoinTaskSink>)arg0;
                    try {
                        return f.get();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (final ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        
//        /**
//         * Called by the thread which atomically sets the
//         * {@link AbstractNode#childRefs} element to the computed
//         * {@link AbstractNode}. At that point a reference exists to the child
//         * on the parent.
//         * 
//         * @param req
//         *            The request.
//         */
//        void removeFromCache(final SinkRequest req) {
//
//            if (cache.remove(req) == null) {
//
//                throw new AssertionError();
//                
//            }
//
//        }

//        /**
//         * Called from {@link AbstractBTree#close()}.
//         * 
//         * @todo should we do this?  There should not be any reads against the
//         * the B+Tree when it is close()d.  Therefore I do not believe there 
//         * is any reason to clear the FutureTask cache.
//         */
//        void clear() {
//            
//            cache.clear();
//            
//        }
        
    } // class SinkMemoizer

}
