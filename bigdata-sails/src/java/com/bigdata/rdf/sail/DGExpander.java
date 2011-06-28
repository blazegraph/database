package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOAccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.service.IBigdataClient;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.LatchedExecutor;

import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Parallel subquery for a default graph access path. An expander pattern is
 * used to ensure that the "DISTINCT SPO" constraint is applied across the
 * subqueries rather than to each subquery individually.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DGExpander implements IAccessPathExpander<ISPO> {

    private static final long serialVersionUID = 1L;

    final private int maxParallel;

    final private Collection<IV> graphs;

    final private long estimatedRangeCount;

    /**
     * 
     * @param maxParallel
     * @param graphs
     *            A dense ordered array of {@link IV}s.
     * @param estimatedRangeCount
     *            The estimated range count for the subquery operation across
     *            those graphs.
     * 
     * @todo A parallelism limitation is placed on the ClientIndexView through
     *       the
     *       {@link IBigdataClient.Options#CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST}
     *       . We should be able to override that through annotations on a query
     *       plan.
     */
    public DGExpander(final int maxParallel, final Collection<IV> graphs,
            final long estimatedRangeCount) {

        this.maxParallel = maxParallel;

        this.graphs = graphs;

        this.estimatedRangeCount = estimatedRangeCount;

    }

    public boolean backchain() {
        return false;
    }

    public boolean runFirst() {
        return false;
    }

    public IAccessPath<ISPO> getAccessPath(final IAccessPath<ISPO> accessPath) {

        return new DefaultGraphParallelEvaluationAccessPath(
                (SPOAccessPath) accessPath);

    }

    public String toString() {

        return super.toString() + "{maxParallel=" + maxParallel + ",ngraphs="
                + graphs.size() + ", estimatedRangeCount="
                + estimatedRangeCount + ", graphs=" + graphs + "}";

    }

    /**
     * Inner class evaluates the access path for each context using limited
     * parallelism, discarding the context argument for each {@link ISPO}, and
     * filtering out duplicate triples based on their (s,p,o) term identifiers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private final class DefaultGraphParallelEvaluationAccessPath implements
            IAccessPath<ISPO> {

        /**
         * The original access path.
         */
        private final SPOAccessPath sourceAccessPath;

        final private Executor executor;

        public String toString() {

            return super.toString() + "{baseAccessPath="
                    + sourceAccessPath.toString() + "}";

        }

        /**
         * @param accessPath
         *            The original access path.
         */
        public DefaultGraphParallelEvaluationAccessPath(
                final SPOAccessPath accessPath) {

            this.sourceAccessPath = accessPath;

            this.executor = new LatchedExecutor(accessPath.getIndexManager()
                    .getExecutorService(), maxParallel);

        }

        public IIndex getIndex() {

            return sourceAccessPath.getIndex();

        }

        public IKeyOrder<ISPO> getKeyOrder() {

            return sourceAccessPath.getKeyOrder();

        }

        public IPredicate<ISPO> getPredicate() {

            return sourceAccessPath.getPredicate();

        }

        public boolean isEmpty() {

            final IChunkedOrderedIterator<ISPO> itr = iterator(0L/* offset */,
                    1/* limit */, 1/* capacity */);

            try {

                return !itr.hasNext();

            } finally {

                itr.close();

            }

        }

        public ITupleIterator<ISPO> rangeIterator() {

            return sourceAccessPath.rangeIterator();

        }

        /**
         * Unsupported operation.
         * <p>
         * Note: this could be implemented by delegation but it is not used from
         * the context of SPARQL which lacks SELECT ... INSERT or SELECT ...
         * DELETE constructions, at least at this time.
         */
        public long removeAll() {

            throw new UnsupportedOperationException();

        }

        public IChunkedOrderedIterator<ISPO> iterator() {

            return iterator(0L/* offset */, 0L/* limit */, 0/* capacity */);

        }

        public IChunkedOrderedIterator<ISPO> iterator(final int limit,
                final int capacity) {

            return iterator(0L/* offset */, limit, capacity);

        }

        /**
         * This is the common entry point for all iterator implementations.
         * 
         * @todo Consider an alternative implementation using fully parallel
         *       evaluation of the access paths and a merge sort to combine
         *       chunks drawn from each access path, and then an iterator which
         *       skips over duplicates by considering the last returned (s,p,o).
         *       We need to: (a) allocate a buffer each time we draw from the
         *       current chunks based on the total size of the current chunks;
         *       and (b) we can only draw keys from the current chunks up to the
         *       min(nextKey) for each chunk. The min(nextKey) constraint is
         *       necessary to ensure that a merge sort will get rid of
         *       duplicates. Without that constraint it is possible that a
         *       latter chunk from some access path will report an (s,p,o) that
         *       has already be visited. (The constraint allows us to use a
         *       closed world assumption to filter duplicates after the merge
         *       sort.)
         */
        public IChunkedOrderedIterator<ISPO> iterator(final long offset,
                final long limit, final int capacity) {

            final ICloseableIterator<ISPO> src = new InnerIterator1(offset,
                    limit, capacity);

            // if (src instanceof IChunkedOrderedIterator<?>) {
            //
            // return (IChunkedOrderedIterator<ISPO>) src;
            //
            // }

            return new ChunkedWrappedIterator<ISPO>(src);

        }

        /**
         * Iterator implementation based on limited parallelism over the
         * iterators for the {@link IAccessPath} associated with each graph in
         * the default graphs set and using a {@link BTree} to filter out
         * duplicate (s,p,o) tuples.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id: DefaultGraphSolutionExpander.java 3678 2010-09-29
         *          15:48:34Z thompsonbry $
         */
        private class InnerIterator1 implements ICloseableIterator<ISPO> {

            // private final long offset;
            //
            // private final long limit;
            //
            // private final int capacity;

            /**
             * @todo buffer chunks of {@link #ISPO}s for more efficiency (lock
             *       amortization) and better alignment with the chunked source
             *       iterators. (It used to be that the only issue was
             *       {@link #hasNext()} having to maintain a chunk of known
             *       distinct tuples to be visited, but I think that wrapping
             *       the DISTINCT filter around the DGExpander fixed that
             *       problem.)
             */
            private final BlockingBuffer<ISPO> buffer;

            /**
             * The source iterator.
             */
            private final ICloseableIterator<ISPO> src;

            /**
             * @param offset
             * @param limit
             * @param capacity
             */
            public InnerIterator1(final long offset, final long limit,
                    final int capacity) {

                // this.offset = offset;
                //
                // this.limit = limit;
                //
                // this.capacity = capacity;

                this.buffer = new BlockingBuffer<ISPO>(sourceAccessPath
                        .getChunkCapacity());

                Future<Void> future = null;
                try {

                    /*
                     * Note: We do NOT get() this Future. This task will run
                     * asynchronously.
                     * 
                     * The Future is canceled IF (hopefully WHEN) the iterator
                     * is closed.
                     * 
                     * If the task itself throws an error, then it will use
                     * buffer#abort(cause) to notify the buffer of the cause (it
                     * will be passed along to the iterator) and to close the
                     * buffer (the iterator will notice that the buffer has been
                     * closed as well as that the cause was set on the buffer).
                     */

                    // run the task.
                    future = sourceAccessPath.getIndexManager()
                            .getExecutorService().submit(
                                    newRunIteratorsTask(buffer));

                    // set the future on the BlockingBuffer.
                    buffer.setFuture(future);

                    /*
                     * The outer access path will impose the "DISTINCT SPO"
                     * constraint.
                     */
                    // /*
                    // * Wrap the asynchronous iterator with one that imposes
                    // * a distinct (s,p,o) filter.
                    // */
                    // src = sourceAccessPath.getRelation()
                    // .distinctSPOIterator(buffer.iterator());
                    final IFilter filter = sourceAccessPath.getPredicate()
                            .getAccessPathFilter();
                    if (filter != null) {
                        src = new ChunkedWrappedIterator<ISPO>(new Striterator(
                                buffer.iterator()).addFilter(filter));
                    } else {
                        src = buffer.iterator();
                    }

                } catch (Throwable ex) {

                    try {

                        buffer.close();

                        if (future != null) {

                            future.cancel(true/* mayInterruptIfRunning */);

                        }

                    } catch (Throwable t) {

                        Rule2BOpUtility.log.error(t, t);

                    }

                    throw new RuntimeException(ex);

                }

            }

            public void close() {

                /*
                 * Close the iterator, interrupting the running task if
                 * necessary.
                 */

                src.close();

            }

            public boolean hasNext() {

                return src.hasNext();

            }

            public ISPO next() {

                return src.next();

            }

            public void remove() {

                throw new UnsupportedOperationException();

            }

            /**
             * Return task which will submit tasks draining the iterators for
             * each access path onto the caller's buffer.
             * 
             * @param buffer
             *            The elements drained from the iterators will be added
             *            to this buffer.
             * 
             * @return The task whose future is set on the buffer.
             */
            private Callable<Void> newRunIteratorsTask(
                    final BlockingBuffer<ISPO> buffer) {

                return new RunIteratorsTask(buffer);

            }

            /**
             * Inner {@link Callable} queues up the {@link DrainIteratorTask}s
             * on the {@link Executor}.
             */
            private final class RunIteratorsTask implements Callable<Void> {

                private final BlockingBuffer<ISPO> buffer;

                public RunIteratorsTask(final BlockingBuffer<ISPO> buffer) {

                    this.buffer = buffer;

                }

                /**
                 * Outer callable submits tasks for execution.
                 */
                public Void call() throws Exception {

                    final List<Future<Void>> tasks = new LinkedList<Future<Void>>();

                    try {

                        // Schedule tasks.
                        for (IV<?, ?> termId : graphs) {

                            final FutureTask<Void> ft = new FutureTask<Void>(
                                    new DrainIteratorTask(termId));

                            tasks.add(ft);

                            executor.execute(ft);

                        }

                        // Wait for the futures.
                        for (Future<Void> f : tasks) {

                            f.get();

                        }

                    } catch (Throwable ex) {

                        for (Future<Void> f : tasks)
                            f.cancel(true/* mayInterruptIfRunning */);

                        throw new RuntimeException(ex);

                    } finally {

                        // nothing more can be written onto the buffer.
                        buffer.close();

                    }

                    return null;

                }

            }

            /**
             * Inner callable runs an iterator for a specific access path,
             * draining the iterator onto the blocking buffer.
             * <p>
             * Note: don't pass the top-level offset, limit, capacity into the
             * per-graph AP iterator or it will skip over offset results per
             * graph! The limit needs to be imposed on the data pulled from the
             * blocking buffer, not here.
             */
            private final class DrainIteratorTask implements Callable<Void> {

                final IV<?, ?> termId;

                public DrainIteratorTask(final IV<?, ?> termId) {

                    if (termId == null)
                        throw new IllegalArgumentException();

                    this.termId = termId;

                }

                public Void call() throws Exception {

                    if (Rule2BOpUtility.log.isDebugEnabled())
                        Rule2BOpUtility.log.debug("Running iterator: c="
                                + termId);

                    /*
                     * Clear various annotations from source predicate.
                     * 
                     * expander: we are the expander.
                     * 
                     * accessPathFilter: This wraps the DGExpander. It should
                     * not be applied to each subquery.
                     * 
                     * keyOrder: The right index can change as soon as we bind
                     * [c].
                     */
                    final Predicate<ISPO> sourcePred = (Predicate<ISPO>) sourceAccessPath
                            .getPredicate()
                            .clearAnnotations(
                                    new String[] {
                                            IPredicate.Annotations.ACCESS_PATH_EXPANDER,
                                            IPredicate.Annotations.ACCESS_PATH_FILTER,
                                    // IPredicate.Annotations.KEY_ORDER
                                    });

                    // Bind the graph onto the context position variable.
                    final Predicate<ISPO> asBound = sourcePred.asBound(
                            (IVariable<?>) sourcePred.get(3/* cvar */),
                            new Constant<IV<?, ?>>(termId));

                    // Obtain the access path for the asBound predicate.
                    final IAccessPath<ISPO> asBoundAP = sourceAccessPath
                            .getRelation().getAccessPath(asBound);

                    final IChunkedOrderedIterator<ISPO> itr = asBoundAP
                            .iterator();

                    // Note: deprecated SPOAccessPath method.
                    // final IChunkedOrderedIterator<ISPO> itr =
                    // sourceAccessPath
                    // .bindContext(termId).iterator();

                    try {

                        long n = 0;

                        while (itr.hasNext()) {

                            // @todo chunk at a time processing.
                            final ISPO spo = itr.next();

                            buffer.add(spo);

                            // System.err.println(spo);

                            n++;

                        }

                        if (Rule2BOpUtility.log.isDebugEnabled())
                            Rule2BOpUtility.log.debug("Ran iterator: c="
                                    + termId + ", nvisited=" + n);

                    } finally {

                        itr.close();

                    }

                    return null;

                }

            } // class DrainIteratorTask

        } // class InnerIterator

        /**
         * Return the estimated range count.
         */
        public long rangeCount(final boolean exactIsIgnored) {

            return estimatedRangeCount;

        }

    } // class DefaultGraphAccessPath

}
