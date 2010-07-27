/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Sep 25, 2009
 */

package com.bigdata.rdf.spo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.MappedTaskExecutor;

/**
 * Solution expander provides an efficient merged access path for SPARQL named
 * graphs. The expander applies the access path to the graph associated with
 * each specified URI, visiting the all (s,p,o,c) quads found in those graph(s).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME See notes on the {@link DefaultGraphSolutionExpander}
 *          concerning scale-out joins.
 */
public class NamedGraphSolutionExpander implements ISolutionExpander<ISPO> {

    private static final long serialVersionUID = 2399492655063411719L;

    protected static transient Logger log = Logger
            .getLogger(NamedGraphSolutionExpander.class);

    /**
     * The set of named graphs. The {@link URI}s MUST have been resolved against
     * the appropriate {@link LexiconRelation} such that their term identifiers
     * (when the exist) are known. If any term identifier is
     * {@link IRawTripleStore#NULL}, then the corresponding graph does not exist
     * and no access path will be queried for that graph. However, a non-
     * {@link IRawTripleStore#NULL} term identifier may also identify a graph
     * which does not exist, in which case an access path will be created for
     * that {@link URI}s but will no visit any data.
     */
    private final Iterable<? extends URI> namedGraphs;

    /**
     * The #of source graphs in {@link #namedGraphs} whose term identifier is
     * known. While this is not proof that there is data in the quad store for a
     * graph having the corresponding {@link URI}, it does allow the possibility
     * that a graph could exist for that {@link URI}. If {@link #nknown} is ZERO
     * (0), then the {@link EmptyAccessPath} should be used. If {@link #nknown}
     * is ONE (1), then the caller's {@link IAccessPath} should be used and
     * filtered to remove the context information. If {@link #namedGraphs} is
     * <code>null</code>, which implies that ALL graphs in the quad store will
     * be used as the default graph, then {@link #nknown} will be
     * {@link Integer#MAX_VALUE}.
     */
    // note: not 'final' due to bizarre compiler error under linux for JDK 1.6.0_16
    private /*final*/ int nknown;

    /**
     * The term identifier for the first graph and {@link IRawTripleStore#NULL}
     * if no graphs were specified having a term identifier.
     */
    // note: not 'final' due to bizarre compiler error under linux for JDK 1.6.0_16
    private /*final*/ IV firstContext;

    /**
     * Filter iff we will leave [c] unbound and filter for graphs which are in
     * the source graph set.
     */
    private /*final*/ IElementFilter<ISPO> filter;
    
    /**
     * If the caller can identify that some graph URIs are not known to the
     * database, then they may be safely removed from the namedGraphs (and query
     * will proceed as if they had been removed). If this leaves an empty set,
     * then no query against the default graph can yield any data.
     * 
     * @param namedGraphs
     *            The set of named graphs in the SPARQL DATASET (optional). A
     *            runtime exception will be thrown during evaluation of the if
     *            the {@link URI}s are not {@link BigdataURI}s. If this is
     *            <code>null</code>, then the set of named graphs is understood
     *            to be ALL graphs in the quad store.
     */
    public NamedGraphSolutionExpander(final Iterable<? extends URI> namedGraphs) {

        this.namedGraphs = namedGraphs;

        IV firstContext = null;
        
        if(namedGraphs == null) {
            
            nknown = Integer.MAX_VALUE;
            
        } else {

            final Iterator<? extends URI> itr = namedGraphs.iterator();

            int nknown = 0;

            while (itr.hasNext()) {

                final BigdataURI uri = (BigdataURI) itr.next();

                if (uri.getIV() != null) {

                    if (++nknown == 1) {

                        firstContext = uri.getIV();

                    }

                }

            } // while
            
            this.nknown = nknown;
            
        }
        
        this.firstContext = firstContext;
        
        // @todo configure threshold or pass into this constructor.
        if (namedGraphs != null && nknown > 200) {

            filter = new InGraphHashSetFilter(nknown, namedGraphs);
            
        } else filter = null;

    }
    
    public boolean backchain() {
        
        return true;
        
    }

    public boolean runFirst() {

        return false;
        
    }

    public IAccessPath<ISPO> getAccessPath(final IAccessPath<ISPO> accessPath1) {

        if (accessPath1 == null)
            throw new IllegalArgumentException();

        if(!(accessPath1 instanceof SPOAccessPath)) {
            
            // The logic relies on wrapping an SPOAccessPath, at least for now.
            throw new IllegalArgumentException();
            
        }

        final SPOAccessPath accessPath = (SPOAccessPath) accessPath1;
        
        final IVariableOrConstant<IV> c = accessPath.getPredicate().get(3);

        if (c != null && c.isConstant()) {

            /*
             * The context is bound, so we will query a single graph (the
             * context MUST NOT be bound if you want to query more than one
             * graph).
             */
            
            return accessPath1;
            
        }
        
        if (nknown == 0) {

            /*
             * None of the graph URIs is known to the quad store.
             */

            return new EmptyAccessPath<ISPO>(accessPath.getPredicate(),
                    accessPath.getKeyOrder());

        }

        if (nknown == 1) {

            /*
             * There is just one source graph. We bind the context on the access
             * path and return that.
             */

            return accessPath.bindContext(firstContext);

        }

        if (namedGraphs == null) {

            /*
             * Query all graphs in the quad store. As long as [c] is not bound
             * as a constant this is the behavior of the original access path
             * and we do not have to do anything.
             */

            if (c != null && c.isConstant()) {

                // [c] must not be bound as a constant.

                throw new AssertionError();
                
            }
            
            return accessPath;

        }

        if (filter != null) {

            /*
             * FIXME choose this approach when we need to visit most of the
             * index anyway.
             * 
             * RDF merge of the graphs whose term identifiers are defined based
             * on an access path in which the context position is unbound and an
             * IN filter which restricts the selected quads to those found in
             * the set of source graphs. The access path must also strip off the
             * context position and then filter for the distinct (s,p,o) tuples.
             * 
             * Note: One advantage of this approach is that it does not require
             * us to use a remote read on a different shard. This approach may
             * also perform better when reading against a significant fraction
             * of the KB, e.g., 20% or better.
             * 
             * Note: When using this code path, rangeCount(false) will
             * overestimate the count because it is not using the iterator and
             * applying the filter and therefore will see all contexts, not just
             * the one specified by [c].
             */

            final SPOPredicate p = accessPath.getPredicate().setConstraint(
                    filter);

            /*
             * Wrap with access path that leaves [c] unbound.
             * 
             * Note: This will wind up assigning the same index since we have
             * not changed the bindings on the predicate, only made the filter
             * associated with the predicate more restrictive.
             */

            return new NamedGraphsFilteredAccessPath((SPOAccessPath) accessPath
                    .getRelation().getAccessPath(p));

        }
        
        // @todo should we check accessPath.isEmpty() here?

        return new NamedGraphsParallelEvaluationAccessPath(accessPath);

    }

    /**
     * Inner class evaluates the access path for each context specified in the
     * {@link NamedGraphSolutionExpander#namedGraphs} using limited parallelism.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private final class NamedGraphsParallelEvaluationAccessPath implements IAccessPath<ISPO> {

        private final long timeout = Long.MAX_VALUE;

        private final TimeUnit unit = TimeUnit.SECONDS;

        private final int maxParallel;

        private final MappedTaskExecutor executor;

        /**
         * The original access path.
         */
        private final SPOAccessPath sourceAccessPath;

        public String toString() {

            return super.toString() + "{baseAccessPath="
                    + sourceAccessPath.toString() + "}";

        }

        /**
         * @param accessPath
         *            The original access path.
         */
        public NamedGraphsParallelEvaluationAccessPath(final SPOAccessPath accessPath) {

            this.sourceAccessPath = accessPath;

            this.executor = new MappedTaskExecutor(accessPath.getIndexManager()
                    .getExecutorService());

            /*
             * FIXME maxParallelSubqueries is zero for pipeline joins, but we
             * want to use a non-zero value here anyway. Rather than declaring a
             * new parameter, change the pipeline joins so that the ignore this
             * parameter. Also note that a parallelism limitation is placed on
             * the ClientIndexView through the IBigdataClient.Options.
             */

            this.maxParallel = accessPath.getRelation()
                    .getMaxParallelSubqueries();

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
         */
        @SuppressWarnings("unchecked")
        public IChunkedOrderedIterator<ISPO> iterator(final long offset,
                final long limit, final int capacity) {

            final ICloseableIterator<ISPO> src = new InnerIterator(offset,
                    limit, capacity);

            if(src instanceof IChunkedOrderedIterator) {
                
                return (IChunkedOrderedIterator<ISPO>)src;

            }
            
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
         * @version $Id$
         */
        private class InnerIterator implements ICloseableIterator<ISPO> {

            private final long offset;

            private final long limit;

            private final int capacity;

            /**
             * @todo buffer chunks of {@link #ISPO}s for more efficiency and
             *       better alignment with the chunked source iterators? The
             *       only issue is that {@link #hasNext()} will have to maintain
             *       a chunk of known distinct tuples to be visited.
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
            public InnerIterator(final long offset, final long limit,
                    final int capacity) {

                this.offset = offset;

                this.limit = limit;
                
                this.capacity = capacity;

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
                    future = sourceAccessPath.getIndexManager().getExecutorService()
                            .submit(newRunIteratorsTask(buffer));

                    // set the future on the BlockingBuffer.
                    buffer.setFuture(future);

                    // Save reference to the asynchronous iterator.
                    src = buffer.iterator();

                } catch (Throwable ex) {

                    try {

                        buffer.close();

                        if (future != null) {

                            future.cancel(true/* mayInterruptIfRunning */);

                        }

                    } catch (Throwable t) {

                        log.error(t, t);

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
             * 
             * @todo if the outer task is interrupted, will that interrupt
             *       propagate to the inner tasks? If not, then use a volatile
             *       boolean halted pattern here.
             */
            private Callable<Void> newRunIteratorsTask(
                    final BlockingBuffer<ISPO> buffer) {

                return new Callable<Void>() {

                    /**
                     * Outer callable submits tasks for execution.
                     */
                    public Void call() throws Exception {

                        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

                        for (URI g : namedGraphs) {

                            final IV termId = ((BigdataURI)g).getIV();

                            if (termId == null) {

                                // unknown URI means no data for that graph.
                                continue;

                            }

                            tasks.add(new DrainIteratorTask(termId));

                        }

                        try {

                            if (log.isDebugEnabled())
                                log.debug("Running " + tasks.size() + " tasks");
                            
                            executor
                                    .runTasks(tasks, timeout, unit, maxParallel);

                        } catch (Throwable ex) {

                            throw new RuntimeException(ex);

                        } finally {

                            // nothing more can be written onto the buffer.
                            buffer.close();
                            
                        }

                        return null;

                    }

                };

            }

            /**
             * Inner callable runs an iterator for a specific access path,
             * draining the iterator onto the blocking buffer.
             */
            private final class DrainIteratorTask implements Callable<Void> {

                final IV termId;

                public DrainIteratorTask(final IV termId) {

                    this.termId = termId;

                }

                public Void call() throws Exception {

                    if (log.isDebugEnabled())
                        log.debug("Running iterator: c=" + termId);

                    final IChunkedOrderedIterator<ISPO> itr = sourceAccessPath
                            .bindContext(termId).iterator(offset, limit,
                                    capacity);

                    try {

                        long n = 0;

                        while (itr.hasNext()) {

                            // @todo chunk at a time processing.
                            buffer.add(itr.next());
                            
                            n++;

                        }

                        if (log.isDebugEnabled())
                            log.debug("Ran iterator: c=" + termId
                                    + ", nvisited=" + n);
                        
                    } finally {

                        itr.close();

                    }

                    return null;

                }

            } // class DrainIteratorTask

        } // class InnerIterator

        /**
         * Takes the sum over the estimated/exact range count of each access
         * path using limited (chunked) parallel evaluation since there could be
         * a lot of graphs in the default graph.
         * 
         * FIXME cache historical range counts (exact and fast) for reuse.
         */
        public long rangeCount(final boolean exact) {

            final List<Callable<Long>> tasks = new LinkedList<Callable<Long>>();

            for (URI g : namedGraphs) {

                final IV termId = ((BigdataURI) g).getIV();

                if (termId == null) {

                    // unknown URI means no data for that graph.
                    continue;

                }

                tasks.add(new RangeCountTask(exact, termId));

            }

            try {

                final List<Long> counts = executor.runTasks(tasks, timeout,
                        unit, maxParallel);

                long n = 0;

                for (Long c : counts) {

                    n += c.longValue();

                }

                return n;

            } catch (Throwable ex) {

                throw new RuntimeException(ex);

            }

        }

        /**
         * Fast range count for the access path for a graph.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        private class RangeCountTask implements Callable<Long> {

            private final boolean exact;

            private final IV c;

            /**
             * 
             * @param c
             *            The graph identifier.
             */
            public RangeCountTask(final boolean exact, final IV c) {

                this.exact = exact;

                this.c = c;

            }

            public Long call() throws Exception {

                return Long.valueOf(sourceAccessPath.bindContext(c).rangeCount(
                        exact));

            }

        }

    } // class NamedGraphParallelEvaluationAccessPath

    /**
     * An access path that leaves [c] unbound and filters for the desired set of
     * named graphs.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class NamedGraphsFilteredAccessPath implements IAccessPath<ISPO> {

        private final SPOAccessPath sourceAccessPath;
        
        public NamedGraphsFilteredAccessPath(final SPOAccessPath accessPath) {

            final IVariableOrConstant<?> cvar = accessPath.getPredicate()
                    .get(3);

            if (cvar != null && cvar.isConstant()) {

                // The context position must not be bound.
                throw new IllegalArgumentException();
                
            }
            
            this.sourceAccessPath = accessPath;
            
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
         
            return sourceAccessPath.isEmpty();
            
        }

        public IChunkedOrderedIterator<ISPO> iterator() {
            
            return iterator(0L/* offset */, 0L/* limit */, 0/* capacity */);
            
        }

        public IChunkedOrderedIterator<ISPO> iterator(final int limit,
                final int capacity) {

            return iterator(0L/* offset */, limit, capacity);

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
        
        /**
         * FIXME cache historical range counts (exact and fast) for reuse.
         */
        public long rangeCount(final boolean exact) {
            
            if (exact) {

                /*
                 * Exact range count.
                 * 
                 * Note: The only way to get the exact range count is to run the
                 * iterator so we can decide which triples are duplicates.
                 */
                
                final IChunkedOrderedIterator<ISPO> itr = iterator();

                long n = 0;

                try {

                    while(itr.hasNext()) {
                        
                        itr.next();
                        
                        n++;
                        
                    }
                    
                } finally {

                    itr.close();

                }

                return n;

            } else {

                /*
                 * Estimated range count.
                 * 
                 * Note: The estimate is the maximum since it does not filter
                 * duplicates.
                 */

                return sourceAccessPath.rangeCount(false/* exact */);
                
            }
            
        }

        /**
         * Core implementation delegates the iterator to the source access path.
         */
        public IChunkedOrderedIterator<ISPO> iterator(final long offset,
                final long limit, final int capacity) {

            return sourceAccessPath.iterator(offset, limit, capacity);

        }
        
    } // NamedGraphsFilteredAccessPath

}
