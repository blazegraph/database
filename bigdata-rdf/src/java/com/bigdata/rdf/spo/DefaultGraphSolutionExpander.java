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


import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.MappedTaskExecutor;

/**
 * Solution expander provides an efficient merged access path for the graphs in
 * the SPARQL default graph. The expander applies the access path to the graph
 * associated with each specified URI, visiting the distinct (s,p,o) tuples
 * found in those graph(s). The context position of the visited {@link ISPO}s is
 * discarded (set to null). Duplicate triples are discarded using a
 * {@link BTree} for the {@link SPOKeyOrder#SPO} key order with its bloom filter
 * enabled. The result is the distinct union of the access paths and hence
 * provides a view of the source graphs as if they had been merged according to
 * <a href="http://www.w3.org/TR/rdf-mt/#graphdefs>RDF Semantics</a>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This class will have to be revisited if want to support quad store
 *       inference and expose information about inferred vs explicit statements
 *       when reading on the default graph. All of its access paths strip out
 *       the {@link StatementEnum}.
 * 
 *       FIXME Scale-out joins depend on knowledge of the best access path and
 *       the index partitions (shards) which it will traverse. Review all of the
 *       new expanders and make sure that they do not violate this principle.
 *       Expanders tend to lazily determine the access path, and I believe that
 *       RDFJoinNexus#getTailAccessPath() may even refuse to operate with
 *       expanders. If this is the case, then the choice of the access path
 *       needs to be completely coded into the predicate as a combination of
 *       binding or clearing the context variable and setting an appropriate
 *       constraint (filter).
 *       <p>
 *       For scale-out this could place us onto a different shard and hence a
 *       different data service with the consequence that we wind up doing RMI
 *       for the access path. In order to avoid that we need to rewrite the rule
 *       to use a nested query along the lines of:
 * 
 *       <pre>
 * DISTINCT (s,p,o)
 *  UNION
 *      SELECT s,p,o FROM g1
 *      SELECT s,p,o FROM g2
 *      ...
 *      SELECT s,p,o FROM gn
 * </pre>
 * 
 *       The alternative approach for scale-out is to add a filter so that only
 *       the specific context is accepted. This filter MUST applied for ALL
 *       possible contexts (or all on that shard) so we only run the access path
 *       once rather than once per context.
 */
public class DefaultGraphSolutionExpander implements IAccessPathExpander<ISPO> {

    protected static transient Logger log = Logger
            .getLogger(DefaultGraphSolutionExpander.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 3092400550324170339L;
    
    /**
     * The set of graphs in the SPARQL DATASET's default graph. The {@link URI}s
     * MUST have been resolved against the appropriate {@link LexiconRelation}
     * such that their term identifiers (when the exist) are known. If any term
     * identifier is {@link IRawTripleStore#NULL}, then the corresponding graph
     * does not exist and no access path will be queried for that graph.
     * However, a non-{@link IRawTripleStore#NULL} term identifier may also
     * identify a graph which does not exist, in which case an access path will
     * be created for that {@link URI}s but will no visit any data.
     */
    private final Iterable<? extends URI> defaultGraphs;

    /**
     * The #of source graphs in {@link #defaultGraphs} whose term identifier is
     * known. While this is not proof that there is data in the quad store for a
     * graph having the corresponding {@link URI}, it does allow the possibility
     * that a graph could exist for that {@link URI}. If {@link #nknown} is ZERO
     * (0), then the {@link EmptyAccessPath} should be used. If {@link #nknown}
     * is ONE (1), then the caller's {@link IAccessPath} should be used and
     * filtered to remove the context information. If {@link #defaultGraphs} is
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
    // note: not 'final' due to bizarre compiler error under linux for JDK 1.6.0_16
    private /*final*/ IElementFilter<ISPO> filter;

    /**
     * Return the #of source graphs URIs associated with term identifiers in
     * the database (possible graphs).
     */
    public int getKnownGraphCount() {
        
        return nknown;
        
    }
    
    /**
     * Return an iterator which will visit the source graphs.
     */
    public Iterator<? extends URI> getGraphs() {
        
        return defaultGraphs.iterator();
        
    }
    
    /**
     * Using the expander makes sense even when there is a single graph in the
     * default graph since the expander will strip the context information from
     * the materialized {@link ISPO}s. If the caller can identify that some
     * graph URIs are not known to the database, then they may be safely removed
     * from the defaultGraphs (and query will proceed as if they had been
     * removed). If this leaves an empty set, then no query against the default
     * graph can yield any data.
     * 
     * @param defaultGraphs
     *            The set of default graphs in the SPARQL DATASET (optional). A
     *            runtime exception will be thrown during evaluation of the if
     *            the {@link URI}s are not {@link BigdataURI}s. If this is
     *            <code>null</code>, then the default graph is understood to be
     *            the RDF merge of ALL graphs in the quad store.
     */
    public DefaultGraphSolutionExpander(
            final Iterable<? extends URI> defaultGraphs) {
        
        this.defaultGraphs = defaultGraphs;
        
        if(defaultGraphs == null) {
            
            this.nknown = Integer.MAX_VALUE;
            
            this.filter = null;
            
            this.firstContext = null;

            return;
            
        }

        final Iterator<? extends URI> itr = defaultGraphs.iterator();

        IV firstContextLocal = null;
        
        int nknownLocal = 0;

        while (itr.hasNext()) {

            final BigdataURI uri = (BigdataURI) itr.next();

            if (uri.getIV() != null) {

                if (++nknownLocal == 1) {

                    firstContextLocal = uri.getIV();

                }

            }

        } // while
        
        this.nknown = nknownLocal;
        
        // @todo configure threshold or pass into this constructor.
        this.filter = nknown > 200 ? new InGraphHashSetFilter(nknown,
                defaultGraphs) : null;

        this.firstContext = firstContextLocal;

    }
    
    public boolean backchain() {
        
        return true;
        
    }

    public boolean runFirst() {

        return false;
        
    }

    /**
     * @throws IllegalArgumentException
     *             if the context position is bound.
     */
    public IAccessPath<ISPO> getAccessPath(final IAccessPath<ISPO> accessPath1) {

        if (accessPath1 == null)
            throw new IllegalArgumentException();

        if(!(accessPath1 instanceof SPOAccessPath)) {
            
            // The logic relies on wrapping an SPOAccessPath, at least for now.
            throw new IllegalArgumentException(accessPath1.getClass().toString());
            
        }

        final SPOAccessPath accessPath = (SPOAccessPath) accessPath1;
        
        final IVariableOrConstant<IV> c = accessPath.getPredicate().get(3);

        if (c != null && c.isConstant()) {

            // the context position should not be bound.
            throw new IllegalArgumentException();
            
        }
        
        if (nknown == 0) {

            /*
             * The default graph is an empty graph.
             */

            return new EmptyAccessPath<ISPO>(accessPath.getPredicate(),
                    accessPath.getKeyOrder());

        }

        if (nknown == 1) {

            /*
             * There is just one source graph for the default graph. The RDF
             * Merge of a single graph is itself. We bind the context on the
             * access path and strip out the context from the visited SPOs.
             */

            return new StripContextAccessPath(firstContext, accessPath);

        }

        if (defaultGraphs == null) {

            /*
             * RDF merge of all graphs in the quad store.
             * 
             * Note: We handle the single graph case first since it scales
             * better if there is only one graph.
             */
            
            return new MergeAllGraphsAccessPath(accessPath);

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

            final Predicate<ISPO> p = accessPath.getPredicate().addIndexLocalFilter(
                    ElementFilter.newInstance(filter));

            /*
             * Wrap with access path that leaves [c] unbound but strips off the
             * context position and filters for the distinct (s,p,o) tuples.
             * 
             * Note: This will wind up assigning the same index since we have
             * not changed the bindings on the predicate, only made the filter
             * associated with the predicate more restrictive.
             */

            return new MergeAllGraphsAccessPath((SPOAccessPath) accessPath
                    .getRelation().getAccessPath(p));

        }
        
        // @todo should we check accessPath.isEmpty() here?

        return new DefaultGraphParallelEvaluationAccessPath(accessPath);

    }

    /**
     * Inner class evaluates the access path for each context specified in the
     * {@link DefaultGraphSolutionExpander#defaultGraphs} using limited
     * parallelism, discarding the context argument for each {@link ISPO}, and
     * filtering out duplicate triples based on their (s,p,o) term identifiers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private final class DefaultGraphParallelEvaluationAccessPath implements IAccessPath<ISPO> {

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
        public DefaultGraphParallelEvaluationAccessPath(final SPOAccessPath accessPath) {

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
         * 
         * @todo Alternative implementation using fully parallel evaluation of
         *       the access paths and a merge sort to combine chunks drawn from
         *       each access path, and then an iterator which skips over
         *       duplicates by considering the last returned (s,p,o). We need
         *       to: (a) allocate a buffer each time we draw from the current
         *       chunks based on the total size of the current chunks; and (b)
         *       we can only draw keys from the current chunks up to the
         *       min(nextKey) for each chunk. The min(nextKey) constraint is
         *       necessary to ensure that a merge sort will get rid of
         *       duplicates. Without that constraint it is possible that a
         *       latter chunk from some access path will report an (s,p,o) that
         *       has already be visited. (The constraint allows us to use a
         *       closed world assumption to filter duplicates after the merge
         *       sort.)
         */
        @SuppressWarnings("unchecked")
        public IChunkedOrderedIterator<ISPO> iterator(final long offset,
                final long limit, final int capacity) {

            final ICloseableIterator<ISPO> src = new InnerIterator1(offset,
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
        private class InnerIterator1 implements ICloseableIterator<ISPO> {

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
            public InnerIterator1(final long offset, final long limit,
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

                    /*
                     * Wrap the asynchronous iterator with one that imposes a
                     * distinct (s,p,o) filter.
                     */
                    src = sourceAccessPath.getRelation().distinctSPOIterator(
                            buffer.iterator());

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

                /*
                 * Put the graphs into termId order. Since the individual access
                 * paths will be formed by binding [c] to each graphId in turn,
                 * evaluating those access paths in graphId order will make
                 * better use of the B+Tree cache as the reads will tend to be
                 * more clustered.
                 * 
                 * FIXME ordered visitation for named graphs also.
                 */
                final IV[] a = new IV[nknown];
                
                int i = 0;
                
                for (URI g : defaultGraphs) {

                    final IV termId = ((BigdataURI)g).getIV();

                    if (termId == null) {

                        // unknown URI means no data for that graph.
                        continue;

                    }

                    a[i++] = termId;

                }

                Arrays.sort(a);
                
                return new Callable<Void>() {

                    /**
                     * Outer callable submits tasks for execution.
                     */
                    public Void call() throws Exception {

                        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

                        for (IV termId : a) {

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

                    /*
                     * Note: don't pass the top-level offset, limit, capacity
                     * into the per-graph AP iterator or it will skip over
                     * offset results per graph! The limit needs to be imposed
                     * on the data pulled from the blocking buffer, not here.
                     * 
                     * FIXME verify offset/limit iterators patterns for this
                     * class and that the tasks are properly shutdown. In fact,
                     * we will need a halt() method since we want to not only
                     * interrupt the running tasks but also not start new tasks.
                     * Further, it would be better to incrementally schedule
                     * tasks rather than scheduling them all to run up front.
                     */

                    final IChunkedOrderedIterator<ISPO> itr = sourceAccessPath
                            .bindContext(termId).iterator();

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
                        
                        if (BigdataStatics.debug)
                            System.err.println("Ran iterator: c=" + termId
                                    + ", nvisited=" + n);
                        
                    } finally {

                        itr.close();

                    }

                    return null;

                }

            } // class DrainIteratorTask

        } // class InnerIterator

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
                 * Take the sum over the estimated range count of each access
                 * path using limited (chunked) parallel evaluation since there
                 * could be a lot of graphs in the default graph. While the sum
                 * can overestimate (it presumes that there are no duplicates),
                 * but using max can underestimate since it presumes that all
                 * tuples from distinct graphs are duplicates.
                 */
             
                final List<Callable<Long>> tasks = new LinkedList<Callable<Long>>();
                
                for(URI g : defaultGraphs) {

                    final IV termId = ((BigdataURI)g).getIV();

                    if (termId == null) {
                        
                        // unknown URI means no data for that graph.
                        continue;
                        
                    }

                    tasks.add(new RangeCountTask(termId));
                 
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

        }

        /**
         * Fast range count for the access path for a graph.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        private class RangeCountTask implements Callable<Long> {

            private final IV c;

            /**
             * 
             * @param c
             *            The graph identifier.
             */
            public RangeCountTask(final IV c) {

                this.c = c;

            }

            public Long call() throws Exception {

                return Long.valueOf(sourceAccessPath.bindContext(c).rangeCount(
                        false/* exact */));

            }

        }

    } // class DefaultGraphAccessPath

    /**
     * An access path which is used when the RDF merge of all graphs in the quad
     * store is required. The access path leaves [c] unbound, strips off the
     * context information, and filters for distinct (s,p,o) triples.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class MergeAllGraphsAccessPath implements IAccessPath<ISPO> {

        private final SPOAccessPath sourceAccessPath;
        
        public MergeAllGraphsAccessPath(final SPOAccessPath accessPath) {

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
         * Core implementation delegates the iterator to the source access path
         * and then applies a filter such that only the distinct (s,p,o) tuples
         * will be visited.
         */
        public IChunkedOrderedIterator<ISPO> iterator(final long offset,
                final long limit, final int capacity) {

            if (sourceAccessPath.rangeCount(false/* exact */) == 1L) {
                // do not wrap since must be distinct.
                return sourceAccessPath.iterator(offset, limit, capacity);
            }

            final ICloseableIterator<ISPO> src = sourceAccessPath.iterator(
                    offset, limit, capacity);

            return new ChunkedWrappedIterator<ISPO>(sourceAccessPath
                    .getRelation().distinctSPOIterator(src), sourceAccessPath
                    .getChunkCapacity(), sourceAccessPath.getKeyOrder(), null/* filter */);

        }
        
    } // MergeAllGraphsAccessPath

    /**
     * Delegates everything but strips the context position and the statement
     * type from the visited (s,p,o)s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class StripContextAccessPath implements IAccessPath<ISPO> {

        private final IAccessPath<ISPO> sourceAccessPath;

        /**
         * 
         * @param c
         *            The term identifier for the graph.
         * 
         * @param sourceAccessPath
         *            The original access path.
         */
        public StripContextAccessPath(final IV c,
                final SPOAccessPath sourceAccessPath) {

            this.sourceAccessPath = sourceAccessPath.bindContext(c);
            
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

        public long rangeCount(final boolean exact) {
            
            return sourceAccessPath.rangeCount(exact);
            
        }

        public ITupleIterator<ISPO> rangeIterator() {
            
            return sourceAccessPath.rangeIterator();
            
        }

        public long removeAll() {
            
            return sourceAccessPath.removeAll();
            
        }

        public IChunkedOrderedIterator<ISPO> iterator(final long offset,
                final long limit, final int capacity) {

            return new ChunkedWrappedIterator<ISPO>(new StripContextIterator(
                    sourceAccessPath.iterator(offset, limit, capacity)));

        }

    }

    /**
     * Strips off the context information.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class StripContextIterator implements
            ICloseableIterator<ISPO> {

        private final ICloseableIterator<ISPO> src;
        
        public StripContextIterator(final ICloseableIterator<ISPO> src){ 
        
            this.src = src;
            
        }
        
        public void close() {
        
            src.close();
            
        }

        public boolean hasNext() {
            
            return src.hasNext();
            
        }

        public ISPO next() {

            final ISPO tmp = src.next();

            return new SPO(tmp.s(), tmp.p(), tmp.o());

        }

        public void remove() {

            src.remove();
            
        }

    }

}
