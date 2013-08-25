package com.bigdata.rdf.graph.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;

import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IScheduler;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;

import cutthecrap.utils.striterators.ArrayIterator;

@SuppressWarnings("rawtypes")
public class GASState<VS, ES, ST> implements IGASState<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASState.class);

    /**
     * The {@link GASEngine} on which the {@link IGASProgram} will be run.
     */
    private final GASEngine gasEngine;

    /**
     * The {@link IGASProgram} to be run.
     */
    private final IGASProgram<VS, ES, ST> gasProgram;
    
    /**
     * Factory for the vertex state objects.
     */
    private final Factory<IV, VS> vsf;

    /**
     * Factory for the edge state objects.
     */
    private final Factory<ISPO, ES> esf;

    /**
     * The set of vertices that were identified in the current iteration.
     * <p>
     * Note: This data structure is reused for each round.
     * 
     * @see StaticFrontier
     * @see CHMScheduler
     * @see #scheduler
     */
    private final StaticFrontier frontier;

    /**
     * Used to schedule the new frontier and then compact it onto
     * {@link #frontier} at the end of the round.
     */
    private final MyScheduler scheduler;
    
    /**
     * The current evaluation round.
     */
    private final AtomicInteger round = new AtomicInteger(0);

    /**
     * The state associated with each visited vertex.
     * 
     * TODO Offer scalable backend with high throughput, e.g., using a batched
     * striped lock as per DISTINCT (we might be better off with such large
     * visited sets using a full traveral strategy, but overflow to an HTree or
     * (if fixed stride) a MemStore or BigArray could help).
     */
    private final ConcurrentMap<IV, VS> vertexState = new ConcurrentHashMap<IV, VS>();

    /**
     * TODO EDGE STATE: state needs to be configurable. When disabled, leave
     * this as <code>null</code>.
     */
    private final ConcurrentMap<ISPO, ES> edgeState = null;

    GASState(final GASEngine gasEngine, final IGASProgram<VS, ES, ST> program) {

        this.gasEngine = gasEngine;
        
        this.gasProgram = program;
        
        this.vsf = program.getVertexStateFactory();

        this.esf = program.getEdgeStateFactory();

        this.frontier = new StaticFrontier();
        
        /*
         * TODO FRONTIER: Choose thread-local versus CHM implementation using a
         * GASEngine option and then echo in the GASRunner reports.
         */
        if (false) {
            this.scheduler = new SingleThreadScheduler();
        } else if (false) {
            this.scheduler = new CHMScheduler(gasEngine.getNThreads());
        } else {
            this.scheduler = new ThreadLocalScheduler(gasEngine);
        }
        
    }

    /**
     * The current frontier.
     */
    IStaticFrontier frontier() {

        return frontier;

    }

    /**
     * Return the {@link IScheduler}.
     */
    IScheduler getScheduler() {

        return scheduler;

    }

    @Override
    public VS getState(final IV v) {

        VS vs = vertexState.get(v);

        if (vs == null) {

            VS old = vertexState.putIfAbsent(v, vs = vsf.initialValue(v));

            if (old != null) {

                // Lost data race.
                vs = old;

            }

        }

        return vs;

    }

    @Override
    public ES getState(final ISPO e) {

        if (edgeState == null)
            return null;

        ES es = edgeState.get(e);

        if (es == null) {

            ES old = edgeState.putIfAbsent(e, es = esf.initialValue(e));

            if (old != null) {

                // Lost data race.
                es = old;

            }

        }

        return es;

    }

    @Override
    public int round() {

        return round.get();

    }

    @Override
    public void reset() {

        round.set(0);

        vertexState.clear();

        if (edgeState != null)
            edgeState.clear();

        frontier.clear();

    }
    
    @Override
    public void init(final IV... vertices) {

        if (vertices == null)
            throw new IllegalArgumentException();

        reset();

        // Used to ensure that the initial frontier is distinct.
        final Set<IV> tmp = new HashSet<IV>();

        for (IV v : vertices) {

            if (tmp.add(v)) {

                // Put into the current frontier.
                frontier.schedule(v);

                /*
                 * Callback to initialize the vertex state before the first
                 * iteration.
                 */
                gasProgram.init(this, v);

            }

        }

    }

    /**
     * Trace reports on the details of the frontier, verticx state, and edge
     * state.
     * 
     * TODO EDGE STATE: edge state should be traced out also.
     */
    @SuppressWarnings("unchecked")
    void traceState(final AbstractTripleStore kb) {

        if (log.isInfoEnabled())
            log.info("Round=" + round + ", frontierSize=" + frontier().size()
                    + ", vertexStateSize=" + vertexState.size());

        if (!log.isTraceEnabled())
            return;

        // Get all terms in the frontier.
        final Set<IV<?, ?>> tmp = new HashSet((Collection) frontier());

        // Add all IVs for the vertexState.
        tmp.addAll((Collection) vertexState.keySet());

        // Batch resolve all IVs.
        final Map<IV<?, ?>, BigdataValue> m = kb.getLexiconRelation().getTerms(
                tmp);

        log.trace("frontier: size=" + frontier().size());

        for (IV v : frontier()) {

            log.trace("frontier: iv=" + v + " (" + m.get(v) + ")");

        }

        log.trace("vertexState: size=" + vertexState.size());

        for (Map.Entry<IV, VS> e : vertexState.entrySet()) {

            final IV v = e.getKey();

            final BigdataValue val = m.get(v);

            log.trace("vertexState: vertex=" + v + " (" + val + "), state="
                    + e.getValue());

        }

    }

    /**
     * Package private method visits all vertices whose state has been resolved.
     */
    Iterable<IV> getKnownVertices() {

        return vertexState.keySet();

    }

    /**
     * End the current round, advance the round counter, and compact the new
     * frontier.
     */
    void endRound() {
        
        round.incrementAndGet();

        scheduler.compactFrontier(frontier);

        scheduler.clear();
        
    }

    /**
     * Reset the scheduler (this is used to ensure that thread locals are
     * released if we are using a scheduler that uses per-thread data
     * structures).
     */
    void resetScheduler() {
        
        scheduler.clear();
        
    }
    
    /**
     * Simple implementation of a "static" frontier.
     * <p>
     * Note: This implementation has package private methods that permit certain
     * kinds of mutation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class StaticFrontier implements IStaticFrontier {

        private final ArrayList<IV> vertices;

        private StaticFrontier() {

            vertices = new ArrayList<IV>();

        }

        @Override
        public int size() {

            return vertices.size();

        }

        @Override
        public boolean isEmpty() {
            
            return vertices.isEmpty();
            
        }
        
        @Override
        public Iterator<IV> iterator() {

            return vertices.iterator();

        }

        private void ensureCapacity(final int minCapacity) {
            
            vertices.ensureCapacity(minCapacity);
            
        }
        
        private void clear() {
            
            vertices.clear();
            
        }
        
        private void schedule(IV v) {
            
            vertices.add(v);
            
        }

        /**
         * Setup the same static frontier object for the new compact fronter (it
         * is reused in each round).
         */
        void resetFrontier(final IV[] a) {

            // clear the old frontier.
            clear();

            // ensure enough capacity for the new frontier.
            ensureCapacity(a.length);

            for (IV v : a) {

                vertices.add(v);

            }

        }

    }

    /**
     * Extended interface so we can try different implementation strategies.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private interface MyScheduler extends IScheduler {

        /**
         * Compact the schedule into the new frontier.
         * <p>
         * Note: Typical contracts ensure that the frontier is compact (no
         * duplicates) and in ascending {@link IV} order (this provides cache
         * locality for the index reads, even if those reads are against indices
         * wired into RAM).
         */
        void compactFrontier(StaticFrontier frontier);

        /**
         * Reset all internal state (and get rid of any thread locals).
         */
        void clear();
        
    }

    /**
     * Compact a collection of vertices into an ordered frontier.
     * 
     * @param vertices
     *            The collection of vertices for the new frontier.
     * 
     * @return The compact, ordered frontier.
     */
    private static IV[] compactAndSort(final Set<IV> vertices) {

        final IV[] a;

        final int size = vertices.size();

        // TODO FRONTIER: Could reuse this array for each round!
        vertices.toArray(a = new IV[size]);

        /*
         * Order for index access. An ordered scan on a B+Tree is 10X faster
         * than random access lookups.
         * 
         * Note: This uses natural V order, which is also the index order.
         */
        java.util.Arrays.sort(a);

        return a;
        
    }
    
    /**
     * A scheduler suitable for a single thread.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static private class SingleThreadScheduler implements MyScheduler {

        private final Set<IV> vertices;
        
        public SingleThreadScheduler() {

            this.vertices = new LinkedHashSet<IV>();
        
        }
        
        @Override
        public void schedule(final IV v) {
        
            vertices.add(v);
            
        }

        @Override
        public void compactFrontier(final StaticFrontier frontier) {

            frontier.resetFrontier(compactAndSort(vertices));
            
        }

        @Override
        public void clear() {
            
            vertices.clear();
            
        }
        
    }
    
    /**
     * A simple scheduler based on a concurrent hash collection
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static private class CHMScheduler implements MyScheduler {

        // FIXME This is a Jetty class. Unbundle it!  Use CHM instead.
        private final ConcurrentHashSet<IV> vertices;

        public CHMScheduler(final int nthreads) {

            vertices = new ConcurrentHashSet<IV>();

        }

        @Override
        public void schedule(final IV v) {

            vertices.add(v);

        }

        @Override
        public void clear() {
            
            vertices.clear();
            
        }

        @Override
        public void compactFrontier(final StaticFrontier frontier) {

            frontier.resetFrontier(compactAndSort(vertices));
            
        }

    } // CHMScheduler

    /**
     * This scheduler uses thread-local {@link LinkedHashSet}s to track
     * the distinct vertices scheduled by each execution thread.  After
     * the computation round, those per-thread segments of the frontier
     * are combined into a single global, compact, and ordered frontier.
     * To maximize the parallel activity, the per-thread frontiers are
     * sorted using N threads (one per segment). Finally, the frontier
     * segments are combined using a {@link MergeSortIterator} - this is
     * a sequential step with a linear cost in the size of the frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static private class ThreadLocalScheduler implements MyScheduler {

        private final GASEngine gasEngine;
        private final int nthreads;
        private final ConcurrentHashMap<Long/*threadId*/,SingleThreadScheduler> map;
        
        public ThreadLocalScheduler(final GASEngine gasEngine) {

            this.gasEngine = gasEngine;
            
            this.nthreads = gasEngine.getNThreads();
            
            this.map = new ConcurrentHashMap<Long, SingleThreadScheduler>(
                    nthreads/* initialCapacity */, .75f/* loadFactor */,
                    nthreads);

        }
        
        private IScheduler threadLocalScheduler() {

            final Long id = Thread.currentThread().getId();
            
            SingleThreadScheduler s = map.get(id);
            
            if (s == null) {

                final IScheduler old = map.putIfAbsent(id,
                        s = new SingleThreadScheduler());

                if (old != null) {
                    
                    /*
                     * We should not have a key collision since this is based on
                     * the threadId.
                     */
                    
                    throw new AssertionError();
 
                }
                
            }
            
            return s;
            
        }
        
        @Override
        public void schedule(final IV v) {
            
            threadLocalScheduler().schedule(v);
            
        }

        @Override
        public void clear() {

            /*
             * Clear the per-thread maps, but do not discard. They will be
             * reused in the next round.
             */
            for(SingleThreadScheduler s : map.values()) {

                s.clear();
                
            }

//            if (false) {
//                /*
//                 * Note: This should not be required. It is a bit of a paranoid
//                 * step. It could reduce the efficiency by forcing us to
//                 * reallocate the backing data structures. We should keep those
//                 * on hand for the life of the Scheduler, which is linked to the
//                 * execution of the GASProgram.
//                 */
//                map.clear();
//            }
            
        }

        @Override
        public void compactFrontier(final StaticFrontier frontier) {
            
            /*
             * Extract a sorted, compact frontier from each thread local
             * frontier.
             */
            final IV[][] frontiers = new IV[nthreads][];

            int nsources = 0;
            int nvertices = 0;
            {
                final List<Callable<IV[]>> tasks = new ArrayList<Callable<IV[]>>(nthreads);
            
                for (SingleThreadScheduler s : map.values()) {
                    final SingleThreadScheduler t = s;
                    tasks.add(new Callable<IV[]>(){
                        @Override
                        public IV[] call() throws Exception {
                            return compactAndSort(t.vertices);
                        }
                    });
                    
                }
                // invokeAll() - futures will be done() before it returns.
                final List<Future<IV[]>> futures;
                try {
                    futures = gasEngine.getGASThreadPool()
                            .invokeAll(tasks);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                for (Future<IV[]> f : futures) {

                    final IV[] b;
                    try {
                        b = frontiers[nsources] = f.get();
                        nvertices += b.length;
                        nsources++;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                }
            }

            // Clear the new frontier.
            frontier.clear();
            
            if (nsources == 0) {

                // The new frontier is empty.
                return; 

            }
            
            if (nsources > nthreads) {
             
                /*
                 * nsources could be LT nthreads if we have a very small
                 * frontier, but it should never be GTE nthreads.
                 */

                throw new AssertionError("nsources=" + nsources + ", nthreads="
                        + nthreads);

            }
            
            /*
             * Now merge sort those arrays.
             */
            
            // wrap IVs[] as Iterators.
            @SuppressWarnings("unchecked")
            final Iterator<IV>[] itrs = new Iterator[nsources];

            for (int i = 0; i < nsources; i++) {

                itrs[i] = new ArrayIterator<IV>(frontiers[i]);

            }

            // merge sort of those iterators.
            final Iterator<IV> itr = new MergeSortIterator(itrs);
            
            // ensure enough capacity for the new frontier.
            frontier.ensureCapacity(nvertices);

            // and populate the new frontier.
            while (itr.hasNext()) {

                final IV v = itr.next();

                frontier.vertices.add(v);

            }

        }

    }

    /**
     * An N-way merge sort of N source iterators.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class MergeSortIterator implements Iterator<IV> {

        /**
         * The #of source iterators.
         */
        private final int n;

        /**
         * The source iterators in the order given to the ctor.
         */
        private final Iterator<IV>[] sourceIterator;

        /**
         * The current value from each source and <code>null</code> if we need
         * to get another value from that source. The value for a source
         * iterator that has been exhausted will remain <code>null</code>. When
         * all entries in this array are <code>null</code> there are no more
         * values to be visited and we are done.
         */
        private final IV[] sourceTuple;

        /**
         * Index into {@link #sourceIterator} and {@link #sourceTuple} of the
         * iterator whose value will be returned next -or- <code>-1</code> if we
         * need to choose the next value to be visited.
         */
        private int current = -1;

        /**
         * 
         * @param sourceIterators
         *            Each source iterator MUST be in ascending {@link IV}
         *            order.
         */
        public MergeSortIterator(final Iterator<IV>[] sourceIterators) {

            assert sourceIterators != null;

            assert sourceIterators.length > 0;

            this.n = sourceIterators.length;

            for (int i = 0; i < n; i++) {

                assert sourceIterators[i] != null;

            }

            this.sourceIterator = sourceIterators;

            sourceTuple = new IV[n];

        }

        @Override
        public boolean hasNext() {

            /*
             * Until we find an undeleted tuple (or any tuple if DELETED is
             * true).
             */
            while (true) {

                if (current != -1) {

                    if (log.isTraceEnabled())
                        log.trace("Already matched: source=" + current);

                    return true;

                }

                /*
                 * First, make sure that we have a tuple for each source
                 * iterator (unless that iterator is exhausted).
                 */

                int nexhausted = 0;

                for (int i = 0; i < n; i++) {

                    if (sourceTuple[i] == null) {

                        if (sourceIterator[i].hasNext()) {

                            sourceTuple[i] = sourceIterator[i].next();

                            if (log.isTraceEnabled())
                                log.trace("read sourceTuple[" + i + "]="
                                        + sourceTuple[i]);

                        } else {

                            nexhausted++;

                        }

                    }

                }

                if (nexhausted == n) {

                    // the aggregate iterator is exhausted.

                    return false;

                }

                /*
                 * Now consider the current tuple for each source iterator in
                 * turn and choose the _first_ iterator having a tuple whose key
                 * orders LTE all the others (or GTE if [reverseScan == true]).
                 * This is the next tuple to be visited by the aggregate
                 * iterator.
                 */
                {

                    // current is index of the smallest key so far.
                    assert current == -1;

                    IV key = null; // smallest key so far.

                    for (int i = 0; i < n; i++) {

                        if (sourceTuple[i] == null) {

                            // This source is exhausted.

                            continue;

                        }

                        if (current == -1) {

                            current = i;

                            key = sourceTuple[i];

                            assert key != null;

                        } else {

                            final IV tmp = sourceTuple[i];

                            final int ret = IVUtility.compare(tmp, key);

                            if (ret < 0) {

                                /*
                                 * This key orders LT the current key.
                                 * 
                                 * Note: This test MUST be strictly LT since LTE
                                 * would break the precedence in which we are
                                 * processing the source iterators and give us
                                 * the key from the last source by preference
                                 * when we need the key from the first source by
                                 * preference.
                                 */

                                current = i;

                                key = tmp;

                            }

                        }

                    }

                    assert current != -1;

                }

                if (log.isDebugEnabled()) {

                    log.debug("Will visit next: source=" + current
                            + ", tuple: " + sourceTuple[current]);

                }

                return true;

            }

        }

        @Override
        public IV next() {

            if (!hasNext())
                throw new NoSuchElementException();

            return consumeLookaheadTuple();

        }

        /**
         * Consume the {@link #current} source value.
         * 
         * @return The {@link #current} tuple.
         */
        private IV consumeLookaheadTuple() {

            final IV t = sourceTuple[current];

            // clear tuples from other sources having the same key as the
            // current tuple.
            clearCurrent();

            return t;

        }

        /**
         * <p>
         * Clear tuples from other sources having the same key as the current
         * tuple (eliminates duplicates).
         * </p>
         */
        protected void clearCurrent() {

            assert current != -1;

            final IV key = sourceTuple[current];

            for (int i = current + 1; i < n; i++) {

                if (sourceTuple[i] == null) {

                    // this iterator is exhausted.

                    continue;

                }

                final IV tmp = sourceTuple[i];

                final int ret = IVUtility.compare(key, tmp);

                if (ret == 0) {

                    // discard tuple.

                    sourceTuple[i] = null;

                }

            }

            // clear the tuple that we are returning so that we will read
            // another from that source.
            sourceTuple[current] = null;

            // clear so that we will look again.
            current = -1;

        }

        @Override
        public void remove() {

            throw new UnsupportedOperationException();

        }

    } // MergeSortIterator

}
