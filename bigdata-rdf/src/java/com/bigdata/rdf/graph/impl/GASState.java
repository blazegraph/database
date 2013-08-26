package com.bigdata.rdf.graph.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;

import cutthecrap.utils.striterators.ArrayIterator;

@SuppressWarnings("rawtypes")
public class GASState<VS, ES, ST> implements IGASState<VS, ES, ST> {

    private final Logger log = Logger.getLogger(GASState.class);

//    /**
//     * The {@link GASEngine} on which the {@link IGASProgram} will be run.
//     */
//    private final GASEngine gasEngine;

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

    GASState(final GASEngine gasEngine,
            final GASContext<VS, ES, ST> gasContext,
            final IGASProgram<VS, ES, ST> program) {

//        this.gasEngine = gasEngine;
        
        this.gasProgram = program;
        
        this.vsf = program.getVertexStateFactory();

        this.esf = program.getEdgeStateFactory();

        this.frontier = new StaticFrontier();
        
        this.scheduler = (MyScheduler) gasEngine.newScheduler(gasContext);
        
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
    interface MyScheduler extends IScheduler {

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
    static class STScheduler implements MyScheduler {

        private final Set<IV> vertices;
        
        public STScheduler(final GASEngine gasEngine) {

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
     * 
     *         FIXME SCHEDULER: This is a Jetty class. Unbundle it! Use CHM
     *         instead. See {@link CHMScheduler}.
     */
    static class CHSScheduler implements MyScheduler {

        private final ConcurrentHashSet<IV> vertices;

        public CHSScheduler(final GASEngine gasEngine) {

            vertices = new ConcurrentHashSet<IV>(/* TODO nthreads (CHM) */);

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
     * A simple scheduler based on a {@link ConcurrentHashMap}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static class CHMScheduler implements MyScheduler {

        private final ConcurrentHashMap<IV,IV> vertices;

        public CHMScheduler(final GASEngine gasEngine) {

            vertices = new ConcurrentHashMap<IV,IV>(gasEngine.getNThreads());

        }

        @Override
        public void schedule(final IV v) {

            vertices.putIfAbsent(v,v);

        }

        @Override
        public void clear() {
            
            vertices.clear();
            
        }

        @Override
        public void compactFrontier(final StaticFrontier frontier) {

            frontier.resetFrontier(compactAndSort(vertices.keySet()));
            
        }

    } // CHMScheduler

    /**
     * This scheduler uses thread-local buffers ({@link LinkedHashSet}) to track
     * the distinct vertices scheduled by each execution thread. After the
     * computation round, those per-thread segments of the frontier are combined
     * into a single global, compact, and ordered frontier. To maximize the
     * parallel activity, the per-thread frontiers are sorted using N threads
     * (one per segment). Finally, the frontier segments are combined using a
     * {@link MergeSortIterator} - this is a sequential step with a linear cost
     * in the size of the frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static class TLScheduler implements MyScheduler {

        private final GASEngine gasEngine;
        private final int nthreads;
        private final ConcurrentHashMap<Long/*threadId*/,STScheduler> map;
        
        public TLScheduler(final GASEngine gasEngine) {

            this.gasEngine = gasEngine;
            
            this.nthreads = gasEngine.getNThreads();
            
            this.map = new ConcurrentHashMap<Long, STScheduler>(
                    nthreads/* initialCapacity */, .75f/* loadFactor */,
                    nthreads);

        }
        
        private IScheduler threadLocalScheduler() {

            final Long id = Thread.currentThread().getId();
            
            STScheduler s = map.get(id);
            
            if (s == null) {

                final IScheduler old = map.putIfAbsent(id, s = new STScheduler(
                        gasEngine));

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
            for(STScheduler s : map.values()) {

                s.clear();
                
            }
      
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
            
                for (STScheduler s : map.values()) {
                    final STScheduler t = s;
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
             * Now merge sort those arrays and populate the new frontier.
             */
            mergeSortSourcesAndSetFrontier(nsources, nvertices, frontiers,
                    frontier);

        }

        /**
         * Now merge sort the ordered frontier segments and populate the new
         * frontier.
         * 
         * @param nsources
         *            The #of frontier segments.
         * @param nvertices
         *            The total #of vertice across those segments (may
         *            double-count across segments).
         * @param frontiers
         *            The ordered, compact frontier segments
         * @param frontier
         *            The new frontier to be populated.
         */
        private void mergeSortSourcesAndSetFrontier(final int nsources,
                final int nvertices, final IV[][] frontiers,
                final StaticFrontier frontier) {

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

}
