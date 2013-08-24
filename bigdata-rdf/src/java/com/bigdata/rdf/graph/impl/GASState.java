package com.bigdata.rdf.graph.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

@SuppressWarnings("rawtypes")
public class GASState<VS, ES, ST> implements IGASState<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASState.class);

    private final IGASProgram<VS, ES, ST> program;
    
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
     * @see NextFrontier
     * @see #nextFrontier
     */
    private final StaticFrontier frontier;

    /**
     * Used to schedule the new frontier and then compact it onto
     * {@link #frontier} at the end of the round.
     */
    private final NextFrontier nextFrontier;
    
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

        this.program = program;
        
        this.vsf = program.getVertexStateFactory();

        this.esf = program.getEdgeStateFactory();

        this.frontier = new StaticFrontier();
        
        this.nextFrontier = new NextFrontier(gasEngine.getNThreads());
        
    }

    /**
     * The current frontier.
     */
    IStaticFrontier frontier() {

        return frontier;

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
                frontier.add(v);

                /*
                 * Callback to initialize the vertex state before the first
                 * iteration.
                 */
                program.init(this, v);

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

        nextFrontier.compactFrontier();

        nextFrontier.clear();
        
    }

    /**
     * Return the {@link IScheduler}.
     */
    IScheduler getScheduler() {

        return nextFrontier;

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
        
        private void add(IV v) {
            
            vertices.add(v);
            
        }
        
    }

    /**
     * FIXME FRONTIER: Implement a variation on this that uses per-thread
     * LinkedHashSets for the new frontier, sorts in each thread, and then does
     * an N-way merge sort to produce the new compact frontier. This will
     * require either thread locals (in which case we need to avoid leaking out
     * those resources) or an explicit threadId concept that is coordinated with
     * the GASEngine.
     * 
     * TODO Add option to order the vertices to provide a serializable execution
     * plan (like GraphChi). I believe that this reduces to computing a DAG over
     * the frontier before executing the GATHER and then executing the frontier
     * such that the parallel execution is constrained by arcs in the DAG that
     * do not have mutual dependencies.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    class NextFrontier implements IScheduler {

        private final ConcurrentHashSet<IV> f;

        public NextFrontier(final int nthreads) {

            f = new ConcurrentHashSet<IV>();

        }

        @Override
        public void schedule(final IV v) {

            f.add(v);

        }

        private void clear() {
            
            f.clear();
            
        }
        
        /**
         * Convert the frontier into a representation suitable for the next
         * round of processing.
         * <p>
         * Note: Typical contracts ensure that the frontier is compact (no
         * duplicates) and in ascending {@link IV} order (this provides cache
         * locality for the index reads, even if those reads are against indices
         * wired into RAM).
         * <p>
         * This implementation generates an ordered frontier to maximize the
         * locality of reference within the indices.
         * 
         * FIXME FRONTIER: The frontier should be compacted using parallel
         * threads. For example, we can sort the new frontier within each thread
         * that adds a vertex to be scheduled for the new frontier (in the
         * SCATTER phase). Those per-thread frontiers could then be combined by
         * a merge sort, either using multiple threads (pair-wise) or a single
         * thread (N-way merge).
         * 
         * 2/3rds of the time is CHM.toArray(). 1/3 is the sort.
         * 
         * TODO FRONTIER: Find a parallel sort that we can use for java. This is
         * not required if we do a sort within each SCATTER thread and then a
         * merge sort across the per-thread compact, ordered frontiers. It is
         * required if we defer the sort until we have combined those per-thread
         * frontiers into a global frontier.
         */
        public IStaticFrontier compactFrontier() {

            final IV[] a;

            final int size = this.f.size();

            this.f.toArray(a = new IV[size]);

            /*
             * Order for index access. An ordered scan on a B+Tree is 10X faster
             * than random access lookups.
             * 
             * Note: This uses natural V order, which is also the index order.
             */
            java.util.Arrays.sort(a);

            /*
             * Setup the same static frontier object for the new compact fronter
             * (it is reused in each round).
             */

            // clear the old frontier.
            frontier.clear();

            // ensure enough capacity for the new frontier.
            frontier.ensureCapacity(a.length);

            for (IV v : a) {

                frontier.vertices.add(v);

            }
            
            return frontier;

        }

    } // NextFrontier
    
}
