package com.bigdata.rdf.graph.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.ChunkedStriterator;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * {@link IGASEngine} for dynamic activation of vertices. This implementation
 * maintains a frontier and lazily initializes the vertex state when the vertex
 * is visited for the first time. This is appropriate for algorithms, such as
 * BFS, that use a dynamic frontier.
 * 
 * TODO Algorithms that need to visit all vertices in each round (CC, BC, PR)
 * can be more optimially executed by a different implementation strategy. The
 * vertex state should be arranged in a dense map (maybe an array) and presized.
 * For example, this could be done on the first pass when we identify a vertex
 * index for each distinct V in visitation order.
 * 
 * TODO Vectored expansion with conditional materialization of attribute values
 * could be achieved using CONSTRUCT. This would force URI materialization as
 * well. If we drop down one level, then we can push in the frontier and avoid
 * the materialization. Or we can just write an operator that accepts a frontier
 * and returns the new frontier and which maintains an internal map containing
 * both the visited vertices, the vertex state, and the edge state.
 * 
 * TODO Some computations could be maintained and accelerated. A great example
 * is Shortest Path (as per RDF3X). Reachability queries for a hierarchy can
 * also be maintained and accelerated (again, RDF3X using a ferrari index).
 * 
 * TODO Some of the more interesting questions are how to handle dynamic graphs.
 * This is not yet considered at all by this code.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class GASEngine<VS, ES, ST> implements IGASEngine<VS, ES, ST>,
        IGASContext<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASEngine.class);

    /**
     * Filter visits only edges (filters out attribute values).
     * <p>
     * Note: This filter is pushed down onto the AP and evaluated close to the
     * data.
     * 
     * TODO Lift out as static utility class.
     */
    static final IElementFilter<ISPO> edgeOnlyFilter = new SPOFilter<ISPO>() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isValid(final Object e) {
            return ((ISPO) e).o().isURI();
        }
    };

    /**
     * The KB (aka graph).
     * <p>
     * Note: This COULD be scale-out with remote indices or running embedded
     * inside of a HA server. However, for scale-out we want to partition the
     * work and the VS/ES so that would imply a different {@link IGASEngine}
     * design.
     */
    private final AbstractTripleStore kb;

    /**
     * The graph analytic to be executed.
     */
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
     * The state associated with each visited vertex.
     * 
     * TODO Offer scalable backend with high throughput, e.g., using a batched
     * striped lock as per DISTINCT.
     */
    private final ConcurrentMap<IV, VS> vertexState = new ConcurrentHashMap<IV, VS>();

    /**
     * TODO Edge state needs to be configurable. When disabled, leave this as
     * <code>null</code>.
     */
    private final ConcurrentMap<ISPO, ES> edgeState = null;

    /**
     * The set of vertices that were identified in the current iteration.
     */
    @SuppressWarnings("unchecked")
    private final ConcurrentHashSet<IV>[] frontier = new ConcurrentHashSet[2];

    /**
     * The current evaluation round.
     */
    private final AtomicInteger round = new AtomicInteger(0);

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

    /**
     * The current frontier.
     */
    protected Set<IV> frontier() {

        return frontier[round.get() % 2];

    }

    /**
     * The new frontier - this is populated during the round. At the end of the
     * round, the new frontier replaces the current frontier (this happens when
     * we increment the {@link #round()}). If the current frontier is empty
     * after that replacement, then the traversal is done.
     */
    protected Set<IV> newFrontier() {

        return frontier[(round.get() + 1) % 2];

    }

    @Override
    public int round() {

        return round.get();

    }

    @Override
    public AbstractTripleStore getKB() {
        return kb;
    }

    @Override
    public IGASProgram<VS, ES, ST> getGASProgram() {
        return program;
    }

    @Override
    public IGASContext<VS, ES, ST> getGASContext() {
        return this;
    }

    public GASEngine(final AbstractTripleStore kb,
            final IGASProgram<VS, ES, ST> program) {

        if (kb == null)
            throw new IllegalArgumentException();
        if (program == null)
            throw new IllegalArgumentException();

        this.kb = kb;

        this.program = program;

        this.vsf = program.getVertexStateFactory();

        this.esf = program.getEdgeStateFactory();

        /*
         * TODO set options; setup factory objects; etc.
         * 
         * TODO Could dynamically instantiate the appropriate IGASEngine
         * implementation based on those options and either a factory or
         * delegation pattern.
         */

        this.frontier[0] = new ConcurrentHashSet<IV>();

        this.frontier[1] = new ConcurrentHashSet<IV>();

    }

    @Override
    public void init(final IV... vertices) {

        if (vertices == null)
            throw new IllegalArgumentException();

        reset();
        
        for (IV v : vertices) {

            // Put into the current frontier.
            frontier().add(v);

            /*
             * Callback to initialize the vertex state before the first
             * iteration.
             */
            program.init(getGASContext(), v);

        }

    }
    
    @Override
    public void reset() {

        round.set(0);

        vertexState.clear();
        
        if (edgeState != null)
            edgeState.clear();
        
        frontier().clear();

        newFrontier().clear();

    }

    @Override
    public Void call() throws Exception {

        while (!frontier().isEmpty()) {

            doRound();

        }

        if (log.isInfoEnabled())
            log.info("Done: #rounds=" + round());

        traceState();
        
        return null;

    }

    /**
     * Trace reports on the details of the frontier, verticx state, and edge
     * state.
     * 
     * TODO edgeState is not being traced out.
     */
    @SuppressWarnings("unchecked")
    private void traceState() {
        
        if (!log.isTraceEnabled())
            return;

        // Get all terms in the frontier.
        final Set<IV<?,?>> tmp = new HashSet((Collection) frontier());
        
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
     * {@inheritDoc}
     * 
     * TODO This is an Asynchronous implementation. Further, it does not order
     * the vertices to accelerate converenge (unlike GraphChi) and does not
     * attempt to race ahead to accelerate convergence (unlike an asynchronous
     * neural network).
     * 
     * TODO There should be an option for property value access during the APPLY
     * (either no property values are required, or some (or all) are required
     * and must optionally be materialized. Likewise, there could be an option
     * to force the materialization of the URIs for the (s,p,o).
     * <p>
     * Property value access is on the SPO index. If we are doing a reverse
     * gather (out-edges) then it will be right there and the Apply should be
     * pushed into the Gather. If we are doing a forward gather (in-edges), then
     * we are reading on OSP and will need to do a separate read on SPO.
     */
    public boolean doRound() {

        if (log.isInfoEnabled())
            log.info("Round=" + round + ", frontierSize=" + frontier().size()
                    + ", vertexStateSize=" + vertexState.size());

        traceState();
        
        /*
         * This is the new frontier. It is initially empty. All newly discovered
         * vertices are inserted into this frontier.
         */
        newFrontier().clear();

        final EdgesEnum gatherEdges = program.getGatherEdges();
        final EdgesEnum scatterEdges = program.getScatterEdges();

        /*
         * TODO This logic allows us to push down the APPLY into the GATHER or
         * SCATTER depending on some characteristics of the algorithm. Is this
         * worth while?
         * 
         * TODO The ability to pushd down the APPLY for AllEdges for the GATHER
         * depends on our using the union of the in-edges and out-edges
         * iterators to visit those edges. That union means that we do not have
         * to preserve the accumulant across the in-edges and out-edges aspects
         * of the GATHER. If this UNION over the iterators causes problems with
         * other optimizations, then it could be discarded. Note that this is
         * not an issue for the SCATTER since we can scatter over the in-edges
         * and out-edges for any given vertex independently (so long as the
         * APPLY is done before the SCATTER - this would not work if we pushed
         * down the APPLY into the SCATTER).
         */
        final boolean pushDownApplyInGather;
        final boolean pushDownApplyInScatter;
        final boolean runApplyStage;

        if (scatterEdges == EdgesEnum.NoEdges) {
            // Do APPLY() in GATHER.
            pushDownApplyInGather = true;
            pushDownApplyInScatter = false;
            runApplyStage = false;
        } else if (gatherEdges == EdgesEnum.NoEdges) {
            // APPLY() in SCATTER.
            pushDownApplyInGather = false;
            pushDownApplyInScatter = true;
            runApplyStage = false;
        } else {
            /*
             * Do not push down the APPLY.
             * 
             * TODO We could still push down the apply into the GATHER if we are
             * doing both stages.
             */
            pushDownApplyInGather = false;
            pushDownApplyInScatter = false;
            runApplyStage = true;
        }
        
        gatherEdges(gatherEdges,pushDownApplyInGather);
//        switch (gatherEdges) {
//        case NoEdges:
//            break;
//        case InEdges:
//            gatherEdges(true/*inEdges*/, pushDownApplyInGather);
//            break;
//        case OutEdges:
//            gatherEdges(false/*outEdges*/, pushDownApplyInGather);
//            break;
//        case AllEdges: 
//            /*
//             * TODO When doing the GATHER for both in-edges and out-edges, we
//             * should submit two child GATHER tasks so those things run in
//             * parallel. However, look first at how to parallelize within the
//             * GATHER operation (multiple threads over key-range stripes or
//             * threads racing to consume the frontier in order).
//             * 
//             * TODO The same applies for the SCATTER stage.
//             */ 
//            gatherEdges(true/* inEdges */, pushDownApplyInGather);
//            gatherEdges(false/* outEdges */, pushDownApplyInGather);
//            break;
//        default:
//            throw new UnsupportedOperationException(gatherEdges.name());
//        }

        if(runApplyStage) {
            apply();
        }

        scatterEdges(scatterEdges,pushDownApplyInScatter);
//        switch (scatterEdges) {
//        case NoEdges:
//            break;
//        case OutEdges:
//            scatterEdges(false/*inEdges*/, pushDownApplyInScatter);
//            break;
//        case InEdges:
//            scatterEdges(true/*inEdges*/, pushDownApplyInScatter);
//            break;
//        case AllEdges:
//            scatterEdges(true/* inEdges */, pushDownApplyInScatter);
//            scatterEdges(false/* inEdges */, pushDownApplyInScatter);
//            break;
//        default:
//            throw new UnsupportedOperationException(scatterEdges.name());
//        }

        // Swaps old and new frontiers.
        round.incrementAndGet();

        // True if the new frontier is empty.
        return frontier().isEmpty();
        
    } // doRound()

    /**
     * Generate an ordered frontier to maximize the locality of reference within
     * the indices.
     */
    private IV[] getCompactFrontier() {

        final IV[] f;

        final int size = frontier().size();

        frontier().toArray(f = new IV[size]);

        /*
         * Order for index access. An ordered scan on a B+Tree is 10X faster
         * than random access lookups.
         * 
         * Note: This uses natural V order, which is also the index order.
         */
        java.util.Arrays.sort(f);

        return f;

    }

    /**
     * Do APPLY.
     */
    private void apply() {

        final IGASContext<VS, ES, ST> ctx = getGASContext();

        // Compact, ordered frontier. No duplicates!
        final IV[] f = getCompactFrontier();

        for (IV u : f) {

            program.apply(ctx, u, null/* sum */);
            
        }

    }

    private IChunkedIterator<ISPO> getInEdges(final IV u) {
        
        // in-edges: OSP / OCSP : [u] is the Object.
        return kb
                .getSPORelation()
                .getAccessPath(null/* s */, null/* p */, u/* o */,
                        null/* c */, edgeOnlyFilter).iterator();

    }
    
    private IChunkedIterator<ISPO> getOutEdges(final IV u) {
        
        // out-edges: SPO / SPOC : [u] is the Subject.
        return kb
                .getSPORelation()
                .getAccessPath(u/* s */, null/* p */, null/* o */,
                        null/* c */, edgeOnlyFilter).iterator();

    }
    
    @SuppressWarnings("unchecked")
    private IChunkedIterator<ISPO> getEdges(final IV u, final EdgesEnum edges) {

        switch (edges) {
        case NoEdges:
            return new EmptyChunkedIterator<ISPO>(null/* keyOrder */);
        case InEdges:
            return getInEdges(u);
        case OutEdges:
            return getOutEdges(u);
        case AllEdges:{
            final IChunkedIterator<ISPO> a = getInEdges(u);
            final IChunkedIterator<ISPO> b = getOutEdges(u);
            final IChunkedIterator<ISPO> c = (IChunkedIterator<ISPO>) new ChunkedStriterator<IChunkedIterator<ISPO>, ISPO>(
                    a).append(b);
            return c;
        }
        default:
            throw new UnsupportedOperationException(edges.name());
        }
        
    }
    
    /**
     * @param inEdges
     *            when <code>true</code> the GATHER is over the in-edges.
     *            Otherwise it is over the out-edges.
     * @param pushDownApply
     *            When <code>true</code>, the APPLY() will be done during the
     *            GATHER.
     * 
     * TODO There should be a means to specify a filter on the possible
     * predicates to be used for traversal. If there is a single predicate, then
     * that gives us S+P bound. If there are multiple predicates, then we have
     * an IElementFilter on P (in addition to the filter that is removing the
     * Literals from the scan).
     * 
     * FIXME Striped scan using multiple threads (or implement parallel iterator
     * on B+Tree). This will always do better unless all of the Vs happen to be
     * in the same leaf.
     * 
     * TODO Use the chunk parallelism? Explicit for(x : chunk)? This could make
     * it easier to collect the edges into an array (but that is not required
     * for powergraph).
     */
    private void scatterEdges(final EdgesEnum scatterEdges,
            final boolean pushDownApply) {

        if (scatterEdges == null)
            throw new IllegalArgumentException();

        // Compact, ordered frontier. No duplicates!
        final IV[] f = getCompactFrontier();

        final IGASContext<VS, ES, ST> ctx = getGASContext();

        // For all vertices in the frontier.
        for (IV u : f) {

            if (pushDownApply) {

                /*
                 * Run the APPLY as part of the SCATTER.
                 * 
                 * TODO This can be done on a thread pool or fork/join pool
                 * since we know that there are no duplicates in the frontier.
                 */

                program.apply(ctx, u, null/* sum */);

            }

            if (!program.isChanged(ctx, u)) {

                // Unchanged. Do not scatter.
                continue;

            }
            
            /*
             * Visit the (in|out)-edges of that vertex.
             */
            final IChunkedIterator<ISPO> eitr = getEdges(u, scatterEdges);

            try {

                while (eitr.hasNext()) {

                    // edge
                    final ISPO e = eitr.next();

                    if (log.isTraceEnabled()) // TODO Batch resolve if @ TRACE
                        log.trace("e=" + kb.toString(e));

                    program.scatter(ctx, u, e);

                }

            } finally {

                eitr.close();

            }

        }

    } // scatterOutEdges()

    /**
     * @param gatherEdges
     *            The edges to be gathered.
     * @param pushDownApply
     *            When <code>true</code>, the APPLY() will be done during the
     *            GATHER.
     */
    private void gatherEdges(final EdgesEnum gatherEdges, final boolean pushDownApply) {

        if (gatherEdges == null)
            throw new IllegalArgumentException();

        // Compact, ordered frontier. No duplicates!
        final IV[] f = getCompactFrontier();

        final IGASContext<VS, ES, ST> ctx = getGASContext();

        // For all vertices in the frontier.
        for (IV u : f) {

            final IChunkedIterator<ISPO> eitr = getEdges(u, gatherEdges);

            try {

                /*
                 * Note: since (left,right) may be null, we need to known if
                 * left is defined.
                 */
                boolean first = true;

                ST left = null;

                while (eitr.hasNext()) {

                    final ISPO e = eitr.next();

                    if (log.isTraceEnabled()) // TODO Batch resolve if @ TRACE
                        log.trace("u=" + u + ", e=" + kb.toString(e) + ", sum="
                                + left);

                    final ST right = program.gather(ctx, u, e);

                    if (first) {

                        left = right;

                        first = false;

                    } else {

                        left = program.sum(left, right);

                    }

                }

                // apply() is documented as having a possible null [sum].
//                if (first) {
//
//                    /*
//                     * No in-edges (or no out-edges, as specified)
//                     * 
//                     * TODO The iterator did not visit any edges so the sum is
//                     * null (undefined). Should we call apply anyway in this
//                     * case? If so, document that it needs to handle a [null]
//                     * accumulant!
//                     */
//
//                    continue;
//
//                }

                if (pushDownApply) {

                    /*
                     * Run the APPLY as part of the GATHER.
                     * 
                     * TODO This can be done on a thread pool or fork/join pool
                     * since we know that there are no duplicates in the
                     * frontier.
                     */

                    program.apply(ctx, u, left/* sum */);

                }

            } finally {

                eitr.close();

            }

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This does not add into the current frontier so there is some lost
     * acceleration there when the algorithm would have otherwise permitted
     * that.
     */
    @Override
    public void schedule(final IV v) {

        newFrontier().add(v);

    }

    @Override
    public <T> T reduce(IReducer<VS, ES, ST, T> op) {

        final IGASContext<VS, ES, ST> ctx = getGASContext();

        for (IV v : vertexState.keySet()) {

            op.visit(ctx, v);

        }

        return op.get();
        
    }

} // GASEngine
