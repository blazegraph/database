package com.bigdata.rdf.graph.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.GASUtil;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;
import com.bigdata.util.concurrent.LatchedExecutor;

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
     */
    static final IElementFilter<ISPO> edgeOnlyFilter = new SPOFilter<ISPO>() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isValid(final Object e) {
            return ((ISPO) e).o().isURI();
        }
    };

    /**
     * The {@link IIndexManager} is used to access the graph.
     */
    private final IIndexManager indexManager;
    
    /**
     * The graph (a KB instance).
     */
    private final String namespace;

    /**
     * The timestamp of the view of that graph. This MAY be
     * {@link ITx#READ_COMMITTED} to use the current committed view of the graph
     * for each iteration (dynamic graph).
     */
    private final long timestamp;

    /**
     * The {@link ExecutorService} used to parallelize tasks.
     */
    private final ExecutorService executorService;
    
    /**
     * The graph analytic to be executed.
     */
    private final IGASProgram<VS, ES, ST> program;

    /**
     * The {@link IGASContext}.
     */
    private final IGASContext<VS, ES, ST> ctx = this;

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

//    @Override
    protected AbstractTripleStore getKB() {

        long timestamp = this.timestamp;
        
        if(timestamp == ITx.READ_COMMITTED) {
        
            /**
             * Note: This code is request the view as of the the last commit
             * time. If we use ITx.READ_COMMITTED here then it will cause the
             * Journal to provide us with a ReadCommittedIndex and that has a
             * synchronization hot spot!
             */

            timestamp = indexManager.getLastCommitTime();
            
        }
        
        final AbstractTripleStore kb = (AbstractTripleStore) indexManager
                .getResourceLocator().locate(namespace, timestamp);

        if (kb == null) {

            throw new RuntimeException("Not found: namespace=" + namespace
                    + ", timestamp=" + TimestampUtility.toString(timestamp));

        }

        return kb;
        
    }

    @Override
    public IGASProgram<VS, ES, ST> getGASProgram() {
        return program;
    }

    @Override
    public IGASContext<VS, ES, ST> getGASContext() {
        return ctx;
    }

    /**
     * The parallelism for the SCATTER and GATHER phases.
     * 
     * TODO CONFIG NCores * f(IOWait). If there is lot of IO Wait, then more
     * threads will provide better throughput. If there is very little IO Wait,
     * then fewer threads might do better. Do a parameterized performance study
     * on some large graphs for a variety of algorithms.
     * 
     * FIXME Why is throughput significantly slower for nparallel=1 (using the
     * latched executor) versus the direct loop over the vertices that I used in
     * the initial implementation?!? Is this just the evaluation environment?
     * Try to code multiple implementations of
     * {@link #doFrontierTask(VertexTaskFactory)} and see if it is the overhead
     * of the latched executor. Try thread pool with just that many threads. Try
     * fork/join. Where is the overhead?
     * 
     * <pre>
     * nparallel BFS time (2nd run) using airbook 2013.08.21 under eclipse: 100 vertices from foaf crawl (500MB data, RWStore - 3 degrees?)
     * 1         11s 
     * 2         14s 
     * 3         12s
     * 4         11s
     * 5         11s
     * 6         11s
     * </pre>
     * <pre>
     * nsamples=100, TestBFS, RWStore, Airbook, 2013.08.21.
     * TOTAL: nrounds=839: fontierSize=500459, ms=9774, edges=1230097, teps=125843 @ nparallel=1
     * TOTAL: nrounds=839: fontierSize=500459, ms=9958, edges=1230097, teps=123523 @ nparallel=4
     * TOTAL: nrounds=839: fontierSize=500459, ms=10834, edges=1230097, teps=113537 @ nparallel=8
     * </pre>
     * Parameterize execution runs againt these runtime options!
     */
    private final int nthreads;

    /**
     * 
     * @param indexManager
     *            The index manager.
     * @param namespace
     *            The namespace of the graph (KB instance).
     * @param timestamp
     *            The timestamp of the graph view (this should be a read-only
     *            view for non-blocking index reads).
     * @param program
     *            The program to execute against that graph.
     * 
     *            TODO Scale-out: The {@link IIndexmanager} MAY be an
     *            {@link IBigdataFederation}. The {@link GASEngine} would
     *            automatically use remote indices. However, for proper
     *            scale-out we want to partition the work and the VS/ES so that
     *            would imply a different {@link IGASEngine} design.
     * 
     *            TODO Dynamic graphs: By allowing {@link ITx#READ_COMMITTED} to
     *            be specified for the timestamp this class provides some
     *            support for dynamic graphs, but for some use cases we would
     *            want to synchronize things such the iteration is performed (or
     *            re-converged) with each commit point or to replay a series of
     *            commit points (either through the commit record index or
     *            through the history index).
     */
    public GASEngine(final IIndexManager indexManager, final String namespace,
            final long timestamp, final IGASProgram<VS, ES, ST> program,
            final int nthreads) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (program == null)
            throw new IllegalArgumentException();

        if (nthreads <= 0)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;
        
        this.namespace = namespace;
        
        this.timestamp = timestamp;
        
        this.program = program;
        
        this.nthreads = nthreads;
        
        this.executorService = indexManager.getExecutorService();
        
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
            program.init(ctx, v);

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
    public IGASStats call() throws Exception {

        final GASStats total = new GASStats();

        while (!frontier().isEmpty()) {

            final GASStats roundStats = new GASStats();

            doRound(roundStats);

            total.add(roundStats);

        }

        if (log.isInfoEnabled())
            log.info("Done: " + total);

        traceState(getKB());

        // Done
        return total;

    }

    /**
     * Trace reports on the details of the frontier, verticx state, and edge
     * state.
     * 
     * TODO edgeState is not being traced out.
     */
    @SuppressWarnings("unchecked")
    private void traceState(final AbstractTripleStore kb) {
        
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
    @Override
    public boolean doRound(final IGASStats stats) throws InterruptedException,
            ExecutionException, Exception {

        /*
         * Obtain a view on the graph.
         * 
         * Note: This will automatically advance if there has been an
         * intervening commit and the caller specified ITx.READ_COMMITTED.
         */
        final AbstractTripleStore kb = getKB();
        
        // The size of the fontier for this round.
        final int frontierSize = frontier().size();
        
        if (log.isInfoEnabled())
            log.info("Round=" + round + ", frontierSize=" + frontierSize
                    + ", vertexStateSize=" + vertexState.size());

        traceState(kb);
        
        /*
         * This is the new frontier. It is initially empty. All newly discovered
         * vertices are inserted into this frontier.
         */
        newFrontier().clear();

        // Compact, ordered frontier. No duplicates!
        final IV[] f = getCompactFrontier();

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
        final EdgesEnum gatherEdges = program.getGatherEdges();
        final EdgesEnum scatterEdges = program.getScatterEdges();
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

        /*
         * GATHER
         */
        
        final long beginGather = System.nanoTime();

        final long gatherEdgeCount;
        if (gatherEdges == EdgesEnum.NoEdges) {

            gatherEdgeCount = 0L;

        } else {

            gatherEdgeCount = gatherEdges(kb, f, gatherEdges,
                    pushDownApplyInGather);
            
        }

        final long elapsedGather = System.nanoTime() - beginGather;

        /*
         * APPLY
         */

        final long elapsedApply;

        if (runApplyStage) {

            final long beginApply = System.nanoTime();

            apply(f);

            elapsedApply = System.nanoTime() - beginApply;
            
        } else {
            
            elapsedApply = 0L;
            
        }

        /*
         * SCATTER
         */
        
        final long beginScatter = System.nanoTime();
 
        final long scatterEdgeCount;
        
        if (scatterEdges == EdgesEnum.NoEdges) {

            scatterEdgeCount = 0L;
            
        } else {

            scatterEdgeCount = scatterEdges(kb, f, scatterEdges,
                    pushDownApplyInScatter);

        }
       
        final long elapsedScatter = System.nanoTime() - beginScatter;

        /*
         * Reporting.
         */

        final long totalElapsed = elapsedGather + elapsedApply + elapsedScatter;

        final long totalEdges = scatterEdgeCount + gatherEdgeCount;

        // TODO pure interface for this.
        ((GASStats) stats).add(frontierSize, totalEdges, totalElapsed);

        if (log.isInfoEnabled()) {

            log.info("\ntotal"//
                    + ": fontierSize=" + frontierSize //
                    + ", ms="+TimeUnit.NANOSECONDS.toMillis(totalElapsed)//
                    + ", edges=" + totalEdges//
                    + ", teps=" + GASUtil.getTEPS(totalEdges, totalElapsed)//
                    + "\ngather"//
                    + ": ms=" + TimeUnit.NANOSECONDS.toMillis(elapsedGather)//
                    + ", nedges=" + gatherEdgeCount//
                    + ", fanIn=" + GASUtil.fanOut(frontierSize, gatherEdgeCount)//
                    + ", teps=" + GASUtil.getTEPS(gatherEdgeCount, elapsedGather) //
                    + (runApplyStage ? ", apply="
                            + TimeUnit.NANOSECONDS.toMillis(elapsedApply) : "")//
                    + "\nscatter"//
                    + ": ms=" + TimeUnit.NANOSECONDS.toMillis(elapsedScatter)//
                    + ", nedges=" + scatterEdgeCount //
                    + ", fanOut=" + GASUtil.fanOut(frontierSize, scatterEdgeCount) //
                    + ", teps=" + GASUtil.getTEPS(scatterEdgeCount, elapsedScatter)//
            );

        }
        
        // Swaps old and new frontiers.
        round.incrementAndGet();

        // True if the new frontier is empty.
        return frontier().isEmpty();
        
    } // doRound()

    /**
     * Generate an ordered frontier to maximize the locality of reference within
     * the indices.
     * 
     * FIXME The frontier should be compacted using parallel threads. For
     * example, we can sort the new frontier within each thread that adds a
     * vertex to be scheduled for the new frontier (in the SCATTER phase). Those
     * per-thread frontiers could then be combined by a merge sort, either using
     * multiple threads (pair-wise) or a single thread (N-way merge). 
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
    private void apply(final IV[] f) {

        for (IV u : f) {

            program.apply(ctx, u, null/* sum */);
            
        }

    }

    private final SPOKeyOrder getKeyOrder(final AbstractTripleStore kb,
            final boolean inEdges) {
        final SPOKeyOrder keyOrder;
        if (inEdges) {
            // in-edges: OSP / OCSP : [u] is the Object.
            keyOrder = kb.isQuads() ? SPOKeyOrder.OCSP : SPOKeyOrder.OSP;
        } else {
            // out-edges: SPO / (SPOC|SOPC) : [u] is the Subject.
            keyOrder = kb.isQuads() ? SPOKeyOrder.SPOC : SPOKeyOrder.SPO;
        }
        return keyOrder;
    }

    @SuppressWarnings("unchecked")
    private Striterator<Iterator<ISPO>,ISPO> getEdges(final AbstractTripleStore kb,
            final boolean inEdges, final IV u) {

        final SPOKeyOrder keyOrder = getKeyOrder(kb, inEdges);
        
        final IIndex ndx = kb.getSPORelation().getIndex(keyOrder);

        final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

        keyBuilder.reset();

        IVUtility.encode(keyBuilder, u);

        final byte[] fromKey = keyBuilder.getKey();

        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

        return (Striterator<Iterator<ISPO>,ISPO>) new Striterator(ndx.rangeIterator(
                fromKey, toKey, 0/* capacity */, IRangeQuery.DEFAULT,
                ElementFilter.newInstance(edgeOnlyFilter)))
                .addFilter(new Resolver() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    protected Object resolve(final Object e) {
                        final ITuple<ISPO> t = (ITuple<ISPO>) e;
                        return t.getObject();
                    }
                });

    }
    
    /**
     * Return the edges for the vertex.
     * 
     * @param u
     *            The vertex.
     * @param edges
     *            Typesafe enumeration indicating which edges should be visited.
     * @return An iterator that will visit the edges for that vertex.
     * 
     *         TODO There should be a means to specify a filter on the possible
     *         predicates to be used for traversal. If there is a single
     *         predicate, then that gives us S+P bound. If there are multiple
     *         predicates, then we have an IElementFilter on P (in addition to
     *         the filter that is removing the Literals from the scan).
     */
    private ICloseableIterator<ISPO> getEdges(final AbstractTripleStore kb,
            final IV u, final EdgesEnum edges) {

        switch (edges) {
        case NoEdges:
            return new EmptyCloseableIterator<ISPO>();
        case InEdges:
            return (ICloseableIterator<ISPO>) getEdges(kb, true/*inEdges*/, u);
        case OutEdges:
            return (ICloseableIterator<ISPO>) getEdges(kb, false/*inEdges*/, u);
        case AllEdges:{
            final Striterator<Iterator<ISPO>,ISPO> a = getEdges(kb, true/*inEdges*/, u);
            final Striterator<Iterator<ISPO>,ISPO> b = getEdges(kb, false/*outEdges*/, u);
            a.append(b);
            return (ICloseableIterator<ISPO>) a;
        }
        default:
            throw new UnsupportedOperationException(edges.name());
        }
        
    }

//    private IChunkedIterator<ISPO> getInEdges(final AbstractTripleStore kb,
//            final IV u) {
//
//        // in-edges: OSP / OCSP : [u] is the Object.
//        return kb
//                .getSPORelation()
//                .getAccessPath(null/* s */, null/* p */, u/* o */, null/* c */,
//                        edgeOnlyFilter).iterator();
//
//    }
//
//    private IChunkedIterator<ISPO> getOutEdges(final AbstractTripleStore kb,
//            final IV u) {
//
//        // out-edges: SPO / SPOC : [u] is the Subject.
//        return kb
//                .getSPORelation()
//                .getAccessPath(u/* s */, null/* p */, null/* o */,
//                        null/* c */, edgeOnlyFilter).iterator();
//
//    }
//    
//    /**
//     * Return the edges for the vertex.
//     * 
//     * @param u
//     *            The vertex.
//     * @param edges
//     *            Typesafe enumeration indicating which edges should be visited.
//     * @return An iterator that will visit the edges for that vertex.
//     * 
//     *         TODO There should be a means to specify a filter on the possible
//     *         predicates to be used for traversal. If there is a single
//     *         predicate, then that gives us S+P bound. If there are multiple
//     *         predicates, then we have an IElementFilter on P (in addition to
//     *         the filter that is removing the Literals from the scan).
//     * 
//     *         TODO Use the chunk parallelism? Explicit for(x : chunk)? This
//     *         could make it easier to collect the edges into an array (but that
//     *         is not required for powergraph).
//     */
//    @SuppressWarnings("unchecked")
//    private IChunkedIterator<ISPO> getEdges(final AbstractTripleStore kb,
//            final IV u, final EdgesEnum edges) {
//
//        switch (edges) {
//        case NoEdges:
//            return new EmptyChunkedIterator<ISPO>(null/* keyOrder */);
//        case InEdges:
//            return getInEdges(kb, u);
//        case OutEdges:
//            return getOutEdges(kb, u);
//        case AllEdges:{
//            final IChunkedIterator<ISPO> a = getInEdges(kb, u);
//            final IChunkedIterator<ISPO> b = getOutEdges(kb, u);
//            final IChunkedIterator<ISPO> c = (IChunkedIterator<ISPO>) new ChunkedStriterator<IChunkedIterator<ISPO>, ISPO>(
//                    a).append(b);
//            return c;
//        }
//        default:
//            throw new UnsupportedOperationException(edges.name());
//        }
//        
//    }
    
    /**
     * A factory for tasks that are applied to each vertex in the frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private interface VertexTaskFactory<T> {

        /**
         * Return a new task that will evaluate the vertex.
         * 
         * @param u
         *            The vertex to be evaluated.
         * 
         * @return The task.
         */
        Callable<T> newVertexTask(IV u);

    }

    /**
     * Abstract base class for a strategy that will map a task across the
     * frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private abstract class AbstractFrontierStrategy implements Callable<Long> {

        final protected VertexTaskFactory<Long> taskFactory;

        AbstractFrontierStrategy(final VertexTaskFactory<Long> taskFactory) {

            this.taskFactory = taskFactory;

        }

    }

    /**
     * Stategy uses the callers thread to map the task across the frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class LatchedExecutorFrontierStrategy extends
            AbstractFrontierStrategy {

        private final ExecutorService executorService;
        private final int nparallel;
        /** Compact, ordered frontier. No duplicates! */
        private final IV[] f;

        LatchedExecutorFrontierStrategy(
                final VertexTaskFactory<Long> taskFactory,
                final ExecutorService executorService, final int nparallel,
                final IV[] f) {

            super(taskFactory);

            this.executorService = executorService;
            
            this.nparallel = nparallel;

            this.f = f;
            
        }

        @Override
        public Long call() throws Exception {

            final List<FutureTask<Long>> tasks = new ArrayList<FutureTask<Long>>(
                    f.length);

            long nedges = 0L;

            final LatchedExecutor e = new LatchedExecutor(executorService,
                    nparallel);

            try {

                // For all vertices in the frontier.
                for (IV u : f) {

                    // Future will compute scatter for vertex.
                    final FutureTask<Long> ft = new FutureTask<Long>(
                            taskFactory.newVertexTask(u));

                    // Add to set of created futures.
                    tasks.add(ft);

                    // Enqueue future for execution.
                    e.execute(ft);

                }

                // Await/check futures.
                for (FutureTask<Long> ft : tasks) {

                    nedges += ft.get();

                }

            } finally {

                // Ensure any error cancels all futures.
                for (FutureTask<Long> ft : tasks) {

                    if (ft != null) {

                        // Cancel Future iff created (ArrayList has nulls).
                        ft.cancel(true/* mayInterruptIfRunning */);

                    }

                }

            }

            return nedges;

        }

    }

    /**
     * Stategy uses the callers thread to map the task across the frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class RunInCallersThreadFrontierStrategy extends
            AbstractFrontierStrategy {

        RunInCallersThreadFrontierStrategy(
                final VertexTaskFactory<Long> taskFactory) {

            super(taskFactory);

        }

        public Long call() throws Exception {

            // Compact, ordered frontier. No duplicates!
            final IV[] f = getCompactFrontier();

            long nedges = 0L;

            // For all vertices in the frontier.
            for (IV u : f) {

                nedges += taskFactory.newVertexTask(u).call();

            }

            return nedges;

        }

    }

    /**
     * Factory for the parallelism strategy that is used to map a task across
     * the frontier.
     * 
     * @param taskFactory
     *            The task to be mapped across the frontier.
     * 
     * @return The strategy that will map that task across the frontier.
     */
    private Callable<Long> newFrontierStrategy(
            final VertexTaskFactory<Long> taskFactory, final IV[] f) {

        if (nthreads == 1)
            return new RunInCallersThreadFrontierStrategy(taskFactory);

        return new LatchedExecutorFrontierStrategy(taskFactory,
                executorService, nthreads, f);

    }

    /**
     * @param inEdges
     *            when <code>true</code> the GATHER is over the in-edges.
     *            Otherwise it is over the out-edges.
     * @param pushDownApply
     *            When <code>true</code>, the APPLY() will be done during the
     *            GATHER.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private long scatterEdges(final AbstractTripleStore kb, final IV[] f,
            final EdgesEnum scatterEdges, final boolean pushDownApply)
            throws InterruptedException, ExecutionException, Exception {

        if (scatterEdges == null)
            throw new IllegalArgumentException();

        class ScatterVertexTaskFactory implements VertexTaskFactory<Long> {

            public Callable<Long> newVertexTask(IV u) {

                return new ScatterTask(kb, u) {
                    @Override
                    protected boolean pushDownApply() {
                        return pushDownApply;
                    }

                    @Override
                    protected EdgesEnum getEdgesEnum() {
                        return scatterEdges;
                    }
                };
            };
        }

        return newFrontierStrategy(new ScatterVertexTaskFactory(), f).call();

    }
    
    /**
     * @param gatherEdges
     *            The edges to be gathered.
     * @param pushDownApply
     *            When <code>true</code>, the APPLY() will be done during the
     *            GATHER.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private long gatherEdges(final AbstractTripleStore kb, final IV[] f,
            final EdgesEnum gatherEdges, final boolean pushDownApply)
            throws InterruptedException, ExecutionException, Exception {

        if (gatherEdges == null)
            throw new IllegalArgumentException();

        class GatherVertexTaskFactory implements VertexTaskFactory<Long> {

            public Callable<Long> newVertexTask(final IV u) {

                return new GatherTask(kb, u) {
                    @Override
                    protected boolean pushDownApply() {
                        return pushDownApply;
                    }

                    @Override
                    protected EdgesEnum getEdgesEnum() {
                        return gatherEdges;
                    }
                };
            };
        }

        return newFrontierStrategy(new GatherVertexTaskFactory(), f).call();

    }

    /**
     * Base class for SCATTER or GATHER of edges for a vertex.
     * <p>
     * Note: An abstract task pattern is used to factor out parameters that are
     * constants within the scope of the scatter for each vertex in the
     * frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract private class VertexEdgesTask implements Callable<Long> {

        protected final AbstractTripleStore kb;
        protected final IV u;

        public VertexEdgesTask(final AbstractTripleStore kb, final IV u) {

            this.kb = kb;
            
            this.u = u;
            
        }

        abstract protected boolean pushDownApply();

        abstract protected EdgesEnum getEdgesEnum();

    }
    
    /**
     * Scatter for the edges of a single vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract private class ScatterTask extends VertexEdgesTask {
        
        public ScatterTask(final AbstractTripleStore kb, final IV u) {

            super(kb, u);
            
        }
        
        /**
         * Execute the scatter for the vertex.
         * 
         * @return The #of visited edges.
         */
        public Long call() throws Exception {

            final boolean TRACE = log.isTraceEnabled();
            
            if (pushDownApply()) {

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
                return 0L;

            }

            /*
             * Visit the (in|out)-edges of that vertex.
             */
            long nedges = 0L;

            final ICloseableIterator<ISPO> eitr = getEdges(kb, u,
                    getEdgesEnum());

            try {

                while (eitr.hasNext()) {

                    // edge
                    final ISPO e = eitr.next();

                    nedges++;

                    if (TRACE) // TODO Batch resolve if @ TRACE
                        log.trace("e=" + kb.toString(e));

                    program.scatter(ctx, u, e);

                }

            } finally {

                eitr.close();

            }

            return nedges;

        }

    } // ScatterTask
    
    /**
     * Gather for the edges of a single vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
     abstract private class GatherTask extends VertexEdgesTask {

        public GatherTask(final AbstractTripleStore kb, final IV u) {

            super(kb, u);
            
        }

        @Override
        public Long call() throws Exception {
            
            long nedges = 0;
            
            final ICloseableIterator<ISPO> eitr = getEdges(kb, u,
                    getEdgesEnum());

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

                if (pushDownApply()) {

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

            return nedges;
            
        }

     } // GatherTask
    
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

        for (IV v : vertexState.keySet()) {

            op.visit(ctx, v);

        }

        return op.get();
        
    }

} // GASEngine
