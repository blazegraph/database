package com.bigdata.rdf.graph.impl;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.GASUtil;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.IScheduler;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;

@SuppressWarnings("rawtypes")
public class GASContext<VS, ES, ST> implements IGASContext<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASContext.class);

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

    private final GASEngine gasEngine;

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
     * This {@link IGASState}.
     */
    private final GASState<VS, ES, ST> state;

    /**
     * The graph analytic to be executed.
     */
    private final IGASProgram<VS, ES, ST> program;

    /**
     * 
     * @param namespace
     *            The namespace of the graph (KB instance).
     * @param timestamp
     *            The timestamp of the graph view (this should be a read-only
     *            view for non-blocking index reads).
     * @param program
     *            The program to execute against that graph.
     */
    public GASContext(final GASEngine gasEngine, final String namespace,
            final long timestamp, final IGASProgram<VS, ES, ST> program) {

        if (gasEngine == null)
            throw new IllegalArgumentException();

        if (program == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        this.gasEngine = gasEngine;

        this.namespace = namespace;

        this.timestamp = timestamp;

        this.program = program;

        this.state = new GASState<VS, ES, ST>(gasEngine, this, program);

    }

    @Override
    public IGASState<VS, ES, ST> getGASState() {
        return state;
    }

    @Override
    public IGASProgram<VS, ES, ST> getGASProgram() {
        return program;
    }

    @Override
    public IGASStats call() throws Exception {

        final GASStats total = new GASStats();

        while (!state.frontier().isEmpty()) {

            final GASStats roundStats = new GASStats();

            doRound(roundStats);

            total.add(roundStats);

        }

        if (log.isInfoEnabled())
            log.info("Done: " + total);

        state.traceState(gasEngine.getKB(namespace, timestamp));

        // Done
        return total;

    }

    /**
     * {@inheritDoc}
     * 
     * TODO This is an Asynchronous implementation. Further, it does not attempt
     * to race ahead to accelerate convergence (unlike an asynchronous neural
     * network).
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
         * This is the new frontier. It is initially empty. All newly
         * discovered vertices are inserted into this frontier.
         * 
         * TODO This assumes that only SCATTER can schedule new vertices. If
         * we also permit scheduling during GATHER (or APPLY), then that
         * will require us to communicate about the new frontier during
         * operations other than SCATTER. On a cluster, the communication
         * overhead is real. On a single machine, it is completely
         * artificial. (Some GAS programs visit all vertices in every round
         * and thus do not use a scheduler at all and would not need to
         * implement a SCATTER phase, at least, not to schedule vertices.)
         */

        final IScheduler sch = state.getScheduler();

        try {

            return _doRound(stats, sch);
            
        } finally {
            
            // Ensure that thread-locals are released.
            state.resetScheduler();
            
        }
        
        
    }
    
    private boolean _doRound(final IGASStats stats, final IScheduler sch)
            throws InterruptedException, ExecutionException, Exception {

        /*
         * Obtain a view on the graph.
         * 
         * Note: This will automatically advance if there has been an
         * intervening commit and the caller specified ITx.READ_COMMITTED.
         */
        final AbstractTripleStore kb = gasEngine.getKB(namespace, timestamp);

        // The fontier for this round.
        final IStaticFrontier f = state.frontier();

        state.traceState(kb);

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

            scatterEdgeCount = scatterEdges(kb, f, sch, scatterEdges,
                    pushDownApplyInScatter);

        }

        final long elapsedScatter = System.nanoTime() - beginScatter;

        /*
         * Reporting.
         */

        final long totalElapsed = elapsedGather + elapsedApply + elapsedScatter;

        final long totalEdges = scatterEdgeCount + gatherEdgeCount;

        // TODO pure interface for this.
        ((GASStats) stats).add(f.size(), totalEdges, totalElapsed);

        if (log.isInfoEnabled()) {

            log.info("\ntotal"//
                    + ": fontierSize="
                    + f.size() //
                    + ", ms="
                    + TimeUnit.NANOSECONDS.toMillis(totalElapsed)//
                    + ", edges="
                    + totalEdges//
                    + ", teps="
                    + GASUtil.getTEPS(totalEdges, totalElapsed)//
                    + "\ngather"//
                    + ": ms="
                    + TimeUnit.NANOSECONDS.toMillis(elapsedGather)//
                    + ", nedges="
                    + gatherEdgeCount//
                    + ", fanIn="
                    + GASUtil.fanOut(f.size(), gatherEdgeCount)//
                    + ", teps="
                    + GASUtil.getTEPS(gatherEdgeCount, elapsedGather) //
                    + (runApplyStage ? ", apply="
                            + TimeUnit.NANOSECONDS.toMillis(elapsedApply) : "")//
                    + "\nscatter"//
                    + ": ms="
                    + TimeUnit.NANOSECONDS.toMillis(elapsedScatter)//
                    + ", nedges="
                    + scatterEdgeCount //
                    + ", fanOut="
                    + GASUtil.fanOut(f.size(), scatterEdgeCount) //
                    + ", teps="
                    + GASUtil.getTEPS(scatterEdgeCount, elapsedScatter)//
            );

        }

        // End the round, advance the counter, and compact new frontier.
        state.endRound();
        
        // True if the new frontier is empty.
        return state.frontier().isEmpty();

    } // doRound()

    /**
     * Do APPLY.
     * 
     * TODO The apply() should be parallelized. For some algorithms, there is a
     * moderate amount of work per vertex in apply(). Use {@link #nthreads} to
     * set the parallelism.
     */
    private void apply(final IStaticFrontier f) {

        for (IV u : f) {

            program.apply(state, u, null/* sum */);

        }

    }

    static private final SPOKeyOrder getKeyOrder(final AbstractTripleStore kb,
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
    static private Striterator<Iterator<ISPO>, ISPO> getEdges(
            final AbstractTripleStore kb, final boolean inEdges, final IV u) {

        final SPOKeyOrder keyOrder = getKeyOrder(kb, inEdges);

        final IIndex ndx = kb.getSPORelation().getIndex(keyOrder);

        final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

        keyBuilder.reset();

        IVUtility.encode(keyBuilder, u);

        final byte[] fromKey = keyBuilder.getKey();

        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

        return (Striterator<Iterator<ISPO>, ISPO>) new Striterator(
                ndx.rangeIterator(fromKey, toKey, 0/* capacity */,
                        IRangeQuery.DEFAULT,
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
    static private ICloseableIterator<ISPO> getEdges(
            final AbstractTripleStore kb, final IV u, final EdgesEnum edges) {

        switch (edges) {
        case NoEdges:
            return new EmptyCloseableIterator<ISPO>();
        case InEdges:
            return (ICloseableIterator<ISPO>) getEdges(kb, true/* inEdges */, u);
        case OutEdges:
            return (ICloseableIterator<ISPO>) getEdges(kb, false/* inEdges */,
                    u);
        case AllEdges: {
            final Striterator<Iterator<ISPO>, ISPO> a = getEdges(kb,
                    true/* inEdges */, u);
            final Striterator<Iterator<ISPO>, ISPO> b = getEdges(kb,
                    false/* outEdges */, u);
            a.append(b);
            return (ICloseableIterator<ISPO>) a;
        }
        default:
            throw new UnsupportedOperationException(edges.name());
        }

    }

    // private IChunkedIterator<ISPO> getInEdges(final AbstractTripleStore kb,
    // final IV u) {
    //
    // // in-edges: OSP / OCSP : [u] is the Object.
    // return kb
    // .getSPORelation()
    // .getAccessPath(null/* s */, null/* p */, u/* o */, null/* c */,
    // edgeOnlyFilter).iterator();
    //
    // }
    //
    // private IChunkedIterator<ISPO> getOutEdges(final AbstractTripleStore kb,
    // final IV u) {
    //
    // // out-edges: SPO / SPOC : [u] is the Subject.
    // return kb
    // .getSPORelation()
    // .getAccessPath(u/* s */, null/* p */, null/* o */,
    // null/* c */, edgeOnlyFilter).iterator();
    //
    // }
    //
    // /**
    // * Return the edges for the vertex.
    // *
    // * @param u
    // * The vertex.
    // * @param edges
    // * Typesafe enumeration indicating which edges should be visited.
    // * @return An iterator that will visit the edges for that vertex.
    // *
    // * TODO There should be a means to specify a filter on the possible
    // * predicates to be used for traversal. If there is a single
    // * predicate, then that gives us S+P bound. If there are multiple
    // * predicates, then we have an IElementFilter on P (in addition to
    // * the filter that is removing the Literals from the scan).
    // *
    // * TODO Use the chunk parallelism? Explicit for(x : chunk)? This
    // * could make it easier to collect the edges into an array (but that
    // * is not required for powergraph).
    // */
    // @SuppressWarnings("unchecked")
    // private IChunkedIterator<ISPO> getEdges(final AbstractTripleStore kb,
    // final IV u, final EdgesEnum edges) {
    //
    // switch (edges) {
    // case NoEdges:
    // return new EmptyChunkedIterator<ISPO>(null/* keyOrder */);
    // case InEdges:
    // return getInEdges(kb, u);
    // case OutEdges:
    // return getOutEdges(kb, u);
    // case AllEdges:{
    // final IChunkedIterator<ISPO> a = getInEdges(kb, u);
    // final IChunkedIterator<ISPO> b = getOutEdges(kb, u);
    // final IChunkedIterator<ISPO> c = (IChunkedIterator<ISPO>) new
    // ChunkedStriterator<IChunkedIterator<ISPO>, ISPO>(
    // a).append(b);
    // return c;
    // }
    // default:
    // throw new UnsupportedOperationException(edges.name());
    // }
    //
    // }

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
    private long scatterEdges(final AbstractTripleStore kb,
            final IStaticFrontier f, final IScheduler sch,
            final EdgesEnum scatterEdges, final boolean pushDownApply)
            throws InterruptedException, ExecutionException, Exception {

        if (scatterEdges == null)
            throw new IllegalArgumentException();

        class ScatterVertexTaskFactory implements VertexTaskFactory<Long> {

            public Callable<Long> newVertexTask(final IV u) {

                return new ScatterTask(kb, u) {
                    @Override
                    protected boolean pushDownApply() {
                        return pushDownApply;
                    }

                    @Override
                    protected EdgesEnum getEdgesEnum() {
                        return scatterEdges;
                    }

                    @Override
                    protected IScheduler scheduler() {
                        return sch;
                    }
                };
            };
        }

        return gasEngine.newFrontierStrategy(new ScatterVertexTaskFactory(), f)
                .call();

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
    private long gatherEdges(final AbstractTripleStore kb,
            final IStaticFrontier f, //final IScheduler sch,
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

                    /**
                     * Note: The API does not permit vertices to be scheduled
                     * for execution during the GATHER phase.
                     */
                    @Override
                    protected IScheduler scheduler() {
                        throw new UnsupportedOperationException();
                    }
                };
            };
        }

        return gasEngine.newFrontierStrategy(new GatherVertexTaskFactory(), f)
                .call();

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

        abstract protected IScheduler scheduler();

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

                program.apply(state, u, null/* sum */);

            }

            if (!program.isChanged(state, u)) {

                // Unchanged. Do not scatter.
                return 0L;

            }

            /*
             * Visit the (in|out)-edges of that vertex.
             */
            long nedges = 0L;

            final IScheduler sch = scheduler();

            final ICloseableIterator<ISPO> eitr = getEdges(kb, u,
                    getEdgesEnum());

            try {

                while (eitr.hasNext()) {

                    // edge
                    final ISPO e = eitr.next();

                    nedges++;

                    if (TRACE) // TODO Batch resolve if @ TRACE
                        log.trace("e=" + kb.toString(e));

                    program.scatter(state, sch, u, e);

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

                    final ST right = program.gather(state, u, e);

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

                    program.apply(state, u, left/* sum */);

                }

            } finally {

                eitr.close();

            }

            return nedges;

        }

    } // GatherTask

    // TODO REDUCE : parallelize with nthreads.
    @Override
    public <T> T reduce(final IReducer<VS, ES, ST, T> op) {

        for (IV v : state.getKnownVertices()) {

            op.visit(state, v);

        }

        return op.get();

    }

} // GASContext
