package com.bigdata.rdf.graph.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.util.GASImplUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

@SuppressWarnings("rawtypes")
public class GASState<VS, ES, ST> implements IGASState<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASState.class);

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
     * @see IStaticFrontier
     * @see IGASSchedulerImpl
     * @see #scheduler
     */
    private final IStaticFrontier frontier;

    /**
     * Used to schedule the new frontier and then compact it onto
     * {@link #frontier} at the end of the round.
     */
    private final IGASSchedulerImpl scheduler;
    
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
    protected final ConcurrentMap<IV, VS> vertexState = new ConcurrentHashMap<IV, VS>();

    /**
     * TODO EDGE STATE: state needs to be configurable. When disabled, leave
     * this as <code>null</code>.
     */
    protected final ConcurrentMap<ISPO, ES> edgeState = null;

    /**
     * Provides access to the backing graph. Used to decode vertices and edges
     * for {@link #traceState()}.
     */
    private IGraphAccessor graphAccessor;

    public GASState(final IGraphAccessor graphAccessor, //
            final IStaticFrontier frontier,//
            final IGASSchedulerImpl gasScheduler,//
            final IGASProgram<VS, ES, ST> gasProgram//
    ) {

        if (graphAccessor == null)
            throw new IllegalArgumentException();

        if (frontier == null)
            throw new IllegalArgumentException();

        if (gasScheduler == null)
            throw new IllegalArgumentException();

        if (gasProgram == null)
            throw new IllegalArgumentException();

        this.graphAccessor = graphAccessor;

        this.gasProgram = gasProgram;

        this.vsf = gasProgram.getVertexStateFactory();

        this.esf = gasProgram.getEdgeStateFactory();

        this.frontier = frontier;
        
        this.scheduler = gasScheduler;
        
    }

    /**
     * Provides access to the backing graph. Used to decode vertices and edges
     * for {@link #traceState()}.
     */
    protected IGraphAccessor getGraphAccessor() {

        return graphAccessor;
        
    }
    
    @Override
    public IStaticFrontier frontier() {

        return frontier;

    }

    @Override
    public IGASSchedulerImpl getScheduler() {

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

        frontier.resetFrontier(0/* minCapacity */, true/* ordered */,
                GASImplUtil.EMPTY_VERTICES_ITERATOR);

    }
    
    @Override
    public void init(final IV... vertices) {

        if (vertices == null)
            throw new IllegalArgumentException();

        reset();

        // Used to ensure that the initial frontier is distinct.
        final Set<IV> tmp = new HashSet<IV>();

        for (IV v : vertices) {
            
            tmp.add(v);
            
            
        }
        
        /*
         * Callback to initialize the vertex state before the first
         * iteration.
         */
        for(IV v : tmp) {
            
            gasProgram.init(this, v);

        }

        // Reset the frontier.
        frontier.resetFrontier(tmp.size()/* minCapacity */, false/* ordered */,
                tmp.iterator());

    }

    @Override
    public void traceState() {

        if (log.isInfoEnabled())
            log.info("Round=" + round + ", frontierSize=" + frontier().size()
                    + ", vertexStateSize=" + vertexState.size());

    }

    @Override
    public void endRound() {
        
        round.incrementAndGet();

        scheduler.compactFrontier(frontier);

        scheduler.clear();
        
    }

    /**
     * {@inheritDoc}
     * 
     * TODO REDUCE : parallelize with nthreads. The reduce operations are often
     * lightweight, so maybe a fork/join pool would work better?
     */
    @Override
    public <T> T reduce(final IReducer<VS, ES, ST, T> op) {

        for (IV v : vertexState.keySet()) {

            op.visit(this, v);

        }

        return op.get();

    }

    @Override
    public String toString(final ISPO e) {

        return e.toString();
        
    }
    
}
