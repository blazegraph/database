/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.TraversalDirectionEnum;
import com.bigdata.rdf.graph.util.GASUtil;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IFilter;

public class GASContext<VS, ES, ST> implements IGASContext<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASContext.class);

    private final GASEngine gasEngine;

    /**
     * Used to access the graph (a KB instance).
     */
    private final IGraphAccessor graphAccessor;

    /**
     * This {@link IGASState}.
     */
    private final IGASState<VS, ES, ST> gasState;

    /**
     * The graph analytic to be executed.
     */
    private final IGASProgram<VS, ES, ST> program;

    /**
     * Whether or not the edges of the graph will be traversed with directed
     * graph semantics (default is {@link TraversalDirectionEnum#Forward}).
     */
    private final AtomicReference<TraversalDirectionEnum> traversalDirection = new AtomicReference<TraversalDirectionEnum>(
            TraversalDirectionEnum.Forward);
    
    /**
     * The maximum number of iterations (defaults to {@link Integer#MAX_VALUE}).
     */
    private final AtomicInteger maxIterations = new AtomicInteger(
            Integer.MAX_VALUE);

    /**
     * The maximum number of vertices (defaults to {@link Integer#MAX_VALUE}).
     */
    private final AtomicInteger maxVertices = new AtomicInteger(
            Integer.MAX_VALUE);

    /**
     * An optional constraint on the type of the visited links.
     */
    private final AtomicReference<URI> linkType = new AtomicReference<URI>(null);
    
    /**
     * An optional constraint on the type of the visited link attributes.
     */
    private final AtomicReference<URI> linkAttributeType = new AtomicReference<URI>(null);
    
    /**
     * An optional {@link IReducer} that will executed after the
     * {@link IGASProgram}.
     */
    private final AtomicReference<IReducer<VS, ES, ST, ?>> afterOp = new AtomicReference<IReducer<VS, ES, ST, ?>>(
            null);

    /**
     * A collection of target vertices for the program to reach.
     */
    private final Set<Value> targetVertices = 
    		Collections.synchronizedSet(new LinkedHashSet<Value>());
    
    /**
     * The maximum number of iterations after the target vertices have been
     * reached. Default behavior is to continue on even after the targets have
     * been reached.
     */
    private final AtomicInteger maxIterationsAfterTargets = new AtomicInteger(
            Integer.MAX_VALUE);
    
    /**
     * 
     * @param namespace
     *            The namespace of the graph (KB instance).
     * @param timestamp
     *            The timestamp of the graph view (this should be a read-only
     *            view for non-blocking index reads).
     * @param gasProgram
     *            The program to execute against that graph.
     */
    public GASContext(final GASEngine gasEngine,
            final IGraphAccessor graphAccessor,
            final IGASState<VS, ES, ST> gasState,
            final IGASProgram<VS, ES, ST> gasProgram) {

        if (gasEngine == null)
            throw new IllegalArgumentException();

        if (graphAccessor == null)
            throw new IllegalArgumentException();
        
        if (gasState == null)
            throw new IllegalArgumentException();

        if (gasProgram == null)
            throw new IllegalArgumentException();

        this.gasEngine = gasEngine;

        this.graphAccessor = graphAccessor;

        this.program = gasProgram;

        this.gasState = gasState;

    }

    @Override
    public IGASState<VS, ES, ST> getGASState() {
        return gasState;
    }

    @Override
    public IGASProgram<VS, ES, ST> getGASProgram() {
        return program;
    }

    @Override
    public IGraphAccessor getGraphAccessor() {
        return graphAccessor;
    }

    @Override
    public IGASStats call() throws Exception {

        final GASStats total = new GASStats();

        program.before(this);
        
		if (log.isTraceEnabled()) {
			log.trace("# of targets: " + targetVertices.size());
			log.trace("max iterations after targets: " + maxIterationsAfterTargets.get());
		}

        while (!gasState.frontier().isEmpty()) {

            /*
             * Check halting conditions.
             * 
             * Note: We could also halt on maxEdges since that is tracked in the
             * GASStats.
             */
            
            if (targetVertices.size() > 0 && 
            		getMaxIterationsAfterTargets() < Integer.MAX_VALUE) {
            	
	            if (gasState.isVisited(targetVertices)) {
	            	
	            	/*
	            	 * If we've reached all target vertices then halt the
	            	 * program N rounds from now where 
	            	 * N = maxIterationsAfterTargets.
	            	 */
	            	synchronized(this.maxIterations) {
	            		
	        			this.maxIterations.set(Math.min(getMaxIterations(),
	    					(int) total.getNRounds() + getMaxIterationsAfterTargets()));
	        			
	            	}
	            	
	            	if (log.isTraceEnabled()) {
	            		log.trace("All targets reached at round " + 
	            				total.getNRounds() + ", halting at round " + 
	            				this.maxIterations.get());
	            	}
	            	
	            }
	            	
            }
            
            if (total.getNRounds() + 1 > getMaxIterations()) {

                log.warn("Halting: maxIterations=" + getMaxIterations()
                        + ", #rounds=" + total.getNRounds());

                break;

            }

            if (total.getFrontierSize() >= getMaxVisited()) {

                log.warn("Halting: maxVertices=" + getMaxVisited()
                        + ", frontierSize=" + total.getFrontierSize());
            
                break;

            }
            
            final GASStats roundStats = new GASStats();

            doRound(roundStats);

            total.add(roundStats);

        }

        if (log.isInfoEnabled())
            log.info("Done: " + total);

        gasState.traceState();

        // Optional post-reduction.
        {
            
            final IReducer<VS, ES, ST, ?> op = getRunAfterOp();

            if (op != null) {

                gasState.reduce(op);

            }

        }

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
    public boolean  doRound(final IGASStats stats) throws InterruptedException,
            ExecutionException, Exception {

        // The fontier for this round.
        final IStaticFrontier f = gasState.frontier();

        // Conditionally log the computation state.
        gasState.traceState();

        /*
         * TODO This logic allows us to push down the APPLY into the GATHER or
         * SCATTER depending on some characteristics of the algorithm. Is this
         * worth while?
         * 
         * Note: The ability to push down the APPLY for AllEdges for the GATHER
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
        final EdgesEnum gatherEdges = getTraversalDirection().asTraversed(
                program.getGatherEdges());
        final EdgesEnum scatterEdges = getTraversalDirection().asTraversed(
                program.getScatterEdges());
        final boolean pushDownApplyInGather;
        final boolean pushDownApplyInScatter;
        final boolean runApplyStage;

        if (gatherEdges != EdgesEnum.NoEdges) {
            // Do APPLY() in GATHER.
            pushDownApplyInGather = true;
            pushDownApplyInScatter = false;
            runApplyStage = false;
        } else if (scatterEdges != EdgesEnum.NoEdges) {
            // APPLY() in SCATTER.
            pushDownApplyInGather = false;
            pushDownApplyInScatter = true;
            runApplyStage = false;
        } else {
            // Do not push down the APPLY.
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

            gatherEdgeCount = gatherEdges(graphAccessor, f, gatherEdges,
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

            scatterEdgeCount = scatterEdges(graphAccessor, f,
                    gasState.getScheduler(), scatterEdges,
                    pushDownApplyInScatter);

        }

        final long elapsedScatter = System.nanoTime() - beginScatter;

        /*
         * Reporting.
         */

        final long totalElapsed = elapsedGather + elapsedApply + elapsedScatter;

        final long totalEdges = scatterEdgeCount + gatherEdgeCount;

        stats.add(f.size(), totalEdges, totalElapsed);

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
        gasState.endRound();
        
        /*
         * Handshake with the GASProgram. If it votes to continue -OR- the new
         * frontier is not empty, then we will do another round.
         */

        final boolean nextRound = program.nextRound(this);

        if(nextRound) {

            /*
             * Optionally advance the view of the graph before the next round.
             */
            graphAccessor.advanceView();
            
        }
        
        return nextRound;
        
    } // doRound()

    /**
     * Do APPLY.
     * 
     * @return The #of vertices for which the operation was executed.
     * 
     * @throws Exception
     */
    private void apply(final IStaticFrontier f) throws Exception {

//      for (Value u : f) {
//
//          program.apply(gasState, u, null/* sum */);
//
//      }

        // Note: Return value of ApplyReducer is currently ignored.
        reduceOverFrontier(f, new ApplyReducer<Void>());
        
    }

    private class ApplyReducer<T> implements IReducer<VS, ES, ST, T> {

        @Override
        public void visit(final IGASState<VS, ES, ST> state, final Value u) {

            program.apply(state, u, null/* sum */);
            
        }

        @Override
        public T get() {

            // Note: Nothing returned right now.
            return null;
            
        }

    }
    
    /**
     * Reduce over the frontier (used for apply()).
     * 
     * @param f
     *            The frontier.
     * @param op
     *            The {@link IReducer}.
     * 
     * @return The {@link IReducer#get() result}.
     * 
     * @throws Exception
     */
    public <T> T reduceOverFrontier(final IStaticFrontier f,
            final IReducer<VS, ES, ST, T> op) throws Exception {

        if (f == null)
            throw new IllegalArgumentException();

        if (op == null)
            throw new IllegalArgumentException();

        class ReduceVertexTaskFactory implements VertexTaskFactory<Long> {

            @Override
            public Callable<Long> newVertexTask(final Value u) {

                return new Callable<Long>() {

                    @Override
                    public Long call() {

                        // program.apply(gasState, u, null/* sum */);
                        op.visit(gasState, u);

                        // Nothing returned by visit().
                        return ONE;

                    };
                };

            };
        }

        gasEngine.newFrontierStrategy(new ReduceVertexTaskFactory(), f).call();

        // Return reduction.
        return op.get();

    }
    private static final Long ONE = Long.valueOf(1L);

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
    private long scatterEdges(final IGraphAccessor graphAccessor,
            final IStaticFrontier f, final IGASScheduler sch,
            final EdgesEnum scatterEdges, final boolean pushDownApply)
            throws InterruptedException, ExecutionException, Exception {

        if (scatterEdges == null)
            throw new IllegalArgumentException();

        class ScatterVertexTaskFactory implements VertexTaskFactory<Long> {

            public Callable<Long> newVertexTask(final Value u) {

                return new ScatterTask(u) {
                    @Override
                    protected boolean pushDownApply() {
                        return pushDownApply;
                    }

                    @Override
                    protected EdgesEnum getEdgesEnum() {
                        return scatterEdges;
                    }

                    @Override
                    protected IGASScheduler scheduler() {
                        return sch;
                    }

                    @Override
                    protected IGraphAccessor graphAccessor() {
                        return graphAccessor;
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
    private long gatherEdges(final IGraphAccessor graphAccessor,
            final IStaticFrontier f, //final IScheduler sch,
            final EdgesEnum gatherEdges, final boolean pushDownApply)
            throws InterruptedException, ExecutionException, Exception {

        if (gatherEdges == null)
            throw new IllegalArgumentException();

        class GatherVertexTaskFactory implements VertexTaskFactory<Long> {

            public Callable<Long> newVertexTask(final Value u) {

                return new GatherTask(u) {
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
                    protected IGASScheduler scheduler() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    protected IGraphAccessor graphAccessor() {
                        return graphAccessor;
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

        protected final Value u;

        public VertexEdgesTask(final Value u) {

            this.u = u;

        }

        abstract protected IGraphAccessor graphAccessor();
        
        abstract protected boolean pushDownApply();

        abstract protected EdgesEnum getEdgesEnum();

        abstract protected IGASScheduler scheduler();

    }

    /**
     * Scatter for the edges of a single vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract private class ScatterTask extends VertexEdgesTask {

        public ScatterTask(final Value u) {

            super(u);

        }

        /**
         * Execute the scatter for the vertex.
         * 
         * @return The #of visited edges.
         */
        @Override
        public Long call() throws Exception {

            final boolean TRACE = log.isTraceEnabled();

            if (pushDownApply()) {

                /*
                 * Run the APPLY as part of the SCATTER.
                 * 
                 * TODO This can be done on a thread pool or fork/join pool
                 * since we know that there are no duplicates in the frontier.
                 */

                program.apply(gasState, u, null/* sum */);

            }

            if (!program.isChanged(gasState, u)) {

                // Unchanged. Do not scatter.
                return 0L;

            }

            /*
             * Visit the (in|out)-edges of that vertex.
             */
            long nedges = 0L;

            final IGASScheduler sch = scheduler();

            final Iterator<Statement> eitr = graphAccessor.getEdges(
                    GASContext.this, u, getEdgesEnum());

            try {

                while (eitr.hasNext()) {

                    // edge
                    final Statement e = eitr.next();

                    nedges++;

                    if (TRACE) // TODO Batch resolve if @ TRACE
                        log.trace("e=" + gasState.toString(e));

                    program.scatter(gasState, sch, u, e);

                }

            } finally {

//                eitr.close();

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

        public GatherTask(final Value u) {

            super(u);

        }

        @Override
        public Long call() throws Exception {

            long nedges = 0;

            final Iterator<Statement> eitr = graphAccessor.getEdges(
                    GASContext.this, u, getEdgesEnum());

            try {

                /*
                 * Note: since (left,right) may be null, we need to known if
                 * left is defined.
                 */
                boolean first = true;

                ST left = null;

                while (eitr.hasNext()) {

                    final Statement e = eitr.next();

                    if (log.isTraceEnabled()) // TODO Batch resolve if @ TRACE
                        log.trace("u=" + u + ", e=" + gasState.toString(e) + ", sum="
                                + left);

                    final ST right = program.gather(gasState, u, e);

                    if (first) {

                        left = right;

                        first = false;

                    } else {

                        left = program.sum(gasState, left, right);

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

                    program.apply(gasState, u, left/* sum */);

                }

            } finally {

//                eitr.close();

            }

            return nedges;

        }

    } // GatherTask

    @Override
    public void setMaxIterations(final int newValue) {

        if (newValue <= 0)
            throw new IllegalArgumentException();
        
        this.maxIterations.set(newValue);
        
    }

    @Override
    public TraversalDirectionEnum getTraversalDirection() {

        return traversalDirection.get();

    }

    @Override
    public void setTraversalDirection(final TraversalDirectionEnum newVal) {

        if (newVal == null)
            throw new IllegalArgumentException();

        traversalDirection.set(newVal);

    }
    
    @Override
    public int getMaxIterations() {

        return maxIterations.get();
        
    }

    @Override
    public void setMaxVisited(int newValue) {

        if (newValue <= 0)
            throw new IllegalArgumentException();
        
        this.maxVertices.set(newValue);
        
    }

    @Override
    public int getMaxVisited() {

        return maxVertices.get();
        
    }

    @Override
    public URI getLinkType() {
        
        return linkType.get();
        
    }

    @Override
    public void setLinkType(final URI linkType) {
        
        this.linkType.set(linkType);
        
    }

    @Override
    public URI getLinkAttributeType() {
        
        return linkAttributeType.get();
        
    }

    @Override
    public void setLinkAttributeType(final URI linkAttributeType) {
        
        this.linkAttributeType.set(linkAttributeType);
        
    }
    
    @Override
    public void setTargetVertices(final Value[] targetVertices) {
    	
    	this.targetVertices.addAll(Arrays.asList(targetVertices));
    	
    }

    @Override
    public Set<Value> getTargetVertices() {
    	
    	return this.targetVertices;
    	
    }
    
    @Override
    public void setMaxIterationsAfterTargets(final int newValue) {

        if (newValue < 0)
            throw new IllegalArgumentException();
        
        this.maxIterationsAfterTargets.set(newValue);
        
    }

    @Override
    public int getMaxIterationsAfterTargets() {

        return maxIterationsAfterTargets.get();
        
    }



//    /**
//     * {@inheritDoc}
//     * <p>
//     * The default implementation only visits the edges.
//     */
//    @Override
//    public IStriterator getConstrainEdgeFilter(final IStriterator itr) {
//
//        return itr.addFilter(getEdgeOnlyFilter());
//
//    }

//    /**
//     * Return an {@link IFilter} that will only visit the edges of the graph.
//     * 
//     * @see IGASState#isEdge(Statement)
//     */
//    protected IFilter getEdgeOnlyFilter() {
//
//        return new EdgeOnlyFilter(this);
//        
//    }
//    
//    /**
//     * Filter visits only edges (filters out attribute values).
//     * <p>
//     * Note: This filter is pushed down onto the AP and evaluated close to the
//     * data.
//     */
//    private class EdgeOnlyFilter extends Filter {
//        private static final long serialVersionUID = 1L;
//        private final IGASState<VS, ES, ST> gasState;
//        private EdgeOnlyFilter(final IGASContext<VS, ES, ST> ctx) {
//            this.gasState = ctx.getGASState();
//        }
//        @Override
//        public boolean isValid(final Object e) {
//            return gasState.isEdge((Statement) e);
//        }
//    };
    
    /**
     * Return a filter that only visits the edges of graph that are instances of
     * the specified link attribute type.
     * <p>
     * Note: For bigdata, the visited edges can be decoded to recover the
     * original link as well. 
     * 
     * @see IGASState#isLinkAttrib(Statement, URI)
     * @see IGASState#decodeStatement(Value)
     */
    protected IFilter getLinkAttribFilter(final IGASContext<VS, ES, ST> ctx,
            final URI linkAttribType) {

        return new LinkAttribFilter(ctx, linkAttribType);

    }

    /**
     * Filter visits only edges where the {@link Statement} is an instance of
     * the specified link attribute type. For bigdata, the visited edges can be
     * decoded to recover the original link as well.
     */
    private class LinkAttribFilter extends Filter {
        private static final long serialVersionUID = 1L;

        private final IGASState<VS, ES, ST> gasState;
        private final URI linkAttribType;
        
        public LinkAttribFilter(final IGASContext<VS, ES, ST> ctx,
                final URI linkAttribType) {
            if (linkAttribType == null)
                throw new IllegalArgumentException();
            this.gasState = ctx.getGASState();
            this.linkAttribType = linkAttribType;
        }

        @Override
        public boolean isValid(final Object e) {
            return gasState.isLinkAttrib((Statement) e, linkAttribType);
        }
    }

    @Override
    public <T> void setRunAfterOp(final IReducer<VS, ES, ST, T> afterOp) {

        this.afterOp.set(afterOp);
        
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> IReducer<VS, ES, ST, T> getRunAfterOp() {

        return (IReducer<VS, ES, ST, T>) afterOp.get();

    }
    
} // GASContext
