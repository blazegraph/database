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

import java.util.*;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;


import com.bigdata.rdf.graph.BinderBase;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.FrontierEnum;
import com.bigdata.rdf.graph.IBinder;
import com.bigdata.rdf.graph.IBindingExtractor;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.impl.util.VertexDistribution;

/**
 * Abstract base class with some useful defaults.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <VS>
 * @param <ES>
 * @param <ST>
 */
abstract public class BaseGASProgram<VS, ES, ST> implements
        IGASProgram<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(BaseGASProgram.class);
    
//    /**
//     * If the vertex is actually an edge, then return the decoded edge.
//     * 
//     * @see GASUtil#decodeStatement(Value)
//     */
//    protected Statement decodeStatement(final Value v) {
//       
//        return GASUtil.decodeStatement(v);
//        
//    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns {@link #getGatherEdges()} and the
     * {@link #getScatterEdges()} if {@link #getGatherEdges()} returns
     * {@value EdgesEnum#NoEdges}. 
     * 
     * TODO This ignores {@link IGASContext#isDirectedTraversal()}
     */
    @Override
    public EdgesEnum getSampleEdgesFilter() {

        // Assume that a GATHER will be done for each starting vertex.
        EdgesEnum edges = getGatherEdges();

        if (edges == EdgesEnum.NoEdges) {

            // If no GATHER is performed, then use the SCATTER edges.
            edges = getScatterEdges();

        }

        return edges;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default gathers on the {@link EdgesEnum#InEdges}.
     */
    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.InEdges;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default scatters on the {@link EdgesEnum#OutEdges}.
     */
    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.OutEdges;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation populates the frontier IFF this is an
     * {@link FrontierEnum#AllVertices} {@link IGASProgram}.
     */
    @Override
    public void before(final IGASContext<VS, ES, ST> ctx) {
        
        switch (getInitialFrontierEnum()) {
            case AllVertices: {
                addAllVerticesToFrontier(ctx);
                break;
            }
            case SampledVertices: {
                addSampledVerticesToFrontier(ctx);
                break;
            }
        }

    }
    
//    /**
//     * {@inheritDoc}
//     * <p>
//     * The default implementation is a NOP.
//     */
//    @Override
//    public <T> IReducer<VS, ES, ST, T> getDefaultAfterOp() {
//    
//        return null; // NOP
//        
//    }
    
    /**
     * Populate the initial frontier using all vertices in the graph.
     * 
     * @param ctx
     *            The graph evaluation context.
     * 
     *            TODO This has a random number generator whose initial seed is
     *            not controlled by the caller. However, the desired use case
     *            here is to produce a distribution over ALL vertices so the
     *            random number should be ignored - perhaps we should pass it in
     *            as <code>null</code>?
     */
    private void addAllVerticesToFrontier(final IGASContext<VS, ES, ST> ctx) {

        final IGASState<VS, ES, ST> gasState = ctx.getGASState();

        final EdgesEnum sampleEdges = getSampleEdgesFilter();

        final VertexDistribution dist = ctx.getGraphAccessor().getDistribution(
                new Random());

        final Resource[] initialFrontier = dist.getAll();

        if (ctx.getLinkType() != null) {
            if (log.isDebugEnabled())
                log.debug("linkType=" + ctx.getLinkType());
            final Set<Resource> filtered = new HashSet<>();
            for (Resource r : initialFrontier) {
                if ( ctx.getGraphAccessor().getEdgeCount(ctx, r, EdgesEnum.InEdges) != 0 ||
                        ctx.getGraphAccessor().getEdgeCount(ctx, r, EdgesEnum.OutEdges) != 0) {
                    filtered.add(r);
                } else {
                    continue;
                }
            }
            final Resource[] filteredFrontier = new Resource[filtered.size()];
            int i = 0;
            for (Resource r : filtered) {
                filteredFrontier[i] = r;
                i++;
            }
            gasState.setFrontier(ctx, filteredFrontier);

            if(log.isDebugEnabled())
                log.debug("initialFrontier=" + Arrays.toString(initialFrontier));

        }  else {

            if (log.isDebugEnabled())
                log.debug("initialFrontier=" + Arrays.toString(initialFrontier));

            gasState.setFrontier(ctx, initialFrontier);
        }


    }

    /**
     * Populate the initial frontier using all vertices in the graph.
     *
     * @param ctx
     *            The graph evaluation context.
     *
     *            TODO This has a random number generator whose initial seed is
     *            not controlled by the caller. However, the desired use case
     *            here is to produce a distribution over ALL vertices so the
     *            random number should be ignored - perhaps we should pass it in
     *            as <code>null</code>?
     */
    private void addSampledVerticesToFrontier(final IGASContext<VS, ES, ST> ctx) {

        final IGASState<VS, ES, ST> gasState = ctx.getGASState();

        final EdgesEnum sampleEdges = getSampleEdgesFilter();

        final VertexDistribution dist = ctx.getGraphAccessor().getDistribution(
                new Random());

        final Resource[] initialFrontier = dist.getUnweightedSample(
                dist.size(), sampleEdges);

        if (log.isDebugEnabled())
            log.debug("initialFrontier=" + Arrays.toString(initialFrontier));

        gasState.setFrontier(ctx, initialFrontier);

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default is a NOP.
     */
    @Override
    public void initVertex(final IGASContext<VS, ES, ST> ctx,
            final IGASState<VS, ES, ST> state, final Value u) {

        // NOP

    }

//    public Factory<Value, VS> getVertexStateFactory();

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns <code>null</code>. Override this if
     * the algorithm uses per-edge computation state.
     */
    @Override
    public Factory<Statement, ES> getEdgeStateFactory() {

        return null;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns <code>true</code>. Override this if
     * you know whether or not the computation state of this vertex has changed.
     */
    @Override
    public boolean isChanged(IGASState<VS, ES, ST> state, Value u) {

        return true;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default returns <code>true</code>.
     */
    @Override
    public boolean nextRound(IGASContext<VS, ES, ST> ctx) {

        return true;

    }

    /**
     * {@inheritDoc}
     * <p>
     * <dl>
     * <dt>{@value Bindings#VISITED}</dt>
     * <dd>The visited vertex itself.</dd>
     * </dl>
     */
    @Override
    public List<IBinder<VS, ES, ST>> getBinderList() {

        final List<IBinder<VS, ES, ST>> tmp = new LinkedList<IBinder<VS, ES, ST>>();

        tmp.add(new BinderBase<VS, ES, ST>() {

            @Override
            public int getIndex() {

                return Bindings.VISITED;

            }

            @Override
            public Value bind(final ValueFactory vf,
                    final IGASState<VS, ES, ST> state, final Value u) {

                return u;

            }

        });

        return tmp;

    }

    /**
     * Interface declares symbolic constants for the {@link IBindingExtractor.IBinder}s reported
     * by {@link BaseGASProgram#getBinderList()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface Bindings {
        
        /**
         * The visited vertex identifier.
         */
        int VISITED = 0;

    }
    
}
