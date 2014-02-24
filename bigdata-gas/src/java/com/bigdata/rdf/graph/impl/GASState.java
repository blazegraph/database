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

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;

import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.util.GASUtil;

public class GASState<VS, ES, ST> implements IGASState<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASState.class);

    // /**
    // * The {@link GASEngine} on which the {@link IGASProgram} will be run.
    // */
    // private final GASEngine gasEngine;

    /**
     * When <code>true</code> the frontier will be sorted.
     */
    private final boolean sortFrontier;
    
    /**
     * The {@link IGASProgram} to be run.
     */
    private final IGASProgram<VS, ES, ST> gasProgram;

    /**
     * Factory for the vertex state objects.
     */
    private final Factory<Value, VS> vsf;

    /**
     * Factory for the edge state objects.
     */
    private final Factory<Statement, ES> esf;

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
    protected final ConcurrentMap<Value, VS> vertexState = new ConcurrentHashMap<Value, VS>();

    /**
     * TODO EDGE STATE: state needs to be configurable. When disabled, leave
     * this as <code>null</code>.
     */
    protected final ConcurrentMap<Statement, ES> edgeState = null;

    /**
     * Provides access to the backing graph. Used to decode vertices and edges
     * for {@link #traceState()}.
     */
    private final IGraphAccessor graphAccessor;

    /**
     * Used to establish a total ordering over RDF {@link Value}s.
     */
    private final Comparator<Value> valueComparator;
    
    public GASState(final IGASEngine gasEngine,//
            final IGraphAccessor graphAccessor, //
            final IStaticFrontier frontier,//
            final IGASSchedulerImpl gasScheduler,//
            final IGASProgram<VS, ES, ST> gasProgram//
    ) {

        if (gasEngine == null)
            throw new IllegalArgumentException();

        if (graphAccessor == null)
            throw new IllegalArgumentException();

        if (frontier == null)
            throw new IllegalArgumentException();

        if (gasScheduler == null)
            throw new IllegalArgumentException();

        if (gasProgram == null)
            throw new IllegalArgumentException();

        this.sortFrontier = gasEngine.getSortFrontier();
        
        this.graphAccessor = graphAccessor;

        this.gasProgram = gasProgram;

        this.vsf = gasProgram.getVertexStateFactory();

        this.esf = gasProgram.getEdgeStateFactory();

        this.frontier = frontier;

        this.scheduler = gasScheduler;

        /*
         * TODO This is the SPARQL value ordering. It might be not be total or
         * stable. If not, we can use an ordering over the string values of the
         * RDF Values, but that will push the heap.
         */
        this.valueComparator = new ValueComparator();
        
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
    public VS getState(final Value v) {

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
    public boolean isVisited(final Value v) {
        
        return vertexState.get(v) != null;
        
    }

    @Override
    public ES getState(final Statement e) {

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

        frontier.resetFrontier(0/* minCapacity */, false/* sortFrontier */,
                GASUtil.EMPTY_VERTICES_ITERATOR);

    }

    @Override
    public void setFrontier(final IGASContext<VS, ES, ST> ctx, final Value... vertices) {

        if (vertices == null)
            throw new IllegalArgumentException();

        reset();

        // Used to ensure that the initial frontier is distinct.
        final Set<Value> tmp = new HashSet<Value>();

        for (Value v : vertices) {

            tmp.add(v);

        }

        /*
         * Callback to initialize the vertex state before the first iteration.
         */
        for (Value v : tmp) {

            gasProgram.initVertex(ctx, this, v);

        }

        // Reset the frontier.
        frontier.resetFrontier(tmp.size()/* minCapacity */, tmp.size() > 1
                && sortFrontier, tmp.iterator());

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
     * <p>
     * Note: We can not do a parallel reduction right now because the backing
     * class does not expose a parallel iterator, e.g., a segment-wise iterator.
     * The reduction over the {@link #vertexState} is quite slow as a result.
     */
    @Override
    public <T> T reduce(final IReducer<VS, ES, ST, T> op) {

        for (Value v : vertexState.keySet()) {

            op.visit(this, v);

        }

        return op.get();

    }

    @Override
    public String toString(final Statement e) {

        return e.toString();

    }

    /*
     * This is a set of methods for making tests concerning the nature of an
     * edge, vertex, link attribute, or property value. These tests are gathered
     * together in one place because (a) the openrdf data model does not provide
     * efficient link attribute management; and (b) it is not safe to use
     * instanceof tests on the IV implementations to decide these questions.
     * 
     * The IV interface classes can actually declare multiple interfaces that
     * are distinct in the openrdf model. For example, a TermId can be any kind
     * of Value and implements the URI, BNode, and Literal interfaces. Thus,
     * interface testing on a TermId IV is not diagnostic. However, the IVs are
     * explicitly marked with isURI(), isBNode(), and isLiteral() methods. These
     * methods must be used in preference to interface tests (instanceof tests
     * will provide unexpected behavior).
     * 
     * FIXME TESTS: We need a test suite for compliance of these methods.
     */

    /**
     * {@inheritDoc}
     * <p>
     * FIXME We can optimize this to use reference testing if we are careful in
     * the GATHER and SCATTER implementations to impose a canonical mapping over
     * the vertex objects for the edges that are exposed to the
     * {@link IGASProgram} during a single round.
     */
    @Override
    public Value getOtherVertex(final Value u, final Statement e) {

        if (e.getSubject().equals(u))
            return e.getObject();

        return e.getSubject();

    }

    @Override
    public boolean isEdge(final Statement e) {
        return e.getObject() instanceof Resource;
    }
   
    @Override
    public boolean isAttrib(final Statement e) {
        return !isEdge(e);
    }
   
    /**
     * {@inheritDoc}
     * <p>
     * The openrdf implementation does not support link attributes and this
     * method always returns <code>false</code>.
     * 
     * @return <code>false</code>
     */
    @Override
    public boolean isLinkAttrib(final Statement e,
            final URI linkAttribType) {
        return false;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The openrdf implementation does not support link attributes and this
     * method always returns <code>null</code>.
     * 
     * @return <code>null</code>
     */
    @Override
    public Statement decodeStatement(final Value v) {
        return null;
    }

    @Override
    public int compareTo(final Value u, final Value v) {
        
        final int ret = valueComparator.compare(u, v);
        
//        log.error("ret=" + ret + ", u=" + u + ", v=" + v); // FIXME REMOVE
        
        return ret;
        
    }
    
}
