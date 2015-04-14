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
package com.bigdata.rdf.graph.analytics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
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
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.impl.BaseGASProgram;

/**
 * Page rank assigns weights to the vertices in a graph based by on the relative
 * "importance" as determined by the patterns of directed links in the graph.
 * The algorithm is given stated in terms of a computation that is related until
 * the delta in the computed values for the vertices is within <i>epsilon</i> of
 * ZERO. However, in practice convergence is based on <i>epsilon</i> is
 * problematic due to the manner in which the results of the floating point
 * operations depend on the sequence of those operations (which is why this
 * implementation uses <code>double</code> precision). Thus, page rank is
 * typically executed a specific number of iterations, e.g., 50 or 100. If
 * convergence is based on epsilon, then it is possible that the computation
 * will never converge, especially for smaller values of epsilon.
 * <dl>
 * <dt>init</dt>
 * <dd>All vertices are inserted into the initial frontier.</dd>
 * <dt>Gather</dt>
 * <dd>sum( neighbor_value / neighbor_num_out_edges ) over the in-edges of the
 * graph.</dd>
 * <dt>Apply</dt>
 * <dd>value = <i>resetProb</i> + (1.0 - <i>resetProb</i>) * gatherSum</dd>
 * <dt>Scatter</dt>
 * <dd>if (a) value has significantly changed <code>(fabs(old-new) GT
 * <i>epsilon</i>)</code>; or (b) iterations LT limit</dd>
 * </dl>
 * <ul>
 * <li>where <i>resetProb</i> is a value that determines a random reset
 * probability and defaults to {@value PR#DEFAULT_RESET_PROB}.</li>
 * <li>
 * where <i>epsilon</i> controls the degree of convergence before the algorithm
 * terminates and defaults to {@value PR#DEFAULT_EPSILON}.</li>
 * </ul>
 * 
 * FIXME PR UNIT TEST. Verify computed values (within variance) and max
 * iterations. Ground truth from GL?
 * 
 * FIXME PR: The out-edges can be taken directly from the vertex distribution.
 * That will reduce the initialization overhead for PR.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PR extends BaseGASProgram<PR.VS, PR.ES, Double> {

    private static final Logger log = Logger.getLogger(PR.class);

    // TOOD javadoc and config.
    protected static final int DEFAULT_LIMIT = 100;
    protected static final double DEFAULT_RESET_PROB = 0.15d;
    protected static final double DEFAULT_EPSILON = 0.01d;
    protected static final double DEFAULT_MIN_PAGE_RANK = 1d;
    
    private final double resetProb = DEFAULT_RESET_PROB;
    private final double epsilon = DEFAULT_EPSILON;
    private final int limit = DEFAULT_LIMIT;
    private final double minPageRank = DEFAULT_MIN_PAGE_RANK;
    
    public static class VS {

        /**
         * The current computed value for this vertex.
         * <p>
         * All vertices are initialized to the reset probability. They are then
         * updated in each iteration to the new estimated value by apply().
         */
        private double value;

        /**
         * The number of out-edges. This is computed once, when the vertex state
         * is initialized.
         */
        private long outEdges;
        
        /**
         * The last delta observed for this vertex.
         */
        private double lastChange = 0d;
        
        /**
         * The current computed value for this vertex.
         * <p>
         * All vertices are initialized to the reset probability. They are then
         * updated in each iteration to the new estimated value by apply().
         */
        public double getValue() {

            synchronized (this) {
            
                return value;
                
            }
            
        }
        
        @Override
        public String toString() {
            return "{value=" + value + ",lastChange=" + lastChange + "}";
        }

    }// class VS

    /**
     * Edge state is not used.
     */
    public static class ES {

    }

    private static final Factory<Value, PR.VS> vertexStateFactory = new Factory<Value, PR.VS>() {

        @Override
        public PR.VS initialValue(final Value value) {

            return new VS();

        }

    };

    @Override
    public Factory<Value, PR.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public Factory<Statement, PR.ES> getEdgeStateFactory() {

        return null;

    }

    @Override
    public FrontierEnum getInitialFrontierEnum() {

        return FrontierEnum.AllVertices;
        
    }
    
    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.InEdges;

    }

    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.OutEdges;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Each vertex is initialized to the reset probability.
     * 
     * FIXME We need to do this efficiently. E.g., using a scan to find all of
     * the vertices together with their in-degree or out-degree. That should be
     * done to populate the frontier, initializing the #of out-edges at the same
     * time.
     */
    @Override
    public void initVertex(final IGASContext<PR.VS, PR.ES, Double> ctx,
            final IGASState<PR.VS, PR.ES, Double> state, final Value u) {

        final PR.VS us = state.getState(u);

        synchronized (us) {

            us.value = resetProb;

            us.outEdges = ctx.getGraphAccessor().getEdgeCount(ctx, u,
                    EdgesEnum.OutEdges);
            
        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     */
    @Override
    public Double gather(final IGASState<PR.VS, PR.ES, Double> state,
            final Value u, final Statement e) {

        final Value v = state.getOtherVertex(u, e);

        final PR.VS vs = state.getState(v);

        /*
         * Note: Division by zero should not be possible here since the edge
         * that we used to discover [v] is an out-edge of [v].
         */

        synchronized (vs) {
         
            return (vs.value / vs.outEdges);
            
        }

    }

    /**
     * SUM
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Double sum(final IGASState<PR.VS, PR.ES, Double> state,
            final Double left, final Double right) {

        return left + right;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Compute the new value for this vertex, making a note of the last change
     * for this vertex.
     */
    @Override
    public PR.VS apply(final IGASState<PR.VS, PR.ES, Double> state,
            final Value u, final Double sum) {

        final PR.VS us = state.getState(u);
        
        if (sum == null) {

            /*
             * No in-edges visited by Gather. No change. Vertex will be dropped
             * from the frontier.
             */
            
            synchronized (us) {
                us.lastChange = 0d;
            }

            return null;

        }

        final double newval = resetProb + (1.0 - resetProb) * sum;
        
        synchronized (us) {
        
            us.lastChange = (newval - us.value);

            us.value = newval;
            
        }

        return us;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>true</code> iff the last change was greater then epsilon.
     */
    @Override
    public boolean isChanged(final IGASState<VS, ES, Double> state,
            final Value u) {

        final PR.VS us = state.getState(u);

        return us.lastChange > epsilon;
        
    }

    /**
     * The remote vertex is scheduled for activation unless it has already been
     * visited.
     * <p>
     * Note: We are scattering to out-edges. Therefore, this vertex is
     * {@link Statement#getSubject()}. The remote vertex is
     * {@link Statement#getObject()}.
     */
    @Override
    public void scatter(final IGASState<PR.VS, PR.ES, Double> state,
            final IGASScheduler sch, final Value u, final Statement e) {

        final Value v = state.getOtherVertex(u, e);

        sch.schedule(v);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Continue unless the iteration limit has been reached.
     */
    @Override
    public boolean nextRound(final IGASContext<PR.VS, PR.ES, Double> ctx) {

        return ctx.getGASState().round() < limit;

    }

    /**
     * {@inheritDoc}
     * <p>
     * <dl>
     * <dt>{@value Bindings#RANK}</dt>
     * <dd>The page rank associated with the vertex..</dd>
     * </dl>
     */
    @Override
    public List<IBinder<PR.VS, PR.ES, Double>> getBinderList() {

        final List<IBinder<PR.VS, PR.ES, Double>> tmp = super.getBinderList();

        tmp.add(new BinderBase<PR.VS, PR.ES, Double>() {
            
            @Override
            public int getIndex() {
                return Bindings.RANK;
            }
            
            @Override
            public Value bind(final ValueFactory vf,
                    final IGASState<PR.VS, PR.ES, Double> state, final Value u) {

                return vf.createLiteral(state.getState(u).getValue());

            }

        });

        return tmp;

    }

    /**
     * Additional {@link IBindingExtractor.IBinder}s exposed by {@link PR}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Bindings extends BaseGASProgram.Bindings {
        
        /**
         * The computed page rank for the vertex.
         */
        int RANK = 1;
        
    }

    /**
     * Class reports a map containing the page rank associated with each visited
     * vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public class PageRankReducer implements IReducer<PR.VS, PR.ES, Double, Map<Value,Double>> {

        private final ConcurrentHashMap<Value, Double> values = new ConcurrentHashMap<Value, Double>();
        
        @Override
        public void visit(final IGASState<VS, ES, Double> state,
                final Value u) {

            final VS us = state.getState(u);

            if (us != null) {

                final double pageRank = us.getValue();

                // FIXME Why are NaNs showing up?
                if (Double.isNaN(pageRank))
                    return;

                // FIXME Do infinite values show up?
                if (Double.isInfinite(pageRank))
                    return;
                
                if (pageRank < minPageRank) {
                    // Ignore small values.
                    return;
                }

                /*
                 * Only report the larger ranked values.
                 */

                if (log.isDebugEnabled())
                    log.debug("v=" + u + ", pageRank=" + pageRank);

                values.put(u, Double.valueOf(pageRank));

            }

        }

        @Override
        public Map<Value, Double> get() {

            return Collections.unmodifiableMap(values);

        }
        
    }
    
//    @Override
//    public void after(final IGASContext<PR.VS, PR.ES, Double> ctx) {
//
//        final Map<Value, Double> values = ctx.getGASState().reduce(
//                new PageRankReducer());
//
//        class NV implements Comparable<NV> {
//            public final double n;
//            public final Value v;
//            public NV(double n, Value v) {
//                this.n = n;
//                this.v = v;
//            }
//            @Override
//            public int compareTo(final NV o) {
//                if (o.n > this.n)
//                    return 1;
//                if (o.n < this.n)
//                    return -1;
//                return 0;
//            }
//        }
//
//        final NV[] a = new NV[values.size()];
//
//        int i = 0;
//
//        for (Map.Entry<Value, Double> e : values.entrySet()) {
//        
//            a[i++] = new NV(e.getValue().doubleValue(), e.getKey());
//            
//        }
//
//        Arrays.sort(a);
//
//        System.out.println("rank, pageRank, vertex");
//        i = 0;
//        for (NV t : a) {
//
//            System.out.println(i + ", " + t.n + ", " + t.v);
//            
//            i++;
//            
//        }
//
//    }

}
