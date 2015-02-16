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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.impl.BaseGASProgram;

/**
 * Connected components computes the distinct sets of non-overlapping subgraphs
 * within a graph. All vertices within a connected component are connected along
 * at least one path.
 * <p>
 * The implementation works by assigning a label to each vertex. The label is
 * initially the vertex identifier for that vertex. The labels in the graph are
 * then relaxed with each vertex taking the minimum of its one-hop neighhor's
 * labels. The algorithm halts when no vertex label has changed state in a given
 * iteration.
 * 
 * <dl>
 * <dt>init</dt>
 * <dd>All vertices are inserted into the initial frontier.</dd>
 * <dt>Gather</dt>
 * <dd>Report the source vertex label (not its identifier)</dd>
 * <dt>Apply</dt>
 * <dd>label = min(label,gatherLabel)</dd>
 * <dt>Scatter</dt>
 * <dd>iff the label has changed</dd>
 * </dl>
 * 
 * FIXME CC : Implement version that pushes updates through the scatter function.
 * Find an abstraction to support this pattern. It is used by both CC and SSSP.
 * (We can initially implement this as a Gather (over all edges) plus a
 * conditional Scatter (over all edges iff the vertex label has changed). We can
 * then refactor both this class and SSSP to push the updates through a Scatter
 * (what I think of as a Gather to a remote vertex).)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CC extends BaseGASProgram<CC.VS, CC.ES, Value> {

    private static final Logger log = Logger.getLogger(CC.class);
    
    public static class VS {

        /**
         * The label for the vertex. This value is initially the vertex
         * identifier. It is relaxed by the computation until it is the minimum
         * vertex identifier for the connected component.
         */
        private final AtomicReference<Value> label;

        /**
         * <code>true</code> iff the label was modified.
         */
        private boolean changed = false;

        public VS(final Value v) {

            this.label = new AtomicReference<Value>(v);
            
        }

        /**
         * The assigned label for this vertex. Once converged, all vertices in a
         * given connected component will have the same label and the labels
         * assigned to the vertices in each connected component will be
         * distinct. The labels themselves are just the identifier of a vertex
         * in that connected component. Conceptually, either the MIN or the MAX
         * over the vertex identifiers in the connected component can be used by
         * the algorithm since both will provide a unique labeling strategy.
         */
        public Value getLabel() {
        
            return label.get();
            
        }
        
        private void setLabel(final Value v) {

            label.set(v);
            
        }
        
        @Override
        public String toString() {
            return "{label=" + label + ",changed=" + changed + "}";
        }

    }// class VS

    /**
     * Edge state is not used.
     */
    public static class ES {

    }

    private static final Factory<Value, CC.VS> vertexStateFactory = new Factory<Value, CC.VS>() {

        @Override
        public CC.VS initialValue(final Value value) {

            return new VS(value);

        }

    };

    @Override
    public Factory<Value, CC.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public Factory<Statement, CC.ES> getEdgeStateFactory() {

        return null;

    }

    @Override
    public FrontierEnum getInitialFrontierEnum() {

        return FrontierEnum.AllVertices;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to not impose any filter on the sampled vertices (it does not
     * matter whether they have any connected edges since we need to put all
     * vertices into the initial frontier).
     */
    @Override
    public EdgesEnum getSampleEdgesFilter() {
    
        return EdgesEnum.NoEdges;
        
    }
    
    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.AllEdges;

    }

    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.AllEdges;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Return the label of the remote vertex.
     */
    @Override
    public Value gather(final IGASState<CC.VS, CC.ES, Value> state,
            final Value u, final Statement e) {

        final Value v = state.getOtherVertex(u, e);

        final CC.VS vs = state.getState(v);

        return vs.getLabel();

    }

    /**
     * MIN
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Value sum(final IGASState<CC.VS, CC.ES, Value> state,
            final Value left, final Value right) {

        // MIN(left,right)
        if (state.compareTo(left, right) < 0) {

            return left;
            
        }
        
        return right;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Compute the new value for this vertex, making a note of the last change
     * for this vertex.
     */
    @Override
    public CC.VS apply(final IGASState<CC.VS, CC.ES, Value> state,
            final Value u, final Value sum) {

        final CC.VS us = state.getState(u);

        if (sum == null) {

            /*
             * Nothing visited by Gather. No change. Vertex will be dropped from
             * the frontier.
             */

            us.changed = false;

            return null;

        }

        final Value oldval = us.getLabel();

        // MIN(oldval,gatherSum)
        if (state.compareTo(oldval, sum) <= 0) {

            us.changed = false;
            
            if (log.isDebugEnabled())
                log.debug(" NO CHANGE: " + u + ", val=" + oldval);

        } else {

            us.setLabel(sum);
            
            us.changed = true;

            if (log.isDebugEnabled())
                log.debug("DID CHANGE: " + u + ", old=" + oldval + ", new="
                        + sum);

        }
        
        return us;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>true</code> iff the label was changed in the current round.
     */
    @Override
    public boolean isChanged(final IGASState<VS, ES, Value> state,
            final Value u) {

        final CC.VS us = state.getState(u);

        return us.changed;

    }

    /**
     * The remote vertex is scheduled for activation unless it has already been
     * visited.
     */
    @Override
    public void scatter(final IGASState<CC.VS, CC.ES, Value> state,
            final IGASScheduler sch, final Value u, final Statement e) {

        final Value v = state.getOtherVertex(u, e);

        sch.schedule(v);

    }

    /**
     * {@inheritDoc}
     * <p>
     * <dl>
     * <dt>{@value Bindings#LABEL}</dt>
     * <dd>The label associated with all of the vertices in the same subgraph.
     * The label is a vertex identifier and can be used to jump into the
     * subgraph.</dd>
     * </dl>
     */
    @Override
    public List<IBinder<CC.VS, CC.ES, Value>> getBinderList() {

        final List<IBinder<CC.VS, CC.ES, Value>> tmp = super.getBinderList();

        tmp.add(new BinderBase<CC.VS, CC.ES, Value>() {
            
            @Override
            public int getIndex() {
                return Bindings.LABEL;
            }
            
            @Override
            public Value bind(final ValueFactory vf,
                    final IGASState<CC.VS, CC.ES, Value> state, final Value u) {

                return state.getState(u).label.get();

            }

        });

        return tmp;

    }

    /**
     * Additional {@link IBindingExtractor.IBinder}s exposed by {@link CC}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Bindings extends BaseGASProgram.Bindings {
        
        /**
         * The label associated with all of the vertices in a subgraph. The
         * label is a vertex identifier and can be used to jump into the
         * subgraph.
         */
        int LABEL = 1;
        
    }

    /**
     * Returns a map containing the labels assigned to each connected component
     * (which gives you a vertex in that connected component) and the #of
     * vertices in each connected component.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public class ConnectedComponentsReducer implements IReducer<CC.VS,CC.ES,Value,Map<Value,AtomicInteger>> {

        final ConcurrentHashMap<Value, AtomicInteger> labels = new ConcurrentHashMap<Value, AtomicInteger>();

        @Override
        public void visit(final IGASState<VS, ES, Value> state, final Value u) {

            final VS us = state.getState(u);

            if (us != null) {

                final Value label = us.getLabel();

                if (log.isDebugEnabled())
                    log.debug("v=" + u + ", label=" + label);

                final AtomicInteger oldval = labels.putIfAbsent(label,
                        new AtomicInteger(1));

                if (oldval != null) {

                    // lost race. increment existing counter.
                    oldval.incrementAndGet();

                }

            }

        }

        @Override
        public Map<Value, AtomicInteger> get() {

            return Collections.unmodifiableMap(labels);

        }

    }
    
    /**
     * Returns a map containing the labels assigned to each connected component
     * (which gives you a vertex in that connected component) and the #of
     * vertices in each connected component.
     */
    public Map<Value, AtomicInteger> getConnectedComponents(
            final IGASState<CC.VS, CC.ES, Value> state) {

        return state.reduce(new ConnectedComponentsReducer());
    }
    
//    @Override
//    public void after(final IGASContext<CC.VS, CC.ES, Value> ctx) {
//
//        final Map<Value, AtomicInteger> labels = getConnectedComponents(ctx
//                .getGASState());
//
//        System.out.println("There are " + labels.size()
//                + " connected components");
//        
//        class NV implements Comparable<NV> {
//            public final int n;
//            public final Value v;
//            public NV(int n, Value v) {
//                this.n = n;
//                this.v = v;
//            }
//            @Override
//            public int compareTo(final NV o) {
//                return o.n - this.n;
//            }
//        }
//
//        final NV[] a = new NV[labels.size()];
//        int i = 0;
//        for (Map.Entry<Value, AtomicInteger> e : labels.entrySet()) {
//            a[i++] = new NV(e.getValue().intValue(), e.getKey());
//        }
//
//        Arrays.sort(a);
//        
//        System.out.println("size, label");
//        for(NV t : a) {
//            System.out.println(t.n + ", " + t.v);
//        }
//        
//    }
    
}
