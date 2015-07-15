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
package com.bigdata.rdf.graph.impl.bd;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.graph.BinderBase;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.FrontierEnum;
import com.bigdata.rdf.graph.IBinder;
import com.bigdata.rdf.graph.IBindingExtractor;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IPredecessor;
import com.bigdata.rdf.graph.impl.BaseGASProgram;
import com.bigdata.rdf.internal.IV;

/**
 * PATHS is an iterative graph traversal operation. The frontier is expanded
 * iteratively until no new vertices are discovered, or until the target
 * vertices have all been reached. Each vertex is marked with its depth and with
 * a list of all predecessors and their edges to the vertex. This algorithm is
 * useful for creating a complete connected subgraph between a source and a set
 * of targets.
 */
public class PATHS extends BaseGASProgram<PATHS.VS, PATHS.ES, Void> implements
        IPredecessor<PATHS.VS, PATHS.ES, Void> {

    private static final Logger log = Logger.getLogger(PATHS.class);
    
    public static class VS {

        /**
         * <code>-1</code> until visited. When visited, set to the current round
         * in order to assign each vertex its traversal depth.
         * <p>
         * Note: It is possible that the same vertex may be visited multiple
         * times in a given expansion (from one or more source vertices that all
         * target the same destination vertex). However, in this case the same
         * value will be assigned by each visitor. Thus, synchronization is only
         * required for visibility of the update within the round. As long as
         * one thread reports that it modified the depth, the vertex will be
         * scheduled.
         */
        private final AtomicInteger depth = new AtomicInteger(-1);
        
        /**
         * The predecessors are the source vertices to visit a given target
         * vertex.  Each one has a list of edges along which they were able to
         * reach this vertex.
         */
        private final Map<Value, Set<URI>> predecessors = 
        		Collections.synchronizedMap(new LinkedHashMap<Value, Set<URI>>());
        
        /**
         * The depth at which this vertex was first visited (origin ZERO) and
         * <code>-1</code> if the vertex has not been visited.
         */
        public int depth() {
//            synchronized (this) {
                return depth.get();
//            }
        }

        /**
         * Return the vertices that discovered this vertex during BFS traversal.
         */
        public Map<Value, Set<URI>> predecessors() {

            return predecessors;
            
        }
        
        /**
         * Add a predecessor (might have already been added) and the edge
         * along which the predecessor discovered this vertex.
         */
        public synchronized void addPredecessor(final Value pred, final URI edge) {
        	
        	Set<URI> edges = predecessors.get(pred);
        	
        	if (edges == null) {
        	
        		edges = new LinkedHashSet<URI>();
        		
        		predecessors.put(pred, edges);
        		
        	}
        	
        	edges.add(edge);
        	
        }

        /**
         * Note: This marks the vertex at the current traversal depth.
         * 
         * @return <code>true</code> if the vertex was visited for the first
         *         time in this round and the calling thread is the thread that
         *         first visited the vertex (this helps to avoid multiple
         *         scheduling of a vertex).
         */
        public synchronized boolean visit(final int depth, final Value pred, final URI edge) {

        	boolean ret = false;
        	
            if (this.depth.compareAndSet(-1/* expect */, depth/* newValue */)) {
                // Scheduled by this thread.
                ret = true;
            }
            
        	if (pred != null && this.depth() > 0 && this.depth() == depth) {
//        		this.predecessors.add(pred);
        		addPredecessor(pred, edge);
        	}
        	
            return ret;
            
        }

        @Override
        public String toString() {
            return "{depth=" + depth() + "}";
        }

    }// class VS

    /**
     * Edge state is not used.
     */
    public static class ES {

    }

    private static final Factory<Value, PATHS.VS> vertexStateFactory = new Factory<Value, PATHS.VS>() {

        @Override
        public PATHS.VS initialValue(final Value value) {

            return new VS();

        }

    };

    @Override
    public Factory<Value, PATHS.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public Factory<Statement, PATHS.ES> getEdgeStateFactory() {

        return null;

    }

    @Override
    public FrontierEnum getInitialFrontierEnum() {

        return FrontierEnum.SingleVertex;
        
    }
    
    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.NoEdges;

    }

    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.OutEdges;

    }

    /**
     * Not used.
     */
    @Override
    public void initVertex(final IGASContext<PATHS.VS, PATHS.ES, Void> ctx,
            final IGASState<PATHS.VS, PATHS.ES, Void> state, final Value u) {

        state.getState(u).visit(0, null/* predecessor */, null/* edge */);

    }
    
    /**
     * Not used.
     */
    @Override
    public Void gather(IGASState<PATHS.VS, PATHS.ES, Void> state, Value u, Statement e) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Not used.
     */
    @Override
    public Void sum(final IGASState<PATHS.VS, PATHS.ES, Void> state,
            final Void left, final Void right) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * NOP
     */
    @Override
    public PATHS.VS apply(final IGASState<PATHS.VS, PATHS.ES, Void> state, final Value u, 
            final Void sum) {

        return null;
        
    }

    /**
     * Returns <code>true</code>.
     */
    @Override
    public boolean isChanged(IGASState<VS, ES, Void> state, Value u) {

        return true;
        
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
    public void scatter(final IGASState<PATHS.VS, PATHS.ES, Void> state,
            final IGASScheduler sch, final Value u, final Statement e) {

        // remote vertex state.
        final Value v = state.getOtherVertex(u, e);

        final VS otherState = state.getState(v);
        
        final int otherDepth = otherState.depth();
//        final VS otherState = state.getState(e.getObject()/* v */);

        // visit.
        
        if (otherState.visit(state.round() + 1, u/* predecessor */, e.getPredicate())) {

            /*
             * This is the first visit for the remote vertex. Add it to the
             * schedule for the next iteration.
             */

            sch.schedule(v);

        }

    }
    
    @Override
    public boolean nextRound(final IGASContext<PATHS.VS, PATHS.ES, Void> ctx) {

        return true;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * <dl>
     * <dt>{@value Bindings#DEPTH}</dt>
     * <dd>The depth at which the vertex was first encountered during traversal.
     * </dd>
     * <dt>{@value Bindings#PREDECESSORS}</dt>
     * <dd>The predecessors are all the vertices that discovers a given vertex
     * during traversal.</dd>
     * <dt>{@value Bindings#EDGES}</dt>
     * <dd>These are the edges along which each predecessor discovered a given 
     * vertex during traversal.</dd>
     * </dl>
     */
    @Override
    public List<IBinder<PATHS.VS, PATHS.ES, Void>> getBinderList() {

        final List<IBinder<PATHS.VS, PATHS.ES, Void>> tmp = super.getBinderList();

        tmp.add(new BinderBase<PATHS.VS, PATHS.ES, Void>() {
            
            @Override
            public int getIndex() {
                return Bindings.DEPTH;
            }
            
            @Override
            public Value bind(final ValueFactory vf,
                    final IGASState<PATHS.VS, PATHS.ES, Void> state, final Value u) {

                return vf.createLiteral(state.getState(u).depth.get());

            }

        });

        tmp.add(new IBinder<PATHS.VS, PATHS.ES, Void>() {
            
            @Override
            public int getIndex() {
                return Bindings.PREDECESSORS;
            }
            
            @Override
            public List<Value> bind(final ValueFactory vf,
                    final IGASState<PATHS.VS, PATHS.ES, Void> state, 
                    final Value u, final IVariable<?>[] outVars,
                    final IBindingSet bs) {

            	final VS vs = state.getState(u);
            	
            	return new LinkedList<Value>(vs.predecessors().keySet());

            }
            
        });

        tmp.add(new IBinder<PATHS.VS, PATHS.ES, Void>() {
            
            @Override
            public int getIndex() {
                return Bindings.EDGES;
            }
            
			@Override
			@SuppressWarnings({ "rawtypes", "unchecked" })
            public List<Value> bind(final ValueFactory vf,
                    final IGASState<PATHS.VS, PATHS.ES, Void> state, 
                    final Value u, final IVariable<?>[] outVars,
                    final IBindingSet bs) {

				/*
				 * We want to return a different set of edges depending on
				 * which predecessor the caller is asking about.  We can
				 * find that information in the binding set. 
				 */
				
            	final IVariable<?> var = outVars[Bindings.PREDECESSORS];
            	
            	if (!bs.isBound(var)) {
            		
            		if (log.isTraceEnabled()) {
            			log.trace("no predecessors");
            		}
            		
            		return Collections.EMPTY_LIST;
            		
            	}
            	
            	final IV predIV = (IV) bs.get(var).get();
            	
            	final Value predVal;
            	
            	if (predIV instanceof Value) {
            		
            		predVal = (Value) predIV;
            		
            	} else if (predIV.hasValue()) {
            		
            		predVal = predIV.getValue();
            		
            	} else {
            		
            		throw new RuntimeException("FIXME");
            		
            	}
            	
            	final VS vs = state.getState(u);
            	
            	/*
            	 * Return the edges for this predecessor.
            	 */
            	return new LinkedList<Value>(vs.predecessors().get(predVal));

            }
            
        });

        tmp.add(new IBinder<PATHS.VS, PATHS.ES, Void>() {
            
            @Override
            public int getIndex() {
                return Bindings.PRED_DEPTH;
            }
            
			@Override
			@SuppressWarnings({ "rawtypes", "unchecked" })
            public List<Value> bind(final ValueFactory vf,
                    final IGASState<PATHS.VS, PATHS.ES, Void> state, 
                    final Value u, final IVariable<?>[] outVars,
                    final IBindingSet bs) {

				/*
				 * We want to return a different set of edges depending on
				 * which predecessor the caller is asking about.  We can
				 * find that information in the binding set. 
				 */
				
            	final IVariable<?> var = outVars[Bindings.PREDECESSORS];
            	
            	if (!bs.isBound(var)) {
            		
            		if (log.isTraceEnabled()) {
            			log.trace("no predecessors");
            		}
            		
            		return Collections.EMPTY_LIST;
            		
            	}
            	
            	final IV predIV = (IV) bs.get(var).get();
            	
            	final Value predVal;
            	
            	if (predIV instanceof Value) {
            		
            		predVal = (Value) predIV;
            		
            	} else if (predIV.hasValue()) {
            		
            		predVal = predIV.getValue();
            		
            	} else {
            		
            		throw new RuntimeException("FIXME");
            		
            	}
            	
            	return Arrays.asList(new Value[] { vf.createLiteral(state.getState(predVal).depth.get()) });

            }
            
        });

        return tmp;

    }

    /**
     * Additional {@link IBindingExtractor.IBinder}s exposed by {@link PATHS}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Bindings extends BaseGASProgram.Bindings {
        
        /**
         * The depth at which the vertex was visited.
         */
        int DEPTH = 1;
        
        /**
         * The predecessors are all vertices to discover a given vertex.
         * 
         */
        int PREDECESSORS = 2;
        
        /**
         * The edges along which each predecessor discovered a given vertex.
         */
        int EDGES = 3;
        
        int PRED_DEPTH = 4;
        
    }

    /*
     * TODO Do this in parallel for each specified target vertex.
     */
    @Override
    public void prunePaths(final IGASContext<VS, ES, Void> ctx,
            final Value[] targetVertices) {

//    	if(true)
//    	return;
    	
        if (ctx == null)
            throw new IllegalArgumentException();

        if (targetVertices == null)
            throw new IllegalArgumentException();
        
        final IGASState<PATHS.VS, PATHS.ES, Void> gasState = ctx.getGASState();

        final Set<Value> retainSet = new HashSet<Value>();

        for (Value v : targetVertices) {

            if (!gasState.isVisited(v)) {

                // This target was not reachable.
                continue;

            }
            
            /*
             * Walk the precessors back to a starting vertex.
             */
            retainSet.add(v);
            
            visitPredecessors(gasState, v, retainSet);
            
        } // next target vertex.
        
        gasState.retainAll(retainSet);
        
    }
    
    protected void visitPredecessors(
    		final IGASState<PATHS.VS, PATHS.ES, Void> gasState, final Value v, 
    		final Set<Value> retainSet) {
    	
    	final PATHS.VS currentState = gasState.getState(v);
    	
//    	final int curDepth = currentState.depth.get();
    	
        for (Value pred : currentState.predecessors().keySet()) {

//        	if (pred == null) {
//        		
//        		continue;
//        		
//        	}
        	
//        	if (retainSet.contains(pred)) {
//        		
//        		continue;
//        		
//        	}
        	
//        	final int predDepth = gasState.getState(pred).depth.get();
//        	
//        	if (predDepth >= curDepth) {
//        		
//        		continue;
//        		
//        	}

        	if (!retainSet.contains(pred)) {
        		
	        	retainSet.add(pred);
	        	
	        	visitPredecessors(gasState, pred, retainSet);
	        	
        	}
        	
        }
    	
    }

}
