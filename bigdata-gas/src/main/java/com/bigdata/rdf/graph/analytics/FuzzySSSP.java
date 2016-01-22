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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.analytics.FuzzySSSP.FuzzySSSPResult;

/**
 * This algorithm provides a fuzzy implementation of the shortest paths between
 * a set of source vertices and a set of target vertices. This can be used to
 * identify a set of vertices that are close to the shortest paths between those
 * source and target vertices. For some domains, the resulting set of vertices
 * can be understood as an "interesting subgraph".
 * <p>
 * Problem: We want to find a set of not more than N vertices out of a data set
 * that are "close" to the shortest path between two sets of vertices.
 * <p>
 * Approach: We want to find the set of SP (Shortest Path) vertices that lie
 * along the shortest path between each source vertex and each target vertex. We
 * would also like to know whether a source is connected to each target. To do
 * this, we do NSOURCES SSSP traversals. For each traversal, we note the depth
 * of each target from each source, and mark the depth as -1 if the target was
 * not reachable from that source. The vertices along the shortest path to the
 * target are collected. The sets of collected vertices are merged and
 * duplicates are removed.
 * <p>
 * Finally, we do a BFS starting with all of the vertices in that merged
 * collection and stopping when we have N vertices, including those along the
 * shortest paths. This grows the initial set of vertices that lie along the
 * shortest paths into a broader collection of vertices that are close to that
 * shortest path.
 * <p>
 * Outputs: The N vertices, their distances from the shortest paths (which we
 * get out of the final BFS), and the distance of each target from each source
 * along the shortest path (which we get from the per-source SSSP traversals).
 * 
 * TODO Support breaking out of the analytic as soon as the frontier is known to
 * contain at least N(=2k) distinct vertices (or M=10k edges). Note that for
 * frontier implementations that allow duplicates, this means that you need to
 * wait for the end of the iteration to make the decision. We already support a
 * decision point at the end of each iteration. This would allow us to lift the
 * decision point inside of the iteration and terminate processing eagerly when
 * the frontier size exceeds a specified value.
 * 
 * TODO: Implement unit test with ground truth.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class FuzzySSSP implements Callable<FuzzySSSPResult>{

    /**
     * The source vertices (there must be at least one).
     */
    private final Value[] src;
    /**
     * The target vertices (there must be at least one).
     */
    private final Value[] tgt;
    /**
     * The maximum number of vertices to report (stopping criteria for the BFS
     * expansion).
     */
    private final int N;
    
    /**
     * The {@link IGASEngine} used to run the analytics.
     */
    private final IGASEngine gasEngine;
    
    /**
     * The object used to access the graph.
     */
    private final IGraphAccessor graphAccessor;

    /**
     * Interface for communicating the results back to the caller.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public class FuzzySSSPResult {
        
        /**
         * The reachability map. The keys of the outer map are the source
         * vertices. The values in the inner maps are the target vertices that
         * are reachable from a given source vertex (both the key and the value
         * of the inner map is the target vertex - it is being used as a set).
         */
        private ConcurrentMap<Value, ConcurrentMap<Value, Value>> reachable = new ConcurrentHashMap<Value, ConcurrentMap<Value, Value>>();

        /**
         * The set of visited vertices.
         */
        private Set<Value> visited = new LinkedHashSet<Value>();
        
//        private Map<Value,Set<Value>>
        private boolean addVisited(final Value v) {

            return visited.add(v);
            
        }

        /**
         * Assert that the target was reachable from the source.
         * 
         * @param src
         *            The source.
         * @param tgt
         *            The target.
         */
        private void addReachable(final Value src, final Value tgt) {

            if (src == null)
                throw new IllegalArgumentException();

            if (tgt == null)
                throw new IllegalArgumentException();
            
            ConcurrentMap<Value, Value> tmp = reachable.get(src);

            if (tmp == null) {

                final ConcurrentMap<Value, Value> old = reachable.putIfAbsent(
                        src, tmp = new ConcurrentHashMap<Value, Value>());

                if (old != null) {

                    // Lost the data race.
                    tmp = old;

                }

            }

            // add target to the reachability set for that source.
            tmp.putIfAbsent(tgt, tgt);
            
        }
        
        /**
         * Return the number of visited vertices.
         */
        public int getVisitedCount()  {

            return visited.size();
            
        }
        
        /**
         * Return <code>true</code> if the given target is reachable by the
         * given source.
         * 
         * @param src
         *            The source.
         * @param tgt
         *            The target.
         * @return <code>true</code> iff the target is reachable from that
         *         source.
         */
        public boolean getReachable(Value src, Value tgt) {
            throw new UnsupportedOperationException();
        }
        
        /**
         * Return the set of vertices that were discovered by the analytic. This
         * constitutes an "interesting subgraph". The source and target vertices
         * will be included in this collection. Each vertex along a shortest
         * path from a source vertex to each of the target vertices will be
         * included. Finally, each vertex in the BFS expension of those vertices
         * will be included up to the maximum specified when the analytic was
         * run.
         */
        public Set<Value> getVisitedVertices() {
            throw new UnsupportedOperationException();
        }

        /**
         * TODO Also show the reachability matrix and perhaps the visited
         * vertices in level sets.
         */
        @Override
        public String toString() {

            return getClass().getName() + "{nvisited=" + visited.size() + "}";
            
        }
        
    } // class FuzzySSSPResult
    
    /**
     * 
     * @param src
     *            The source vertices (there must be at least one).
     * @param tgt
     *            The target vertices (there must be at least one).
     * @param N
     *            The maximum number of vertices to report (must be positive),
     *            i.e., the stopping criteria for the BFS expansion.
     * @param gasEngine
     *            The {@link IGASEngine} will be used to execute the analytic.
     * @param graphAccessor
     *            The object used to access the graph.
     */
    public FuzzySSSP(final Value[] src, final Value[] tgt, final int N,
            final IGASEngine gasEngine, final IGraphAccessor graphAccessor) {

        if (src == null)
            throw new IllegalArgumentException();
        if (src.length == 0)
            throw new IllegalArgumentException();
        for (Value v : src)
            if (v == null)
                throw new IllegalArgumentException();
        if (tgt == null)
            throw new IllegalArgumentException();
        if (tgt.length == 0)
            throw new IllegalArgumentException();
        for (Value v : tgt)
            if (v == null)
                throw new IllegalArgumentException();
        if (N <= 0)
            throw new IllegalArgumentException();
        if (gasEngine == null)
            throw new IllegalArgumentException();
        if (graphAccessor == null)
            throw new IllegalArgumentException();

        this.src = src;
        this.tgt = tgt;
        this.N = N;
        this.gasEngine = gasEngine;
        this.graphAccessor = graphAccessor;
    }
    
    @Override
    public FuzzySSSPResult call() throws Exception {

        final FuzzySSSPResult result = new FuzzySSSPResult();
        
        /*
         * For each source vertex, do an SSSP pass. This labels all reachable
         * vertices with their distance from that source vertex. This will also
         * tell us whether each of the target vertices was reachable from a
         * given source vertex.
         * 
         * Each time we do the SSSP for a source vertex, we collect the set of
         * vertices lying along a shortest path from the source vertex to each
         * of the target vertices. These collections are combined and will be
         * used as the starting point for BFS (below).
         */
        
        // The set of vertices along a shortest path.
        final Set<Value> setAll = new LinkedHashSet<Value>();

        for (Value src : this.src) {

            final IGASContext<SSSP.VS, SSSP.ES, Integer> gasContext = gasEngine
                    .newGASContext(graphAccessor, new SSSP());

            final IGASState<SSSP.VS, SSSP.ES, Integer> gasState = gasContext
                    .getGASState();

            // Initialize the frontier.
            gasState.setFrontier(gasContext, src);

            // Converge.
            gasContext.call();

            // The set of vertices along a shortest path for this source.
            final Set<Value> set = new LinkedHashSet<Value>();

            /*
             * FIXME Extract the vertices on a shortest path.
             * 
             * Note: This requires either maintaining the predecessor map or
             * efficiently obtaining it (if this is possible) from the levels.
             */

            // Extract whether each target vertex is reachable
            for (Value tgt : this.tgt) {
                if (gasState.isVisited(tgt)) {
                    // That target was visited for this source.
                    result.addReachable(src, tgt);
                }
            }

            // Combine with the vertices from the other sources.
            setAll.addAll(set);

        }

        /*
         * BFS.
         * 
         * We populate the initial frontier with the set of vertices that we
         * collected above.
         * 
         * Note: BFS is overridden to halt once we have visited at least N
         * vertices.
         */
        {
            final IGASContext<BFS.VS, BFS.ES, Void> gasContext = gasEngine
                    .newGASContext(graphAccessor, new BFS() {
                        @Override
                        public boolean nextRound(IGASContext<VS, ES, Void> ctx) {
                            final IStaticFrontier frontier = ctx.getGASState()
                                    .frontier();
                            final Iterator<Value> itr = frontier.iterator();
                            while (itr.hasNext()) {
                                final Value v = itr.next();
                                if (result.addVisited(v)
                                        && result.getVisitedCount() >= N) {
                                    /*
                                     * We have reached our threshold during the
                                     * BFS expansion.
                                     * 
                                     * Note: Since we are expanding in a breadth
                                     * first manner, all vertices discovered
                                     * during a given iteration are at the same
                                     * distance from the initial set of vertices
                                     * collected from the shortest paths.
                                     */
                                    return false;
                                }
                            }
                            // Inherent the base class behavior.
                            return super.nextRound(ctx);
                        }
                    });

            final IGASState<BFS.VS, BFS.ES, Void> gasState = gasContext
                    .getGASState();

            // Initialize the frontier.
            for (Value v : setAll) {

                // add to frontier.
                gasState.setFrontier(gasContext, v);
                
                // Add to initial visited set.
                result.addVisited(v);
                
            }

            // Converge.
            gasContext.call();

            /*
             * Note: We extracted the active vertices in each iteration from the
             * new frontier, so we are done as soon we the BFS terminates.
             */

        }

        // Return result.
        return result;
        
    }

   

}
