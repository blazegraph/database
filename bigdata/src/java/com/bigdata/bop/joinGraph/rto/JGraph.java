/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Feb 22, 2011
 */

package com.bigdata.bop.joinGraph.rto;

import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.joinGraph.NoSolutionsException;
import com.bigdata.bop.rdf.join.DataSetJoin;

/**
 * A runtime optimizer for a join graph. The {@link JoinGraph} bears some
 * similarity to ROX (Runtime Optimizer for XQuery), but has several significant
 * differences:
 * <ol>
 * <li>
 * 1. ROX starts from the minimum cardinality edge of the minimum cardinality
 * vertex. The {@link JoinGraph} starts with one or more low cardinality
 * vertices.</li>
 * <li>
 * 2. ROX always extends the last vertex added to a given join path. The
 * {@link JoinGraph} extends all vertices having unexplored edges in each
 * breadth first expansion.</li>
 * <li>
 * 3. ROX is designed to interleave operator-at-once evaluation of join path
 * segments which dominate other join path segments. The {@link JoinGraph} is
 * designed to prune all join paths which are known to be dominated by other
 * join paths for the same set of vertices in each round and iterates until a
 * join path is identified which uses all vertices and has the minimum expected
 * cumulative estimated cardinality. Join paths which survive pruning are
 * re-sampled as necessary in order to obtain better information about edges in
 * join paths which have a low estimated cardinality in order to address a
 * problem with underflow of the cardinality estimates.</li>
 * </ol>
 * 
 * TODO For join graphs with a large number of vertices we may need to constrain
 * the #of vertices which are explored in parallel. This could be done by only
 * branching the N lowest cardinality vertices from the already connected edges.
 * Since fewer vertices are being explored in parallel, paths are more likely to
 * converge onto the same set of vertices at which point we can prune the
 * dominated paths.
 * 
 * TODO Compare the cumulative expected cardinality of a join path with the
 * expected cost of a join path. The latter allows us to also explore
 * alternative join strategies, such as the parallel subquery versus scan and
 * filter decision for named graph and default graph SPARQL queries.
 * 
 * TODO Coalescing duplicate access paths can dramatically reduce the work
 * performed by a pipelined nested index subquery. (A hash join eliminates all
 * duplicate access paths using a scan and filter approach.) If we will run a
 * pipeline nested index subquery join, then should the runtime query optimizer
 * prefer paths with duplicate access paths?
 * 
 * TODO How can we handle things like lexicon joins. A lexicon join is is only
 * evaluated when the dynamic type of a variable binding indicates that the RDF
 * Value must be materialized by a join against the ID2T index. Binding sets
 * having inlined values can simply be routed around the join against the ID2T
 * index. Routing around saves network IO in scale-out where otherwise we would
 * route binding sets having identifiers which do not need to be materialized to
 * the ID2T shards.
 * 
 * @todo Examine the overhead of the runtime optimizer. Look at ways to prune
 *       its costs. For example, by pruning the search, by recognizing when the
 *       query is simple enough to execute directly, by recognizing when we have
 *       already materialized the answer to the query, etc.
 * 
 * @todo Cumulative estimated cardinality is an estimate of the work to be done.
 *       However, the actual cost of a join depends on whether we will use
 *       nested index subquery or a hash join and the cost of that operation on
 *       the database. There could be counter examples where the cost of the
 *       hash join with a range scan using the unbound variable is LT the nested
 *       index subquery. For those cases, we will do the same amount of IO on
 *       the hash join but there will still be a lower cardinality to the join
 *       path since we are feeding in fewer solutions to be joined.
 * 
 * @todo Look at the integration with the SAIL. We decorate the joins with some
 *       annotations. Those will have to be correctly propagated to the "edges"
 *       in order for edge sampling and incremental evaluation (or final
 *       evaluation) to work. The {@link DataSetJoin} essentially inlines one of
 *       its access paths. That should really be changed into an inline access
 *       path and a normal join operator so we can defer some of the details
 *       concerning the join operator annotations until we decide on the join
 *       path to be executed. An inline AP really implies an inline relation,
 *       which in turn implies that the query is a searchable context for
 *       query-local resources.
 *       <p>
 *       For s/o, when the AP is remote, the join evaluation context must be ANY
 *       and otherwise (for s/o) it must be SHARDED.
 *       <p>
 *       Since the join graph is fed the vertices (APs), it does not have access
 *       to the annotated joins so we need to generated appropriately annotated
 *       joins when sampling an edge and when evaluation a subquery.
 *       <p>
 *       One solution would be to always use the unpartitioned views of the
 *       indices for the runtime query optimizer, which is how we are estimating
 *       the range counts of the access paths right now. [Note that the static
 *       query optimizer ignores named and default graphs, while the runtime
 *       query optimizer SHOULD pay attention to these things and exploit their
 *       conditional selectivity for the query plan.]
 * 
 * @todo Handle optional join graphs by first applying the runtime optimizer to
 *       the main join graph and obtaining a sample for the selected join path.
 *       That sample will then be feed into the the optional join graph in order
 *       to optimize the join order within the optional join graph (a join order
 *       which is selective in the optional join graph is better since it build
 *       up the #of intermediate results more slowly and hence do less work).
 *       <p>
 *       This is very much related to accepting a collection of non-empty
 *       binding sets when running the join graph. However, optional join graph
 *       should be presented in combination with the original join graph and the
 *       starting paths must be constrained to have the selected join path for
 *       the original join graph as a prefix. With this setup, the original join
 *       graph has been locked in to a specific join path and the sampling of
 *       edges and vertices for the optional join graph can proceed normally.
 *       <p>
 *       True optionals will always be appended as part of the "tail plan" for
 *       any join graph and can not be optimized as each optional join must run
 *       regardless (as long as the intermediate solution survives the
 *       non-optional joins).
 * 
 * @todo There are two cases where a join graph must be optimized against a
 *       specific set of inputs. In one case, it is a sample (this is how
 *       optimization of an optional join group proceeds per above). In the
 *       other case, the set of inputs is fixed and is provided instead of a
 *       single empty binding set as the starting condition. This second case is
 *       actually a bit more complicated since we can not use a random sample of
 *       vertices unless the do not share any variables with the initial binding
 *       sets. When there is a shared variable, we need to do a cutoff join of
 *       the edge with the initial binding sets. When there is not a shared
 *       variable, we can sample the vertex and then do a cutoff join.
 * 
 * @todo When we run into a cardinality estimation underflow (the expected
 *       cardinality goes to zero) we could double the sample size for just
 *       those join paths which hit a zero estimated cardinality and re-run them
 *       within the round. This would imply that we keep per join path limits.
 *       The vertex and edge samples are already aware of the limit at which
 *       they were last sampled so this should not cause any problems there.
 *       <p>
 *       A related option would be to deepen the samples only when we are in
 *       danger of cardinality estimation underflow. E.g., a per-path limit.
 *       Resampling vertices may only make sense when we increase the limit
 *       since otherwise we may find a different correlation with the new sample
 *       but the comparison of paths using one sample base with paths using a
 *       different sample base in a different round does not carry forward the
 *       cardinality estimates from the prior round (unless we do something like
 *       a weighted moving average).
 * 
 * @todo When comparing choices among join paths having fully bound tails where
 *       the estimated cardinality has also gone to zero, we should prefer to
 *       evaluate vertices in the tail with better index locality first. For
 *       example, if one vertex had one variable in the original plan while
 *       another had two variables, then solutions which reach the 2-var vertex
 *       could be spread out over a much wider range of the selected index than
 *       those which reach the 1-var vertex. [In order to support this, we would
 *       need a means to indicate that a fully bound access path should use an
 *       index specified by the query optimizer rather than the primary index
 *       for the relation. In addition, this suggests that we should keep bloom
 *       filters for more than just the SPO(C) index in scale-out.]
 * 
 * @todo Examine behavior when we do not have perfect covering indices. This
 *       will mean that some vertices can not be sampled using an index and that
 *       estimation of their cardinality will have to await the estimation of
 *       the cardinality of the edge(s) leading to that vertex. Still, the
 *       approach should be able to handle queries without perfect / covering
 *       automatically. Then experiment with carrying fewer statement indices
 *       for quads.
 * 
 * @todo Unit test when there are no solutions to the query. In this case there
 *       will be no paths identified by the optimizer and the final path length
 *       becomes zero.
 * 
 * @see <a
 *      href="http://www-db.informatik.uni-tuebingen.de/files/research/pathfinder/publications/rox-demo.pdf">
 *      ROX </a>
 */
public class JGraph {

    private static final transient Logger log = Logger.getLogger(JGraph.class);

    /**
     * Vertices of the join graph.
     */
    private final Vertex[] V;

    /**
     * Constraints on the join graph. A constraint is applied once all
     * variables referenced by a constraint are known to be bound.
     */
    private final IConstraint[] C;

    /**
     * The kind of samples that will be taken when we sample a {@link Vertex}.
     */
    private final SampleType sampleType;
    
    public List<Vertex> getVertices() {
        return Collections.unmodifiableList(Arrays.asList(V));
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("JoinGraph");
        sb.append("{V=[");
        for (Vertex v : V) {
            sb.append("\nV[" + v.pred.getId() + "]=" + v);
        }
        sb.append("{C=[");
        for (IConstraint c : C) {
            sb.append("\nC[" + c.getId() + "]=" + c);
        }
        sb.append("\n]}");
        return sb.toString();
    }

    /**
     * 
     * @param v
     *            The vertices of the join graph. These are
     *            {@link IPredicate}s associated with required joins.
     * @param constraints
     *            The constraints of the join graph (optional). Since all
     *            joins in the join graph are required, constraints are
     *            dynamically attached to the first join in which all of
     *            their variables are bound.
     * 
     * @throws IllegalArgumentException
     *             if the vertices is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the vertices is an empty array.
     * @throws IllegalArgumentException
     *             if any element of the vertices is <code>null</code>.
     * @throws IllegalArgumentException
     *             if any constraint uses a variable which is never bound by
     *             the given predicates.
     * @throws IllegalArgumentException
     *             if <i>sampleType</i> is <code>null</code>.
     * 
     * @todo unit test for a constraint using a variable which is never
     *       bound.
     */
    public JGraph(final IPredicate<?>[] v, final IConstraint[] constraints,
            final SampleType sampleType) {

        if (v == null)
            throw new IllegalArgumentException();

        if (v.length < 2)
            throw new IllegalArgumentException();

        V = new Vertex[v.length];

        for (int i = 0; i < v.length; i++) {

            if (v[i] == null)
                throw new IllegalArgumentException();

            V[i] = new Vertex(v[i]);

        }

        if (constraints != null) {
            C = new IConstraint[constraints.length];
            for (int i = 0; i < constraints.length; i++) {
                if (constraints[i] == null)
                    throw new IllegalArgumentException();
                C[i] = constraints[i];
            }
        } else {
            // No constraints.
            C = null;
        }

        if (sampleType == null)
            throw new IllegalArgumentException();
        
        this.sampleType = sampleType;

    }

    /**
     * Find a good join path in the data given the join graph. The join path is
     * not guaranteed to be the best join path (the search performed by the
     * runtime optimizer is not exhaustive) but it should always be a "good"
     * join path and may often be the "best" join path.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The limit for sampling a vertex and the initial limit for
     *            cutoff join evaluation.
     * @param nedges
     *            The edges in the join graph are sorted in order of increasing
     *            cardinality and up to <i>nedges</i> of the edges having the
     *            lowest cardinality are used to form the initial set of join
     *            paths. For each edge selected to form a join path, the
     *            starting vertex will be the vertex of that edge having the
     *            lower cardinality.
     * @param sampleType
     *            Type safe enumeration indicating the algorithm which will be
     *            used to sample the initial vertices.
     * 
     * @return The join path identified by the runtime query optimizer as the
     *         best path given the join graph and the data.
     * 
     * @throws NoSolutionsException
     *             If there are no solutions for the join graph in the data (the
     *             query does not have any results).
     * @throws IllegalArgumentException
     *             if <i>queryEngine</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>limit</i> is non-positive.
     * @throws IllegalArgumentException
     *             if <i>nedges</i> is non-positive.
     * @throws Exception
     * 
     * @todo It is possible that this could throw a {@link NoSolutionsException}
     *       if the cutoff joins do not use a large enough sample to find a join
     *       path which produces at least one solution (except that no solutions
     *       for an optional join do not cause the total to fail, nor do no
     *       solutions for some part of a UNION).
     * 
     *       TODO We need to automatically increase the depth of search for
     *       queries where we have cardinality estimation underflows or punt to
     *       another method to decide the join order.
     */
    public Path runtimeOptimizer(final QueryEngine queryEngine,
            final int limit, final int nedges)
            throws Exception, NoSolutionsException {

        if (queryEngine == null)
            throw new IllegalArgumentException();
        if (limit <= 0)
            throw new IllegalArgumentException();
        if (nedges <= 0)
            throw new IllegalArgumentException();

        // Setup the join graph.
        Path[] paths = round0(queryEngine, limit, nedges);

        /*
         * The initial paths all have one edge, and hence two vertices. Each
         * round adds one more vertex to each path. We are done once we have
         * generated paths which include all vertices.
         * 
         * This occurs at round := nvertices - 1
         * 
         * Note: There are a few edge cases, such as when sampling can not
         * find any solutions, even with an increased sampling limit.
         * Eventually we wind up proving that there are no solutions for the
         * query.
         */

        final int nvertices = V.length;

        int round = 1;

        /*
         * This map is used to associate join path segments (expressed as an
         * ordered array of bopIds) with edge sample to avoid redundant effort.
         * 
         * FIXME HEAP MANAGMENT : This map holds references to the cutoff join
         * samples. To ensure that the map has the minimum heap footprint, it
         * must be scanned each time we prune the set of active paths and any
         * entry which is not a prefix of an active path should be removed.
         * 
         * TODO MEMORY MANAGER : When an entry is cleared from this map, the
         * corresponding allocation in the memory manager (if any) must be
         * released. The life cycle of the map needs to be bracketed by a
         * try/finally in order to ensure that all allocations associated with
         * the map are released no later than when we leave the lexicon scope of
         * that clause.
         */
        final Map<PathIds, EdgeSample> edgeSamples = new LinkedHashMap<PathIds, EdgeSample>();
        
        while (paths.length > 0 && round < nvertices - 1) {

            paths = expand(queryEngine, limit, round++, paths, edgeSamples);

        }

        if (paths.length == 0) {

            // There are no solutions for the join graph in the data.
            throw new NoSolutionsException();
            
        }
        
        // Should be one winner.
        assert paths.length == 1;

        if (log.isInfoEnabled()) {

            log.info("\n*** Selected join path: "
                    + Arrays.toString(paths[0].getVertexIds()) + "\n"
                    + showPath(paths[0], edgeSamples));

        }
        
        return paths[0];

    }

    /**
     * Return a permutation vector which may be used to reorder the given
     * {@link IPredicate}[] into the evaluation order selected by the
     * runtime query optimizer.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the given {@link Path} does not cover all vertices in
     *             the join graph.
     */
    public int[] getOrder(final Path p) {

        if(p == null)
            throw new IllegalArgumentException();
        
        final IPredicate<?>[] path = p.getPredicates();

        if (path.length != V.length) {
            throw new IllegalArgumentException(
                    "Wrong path length: #vertices=" + V.length
                            + ", but path.length=" + path.length);
        }

        final int[] order = new int[V.length];

        for (int i = 0; i < order.length; i++) {

            boolean found = false;
            for (int j = 0; j < order.length; j++) {

                if (path[i].getId() == V[j].pred.getId()) {
                    order[i] = j;
                    found = true;
                    break;
                }

            }

            if (!found)
                throw new RuntimeException("No such vertex: id="
                        + path[i].getId());

        }
        
        return order;
        
    }

    /**
     * Choose the starting vertices.
     * 
     * @param nedges
     *            The maximum #of edges to choose.
     * @param paths
     *            The set of possible initial paths to choose from.
     * 
     * @return Up to <i>nedges</i> minimum cardinality paths.
     */
    public Path[] chooseStartingPaths(final int nedges, final Path[] paths) {

        final List<Path> tmp = new LinkedList<Path>();

        // Sort them by ascending expected cardinality.
        Arrays.sort(paths, 0, paths.length,
                EstimatedCardinalityComparator.INSTANCE);

        // Choose the top-N edges (those with the least cardinality).
        for (int i = 0; i < paths.length && i < nedges; i++) {

            tmp.add(paths[i]);

        }

        return tmp.toArray(new Path[tmp.size()]);

    }

    /**
     * Choose up to <i>nedges</i> edges to be the starting point. For each of
     * the <i>nedges</i> lowest cardinality edges, the starting vertex will be
     * the vertex with the lowest cardinality for that edge.
     * <p>
     * Note: An edge can not serve as a starting point for exploration if it
     * uses variables (for example, in a CONSTRAINT) which are not bound by
     * either vertex (since the variable(s) are not bound, the constraint would
     * always fail).
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The cutoff used when sampling the vertices and when sampling
     *            the edges.
     * @param nedges
     *            The maximum #of edges to choose. Those having the smallest
     *            expected cardinality will be chosen.
     * 
     * @return An initial set of paths starting from at most <i>nedges</i>.
     * 
     * @throws Exception
     * 
     * @todo UNIT TEST : with runFirst predicates in the join graph. [If we
     *       allow "runFirst" predicates into the join graph, then an initial
     *       non-empty join path needs to be constructed from those vertices.
     *       The operators for those APs would also have to be modified to
     *       support cutoff evaluation or to be fully materialized.]
     */
    public Path[] round0(final QueryEngine queryEngine, final int limit,
            final int nedges) throws Exception {

        /*
         * Sample the vertices.
         */
        sampleAllVertices(queryEngine, limit);

        if (log.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Vertices:\n");
            for (Vertex v : V) {
                sb.append(v.toString());
                sb.append("\n");
            }
            log.debug(sb.toString());
        }

        /*
         * Estimate the cardinality for each edge.
         */
        final Path[] a = estimateInitialEdgeWeights(queryEngine, limit);

        if (log.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("All possible initial paths:\n");
            for (Path x : a) {
                sb.append(x.toString());
                sb.append("\n");
            }
            log.debug(sb.toString());
        }

        /*
         * Choose the initial set of paths.
         */
        final Path[] paths_t0 = chooseStartingPaths(nedges, a);

        if (log.isInfoEnabled())
            log.info("\n*** Paths @ t0\n" + JGraph.showTable(paths_t0));

        /*
         * Discard samples for vertices which were not chosen as starting points
         * for join paths.
         * 
         * Note: We do not need the samples of the other vertices once we decide
         * on the initial vertices from which the join paths will be grown so
         * they could even be discarded at this point.
         */
        {

            final Set<Vertex> initialVertexSet = new LinkedHashSet<Vertex>();

            for (Path x : paths_t0) {

                initialVertexSet.add(x.vertices[0]);

            }

            for (Vertex v : V) {

                if (!initialVertexSet.contains(v)) {

                    // Discard sample.
                    v.sample = null;

                }

            }

        }
        
        return paths_t0;

    }

    /**
     * Do one breadth first expansion. In each breadth first expansion we extend
     * each of the active join paths by one vertex for each remaining vertex
     * which enjoys a constrained join with that join path. In the event that
     * there are no remaining constrained joins, we will extend the join path
     * using an unconstrained join if one exists. In all, there are three
     * classes of joins to be considered:
     * <ol>
     * <li>The target predicate directly shares a variable with the source join
     * path. Such joins are always constrained since the source predicate will
     * have bound that variable.</li>
     * <li>The target predicate indirectly shares a variable with the source
     * join path via a constraint can run for the target predicate and which
     * shares a variable with the source join path. These joins are indirectly
     * constrained by the shared variable in the constraint. BSBM Q5 is an
     * example of this case.</li>
     * <li>Any predicates may always be join to an existing join path. However,
     * joins which do not share variables either directly or indirectly will be
     * full cross products. Therefore such joins are added to the join path only
     * after all constrained joins have been consumed.</li>
     * </ol>
     * 
     * @param queryEngine
     *            The query engine.
     * @param limitIn
     *            The limit (this is automatically multiplied by the round to
     *            increase the sample size in each round).
     * @param round
     *            The round number in [1:n].
     * @param a
     *            The set of paths from the previous round. For the first round,
     *            this is formed from the initial set of edges to consider.
     * @param edgeSamples
     *            A map used to associate join path segments (expressed as an
     *            ordered array of bopIds) with {@link EdgeSample}s to avoid
     *            redundant effort.
     * 
     * @return The set of paths which survived pruning in this round.
     * 
     * @throws Exception
     */
    public Path[] expand(final QueryEngine queryEngine, int limitIn,
            final int round, final Path[] a,
            final Map<PathIds, EdgeSample> edgeSamples) throws Exception {

        if (queryEngine == null)
            throw new IllegalArgumentException();
        if (limitIn <= 0)
            throw new IllegalArgumentException();
        if (round <= 0)
            throw new IllegalArgumentException();
        if (a == null)
            throw new IllegalArgumentException();
        if (a.length == 0)
            throw new IllegalArgumentException();
        
        // increment the limit by itself in each round.
        final int limit = (round + 1) * limitIn;

        if (log.isDebugEnabled())
            log.debug("round=" + round + ", limit=" + limit
                    + ", #paths(in)=" + a.length);

        /*
         * Re-sample the vertices which are the initial vertex of any of the
         * existing paths.
         * 
         * Note: We do not need to resample vertices unless they are the first
         * vertex in some path. E.g., the initial vertices from which we start.
         * The inputs to an EdgeSample are always either the sample of an
         * initial vertex or the sample of a prior cutoff join in the join
         * path's own history.
         * 
         * Note: A request to re-sample a vertex is a NOP unless the limit has
         * been increased since the last time the vertex was sampled. It is also
         * a NOP if the vertex has been fully materialized.
         */
        if (log.isDebugEnabled())
            log.debug("Re-sampling in-use vertices: limit=" + limit);

        for (Path x : a) {

            x.vertices[0].sample(queryEngine, limit, sampleType);

        }

        /*
         * Re-sample the cutoff join for each edge in each of the existing
         * paths using the newly re-sampled vertices.
         * 
         * Note: The only way to increase the accuracy of our estimates for
         * edges as we extend the join paths is to re-sample each edge in
         * the join path in path order.
         * 
         * Note: An edge must be sampled for each distinct join path prefix
         * in which it appears within each round. However, it is common for
         * surviving paths to share a join path prefix, so do not re-sample
         * a given path prefix more than once per round. 
         */
        if (log.isDebugEnabled())
            log.debug("Re-sampling in-use path segments: limit=" + limit);

        for (Path x : a) {

            // The cutoff join sample of the one step shorter path segment.
            EdgeSample priorEdgeSample = null;

            for (int segmentLength = 2; segmentLength <= x.vertices.length; segmentLength++) {

                // Generate unique key for this join path segment.
                final PathIds ids = new PathIds(BOpUtility.getPredIds(x
                        .getPathSegment(segmentLength)));

                // Look for sample for this path in our cache.
                EdgeSample edgeSample = edgeSamples.get(ids);

                if (edgeSample != null && edgeSample.limit < limit
                        && !edgeSample.isExact()) {
                    if (log.isDebugEnabled())
                        log.debug("Will resample at higher limit: " + ids);
                    // Time to resample this edge.
                    edgeSamples.remove(ids);
                    edgeSample = null;
                }

                if (priorEdgeSample == null) {

                    /*
                     * This is the first edge in the path.
                     * 
                     * Test our local table of join path segment estimates to
                     * see if we have already re-sampled that edge. If not, then
                     * re-sample it now.
                     */

                    assert segmentLength == 2;

                    if (edgeSample == null) {

                        /*
                         * Re-sample the 1st edge in the join path, updating the
                         * sample on the edge as a side-effect. The cutoff
                         * sample is based on the vertex sample for the minimum
                         * cardinality vertex.
                         */

                        edgeSample = Path.cutoffJoin(//
                                queryEngine, limit,//
                                x.getPathSegment(2),// 1st edge.
                                C,// constraints
                                x.vertices[0].sample// source sample.
                                );

                        // Cache the sample.
                        if (edgeSamples.put(ids, edgeSample) != null)
                            throw new AssertionError();

                    }

                    // Save sample. It will be used to re-sample the next edge.
                    priorEdgeSample = edgeSample;

                } else {

                    /*
                     * The path segment is at least 3 vertices long.
                     */
                    assert ids.length() >= 3;

                    if (edgeSample == null) {

                        /*
                         * This is some N-step edge in the path, where N is
                         * greater than ONE (1). The source vertex is the vertex
                         * which already appears in the prior edges of this join
                         * path. The target vertex is the next vertex which is
                         * visited by the join path. The sample passed in is the
                         * prior edge sample -- that is, the sample from the
                         * path segment without the target vertex. This is the
                         * sample that we just updated when we visited the prior
                         * edge of the path.
                         */

                        edgeSample = Path.cutoffJoin(queryEngine,//
                                limit,//
                                x.getPathSegment(ids.length()),//
                                C, // constraints
                                priorEdgeSample//
                                );

                        if (log.isDebugEnabled())
                            log.debug("Resampled: " + ids + " : " + edgeSample);

                        if (edgeSamples.put(ids, edgeSample) != null)
                            throw new AssertionError();

                    }

                    // Save sample. It will be used to re-sample the next edge.
                    priorEdgeSample = edgeSample;

                }

            } // next path prefix in Path [x]

            if (priorEdgeSample == null)
                throw new AssertionError();

            // Save the result on the path.
            x.edgeSample = priorEdgeSample;

        } // next Path [x].

        /*
         * Expand each path one step from each vertex which branches to an
         * unused vertex.
         */

        if (log.isDebugEnabled())
            log.debug("Expanding paths: limit=" + limit + ", #paths(in)="
                    + a.length);

        final List<Path> tmp = new LinkedList<Path>();

        for (Path x : a) {

            /*
             * The set of vertices used to expand this path in this round.
             */
            final Set<Vertex> used = new LinkedHashSet<Vertex>();

            {

                /*
                 * Any vertex which (a) does not appear in the path to be
                 * extended; (b) has not already been used to extend the path;
                 * and (c) does not share any variables indirectly via
                 * constraints is added to this collection.
                 * 
                 * If we are not able to extend the path at least once using a
                 * constrained join then we will use this collection as the
                 * source of unconnected edges which need to be used to extend
                 * the path.
                 */
                final Set<Vertex> nothingShared = new LinkedHashSet<Vertex>();
                
                // Consider all vertices.
                for (Vertex tVertex : V) {

                    // Figure out which vertices are already part of this path.
                    final boolean vFound = x.contains(tVertex);

                    if (vFound) {
                        // Vertex is already part of this path.
                        if (log.isTraceEnabled())
                            log.trace("Vertex: " + tVertex
                                    + " - already part of this path.");
                        continue;
                    }

                    if (used.contains(tVertex)) {
                        // Vertex already used to extend this path.
                        if (log.isTraceEnabled())
                            log
                                    .trace("Vertex: "
                                            + tVertex
                                            + " - already used to extend this path.");
                        continue;
                    }

                    if (!BOpUtility.canJoinUsingConstraints(//
                            x.getPredicates(),// path
                            tVertex.pred,// vertex
                            C// constraints
                            )) {
                        /*
                         * Vertex does not share variables either directly
                         * or indirectly.
                         */
                        if (log.isTraceEnabled())
                            log
                                    .trace("Vertex: "
                                            + tVertex
                                            + " - unconstrained join for this path.");
                        nothingShared.add(tVertex);
                        continue;
                    }

                    // add the new vertex to the set of used vertices.
                    used.add(tVertex);

                    // Extend the path to the new vertex.
                    final Path p = x.addEdge(queryEngine, limit,
                            tVertex, /*dynamicEdge,*/ C);

                    // Add to the set of paths for this round.
                    tmp.add(p);

                    // Record the sample for the new path.
                    if (edgeSamples.put(new PathIds(p.getVertexIds()),
                            p.edgeSample) != null)
                        throw new AssertionError();

                    if (log.isTraceEnabled())
                        log.trace("Extended path with dynamic edge: vnew="
                                + tVertex.pred.getId() + ", new path=" + p);

                } // next vertex.

                if (tmp.isEmpty()) {

                    /*
                     * No constrained joins were identified so we must consider
                     * edges which represent fully unconstrained joins.
                     */

                    assert !nothingShared.isEmpty();

                    /*
                     * Choose any vertex from the set of those which do
                     * not share any variables with the join path. Since
                     * all of these are fully unconstrained joins we do
                     * not want to expand the join path along multiple
                     * edges in this iterator, just along a single
                     * unconstrained edge.
                     */
                    final Vertex tVertex = nothingShared.iterator().next();
                    
                    // Extend the path to the new vertex.
                    final Path p = x.addEdge(queryEngine, limit,
                            tVertex,/*dynamicEdge*/ C);

                    // Add to the set of paths for this round.
                    tmp.add(p);

                    if (log.isTraceEnabled())
                        log.trace("Extended path with dynamic edge: vnew="
                                + tVertex.pred.getId() + ", new path=" + p);

                }

            } 

        } // next path

        final Path[] paths_tp1 = tmp.toArray(new Path[tmp.size()]);

        final Path[] paths_tp1_pruned = pruneJoinPaths(paths_tp1);

        if (log.isDebugEnabled())
            log.debug("\n*** round=" + round + ", limit=" + limit
                    + " : generated paths\n"
                    + JGraph.showTable(paths_tp1, paths_tp1_pruned));

        if (log.isInfoEnabled())
            log.info("\n*** round=" + round + ", limit=" + limit
                    + ": paths{in=" + a.length + ",considered="
                    + paths_tp1.length + ",out=" + paths_tp1_pruned.length
                    + "}\n" + JGraph.showTable(paths_tp1_pruned));

        return paths_tp1_pruned;

    }

    /**
     * Return the {@link Vertex} whose {@link IPredicate} is associated with
     * the given {@link BOp.Annotations#BOP_ID}.
     * 
     * @param bopId
     *            The bop identifier.
     * @return The {@link Vertex} -or- <code>null</code> if there is no such
     *         vertex in the join graph.
     */
    public Vertex getVertex(final int bopId) {
        for (Vertex v : V) {
            if (v.pred.getId() == bopId)
                return v;
        }
        return null;
    }

    /**
     * Obtain a sample and estimated cardinality (fast range count) for each
     * vertex.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The sample size.
     */
    public void sampleAllVertices(final QueryEngine queryEngine, final int limit) {

        for (Vertex v : V) {

            v.sample(queryEngine, limit, sampleType);

        }

    }

    /**
     * Estimate the cardinality of each edge. This is only invoked by
     * {@link #round0(QueryEngine, int, int)} when it is trying to select the
     * minimum cardinality edges which it will use to create the initial set of
     * join paths from which the exploration will begin.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The sample size.
     * 
     * @throws Exception
     * 
     *             TODO It would be very interesting to see the variety and/or
     *             distribution of the values bound when the edge is sampled.
     *             This can be easily done using a hash map with a counter. That
     *             could tell us a lot about the cardinality of the next join
     *             path (sampling the join path also tells us a lot, but it does
     *             not explain it as much as seeing the histogram of the bound
     *             values). I believe that there are some interesting online
     *             algorithms for computing the N most frequent observations and
     *             the like which could be used here.
     */
    private Path[] estimateInitialEdgeWeights(final QueryEngine queryEngine,
            final int limit) throws Exception {

        final List<Path> paths = new LinkedList<Path>();

        /*
         * Examine all unordered vertex pairs (v1,v2) once. If any vertex has
         * not been sampled, then it is ignored. For each unordered vertex pair,
         * determine which vertex has the minimum cardinality and create an
         * ordered vertex pair (v,vp) such that [v] is the vertex with the
         * lesser cardinality. Finally, perform a cutoff join of (v,vp) and
         * create a join path with a single edge (v,vp) using the sample
         * obtained from the cutoff join.
         */
 
        for (int i = 0; i < V.length; i++) {

            final Vertex v1 = V[i];
            
            if (v1.sample == null) {
                
                /*
                 * We can only estimate the cardinality of edges connecting
                 * vertices for which samples were obtained.
                 */
            
                continue;
            
            }
 
            for (int j = i + 1; j < V.length; j++) {
            
                final Vertex v2 = V[j];
                
                if (v2.sample == null) {
                
                    /*
                     * We can only estimate the cardinality of edges connecting
                     * vertices for which samples were obtained.
                     */
                    
                    continue;
                    
                }

                /*
                 * Figure out which vertex has the smaller cardinality. The sample
                 * of that vertex is used since it is more representative than the
                 * sample of the other vertex.
                 */
                
                final Vertex v, vp;
                if (v1.sample.estimatedCardinality < v2.sample.estimatedCardinality) {
                    v = v1;
                    vp = v2;
                } else {
                    v = v2;
                    vp = v1;
                }
                
                if (!BOpUtility.canJoinUsingConstraints(
                        new IPredicate[] { v.pred }, vp.pred, C)) {

                    /*
                     * If there are no shared variables, either directly or
                     * indirectly via the constraints, then we can not use this
                     * as an initial edge.
                     * 
                     * @todo UNIT TEST : correct rejection of initial paths for
                     * vertices which are unconstrained joins.
                     * 
                     * @todo UNIT TEST : correct acceptance of initial paths for
                     * vertices which are unconstrained joins IFF there are no
                     * constrained joins in the join graph.
                     */
                    
                    continue;
                    
                }
                        
                // The path segment
                final IPredicate<?>[] preds = new IPredicate[] { v.pred, vp.pred };

                // cutoff join of the edge (v,vp)
                final EdgeSample edgeSample = Path.cutoffJoin(
                        queryEngine,// 
                        limit, // sample limit
                        preds, // ordered path segment.
                        C, // constraints
                        v.sample // sourceSample
                        );

                final Path p = new Path(v, vp, edgeSample);

                paths.add(p);

            }
        
        }
        
        return paths.toArray(new Path[paths.size()]);

    }

    /**
     * Prune paths which are dominated by other paths. Paths are extended in
     * each round. Paths from previous rounds are always pruned. Of the new
     * paths in each round, the following rule is applied to prune the
     * search to just those paths which are known to dominate the other
     * paths covering the same set of vertices:
     * <p>
     * If there is a path, [p] != [p1], where [p] is an unordered variant of
     * [p1] (that is the vertices of p are the same as the vertices of p1),
     * and the cumulative cost of [p] is LTE the cumulative cost of [p1],
     * then [p] dominates (or is equivalent to) [p1] and p1 should be
     * pruned.
     * 
     * @param a
     *            A set of paths.
     * 
     * @return The set of paths with all dominated paths removed.
     */
    public Path[] pruneJoinPaths(final Path[] a) {
        /*
         * Find the length of the longest path(s). All shorter paths are
         * dropped in each round.
         */
        int maxPathLen = 0;
        for (Path p : a) {
            if (p.vertices.length > maxPathLen) {
                maxPathLen = p.vertices.length;
            }
        }
        final StringBuilder sb = new StringBuilder();
        final Formatter f = new Formatter(sb);
        final Set<Path> pruned = new LinkedHashSet<Path>();
        for (int i = 0; i < a.length; i++) {
            final Path Pi = a[i];
            if (Pi.edgeSample == null)
                throw new RuntimeException("Not sampled: " + Pi);
            if (Pi.vertices.length < maxPathLen) {
                /*
                 * Only the most recently generated set of paths survive to
                 * the next round.
                 */
                pruned.add(Pi);
                continue;
            }
            if (pruned.contains(Pi))
                continue;
            for (int j = 0; j < a.length; j++) {
                if (i == j)
                    continue;
                final Path Pj = a[j];
                if (Pj.edgeSample == null)
                    throw new RuntimeException("Not sampled: " + Pj);
                if (pruned.contains(Pj))
                    continue;
                final boolean isPiSuperSet = Pi.isUnorderedVariant(Pj);
                if (!isPiSuperSet) {
                    // Can not directly compare these join paths.
                    continue;
                }
                final long costPi = Pi.cumulativeEstimatedCardinality;
                final long costPj = Pj.cumulativeEstimatedCardinality;
                final boolean lte = costPi <= costPj;
                List<Integer> prunedByThisPath = null;
                if (lte) {
                    prunedByThisPath = new LinkedList<Integer>();
                    if (pruned.add(Pj))
                        prunedByThisPath.add(j);
                    for (int k = 0; k < a.length; k++) {
                        final Path x = a[k];
                        if (x.beginsWith(Pj)) {
                            if (pruned.add(x))
                                prunedByThisPath.add(k);
                        }
                    }
                }
                if (log.isDebugEnabled()) {
                    f
                            .format(
                                    "Comparing: P[%2d] with P[%2d] : %10d %2s %10d %s",
                                    i, j, costPi, (lte ? "<=" : ">"),
                                    costPj, lte ? " *** pruned "
                                            + prunedByThisPath : "");
                    log.debug(sb);
                    sb.setLength(0);
                }
            } // Pj
        } // Pi
        final Set<Path> keep = new LinkedHashSet<Path>();
        for (Path p : a) {
            if (pruned.contains(p))
                continue;
            keep.add(p);
        }
        final Path[] b = keep.toArray(new Path[keep.size()]);
        return b;
    }

    /**
     * Comma delimited table showing the estimated join hit ratio, the estimated
     * cardinality, and the set of vertices for each of the specified join
     * paths.
     * 
     * @param a
     *            An array of join paths.
     * 
     * @return A table with that data.
     */
    static public String showTable(final Path[] a) {

        return showTable(a, null/* pruned */);
        
    }

    /**
     * Comma delimited table showing the estimated join hit ratio, the estimated
     * cardinality, and the set of vertices for each of the specified join
     * paths.
     * 
     * @param a
     *            A set of paths (typically those before pruning).
     * @param pruned
     *            The set of paths after pruning (those which were retained)
     *            (optional). When given, the paths which were pruned are marked
     *            in the table.
     * 
     * @return A table with that data.
     */
    static public String showTable(final Path[] a,final Path[] pruned) {
        final StringBuilder sb = new StringBuilder();
        final Formatter f = new Formatter(sb);
        f.format("%-6s %10s%1s * %10s (%6s %6s %6s) = %10s%1s : %10s %10s",
                "path",//
                "sourceCard",//
                "",// sourceSampleExact
                "f",//
                "in",//
                "read",//
                "out",//
                "estCard",//
                "",// estimateIs(Exact|LowerBound|UpperBound)
                "sumEstCard",//
                "joinPath\n"
                );
        for (int i = 0; i < a.length; i++) {
            final Path x = a[i];
            // true iff the path survived pruning.
            Boolean prune = null;
            if (pruned != null) {
                prune = Boolean.TRUE;
                for (Path y : pruned) {
                    if (y == x) {
                        prune = Boolean.FALSE;
                        break;
                    }
                }
            }
            final EdgeSample edgeSample = x.edgeSample;
            if (edgeSample == null) {
                f.format("%6d %10s%1s * %10s (%6s %6s %6s) = %10s%1s : %10s",//
                            i, "N/A", "", "N/A", "N/A", "N/A", "N/A", "N/A", "",
                                "N/A");
            } else {
                f.format("%6d %10d%1s * % 10.2f (%6d %6d %6d) = % 10d%1s : % 10d", //
                        i,//
                        edgeSample.sourceSample.estimatedCardinality,//
                        edgeSample.sourceSample.estimateEnum.getCode(),//
                        edgeSample.f,//
                        edgeSample.inputCount,//
                        edgeSample.tuplesRead,//
                        edgeSample.outputCount,//
                        edgeSample.estimatedCardinality,//
                        edgeSample.estimateEnum.getCode(),//
                        x.cumulativeEstimatedCardinality//
                        );
            }
            sb.append("  [");
            for (Vertex v : x.getVertices()) {
                f.format("%2d ", v.pred.getId());
            }
            sb.append("]");
            if (pruned != null) {
                if (prune)
                    sb.append(" pruned");
            }
            // for (Edge e : x.edges)
            // sb.append(" (" + e.v1.pred.getId() + " " + e.v2.pred.getId()
            // + ")");
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Show the details of a join path, including the estimated cardinality and
     * join hit ratio for each step in the path.
     * 
     * @param p
     *            The join path.
     * @param edgeSamples
     *            A map containing the samples utilized by the {@link Path}.
     */
    static String showPath(final Path x, final Map<PathIds, EdgeSample> edgeSamples) {
        if (x == null)
            throw new IllegalArgumentException();
        final StringBuilder sb = new StringBuilder();
        final Formatter f = new Formatter(sb);
    {
            /*
             * @todo show limit on samples?
             */
            f.format("%6s %10s%1s * %10s (%6s %6s %6s) = %10s%1s : %10s",//
                    "vertex",
                    "sourceCard",//
                    "",// sourceSampleExact
                    "f",//
                    "in",//
                    "read",//
                    "out",//
                    "estCard",//
                    "",// estimateIs(Exact|LowerBound|UpperBound)
                    "sumEstCard"//
                    );
            long sumEstCard = 0;
            for (int i = 0; i < x.vertices.length; i++) {
                final int[] ids = BOpUtility
                        .getPredIds(x.getPathSegment(i + 1));
                final int predId = x.vertices[i].pred.getId();
                final SampleBase sample;
                if(i==0) {
                    sample = x.vertices[i].sample;
                } else {
                    // edge sample from the caller's map.
                    sample = edgeSamples.get(new PathIds(ids));
                }
                if (sample != null) {
                    sumEstCard += sample.estimatedCardinality;
                    if (sample instanceof EdgeSample)
                        sumEstCard += ((EdgeSample) sample).tuplesRead;
                }
                sb.append("\n");
                if (sample == null) {
                    f.format("% 6d %10s%1s * %10s (%6s %6s %6s) = %10s%1s : %10s",//
                            predId,//
                            "N/A", "", "N/A", "N/A", "N/A", "N/A", "N/A", "", "N/A");
                } else if(sample instanceof VertexSample) {
                    // Show the vertex sample for the initial vertex.
                    f.format("% 6d %10s%1s * %10s (%6s %6s %6s) = % 10d%1s : %10d",//
                            predId,//
                            "N/A",//sample.sourceSample.estimatedCardinality,//
                            " ",//sample.sourceSample.isExact() ? "E" : "",//
                            " ",//sample.f,//
                            "N/A",//sample.inputCount,//
                            "N/A",//sample.tuplesRead,//
                            "N/A",//sample.outputCount,//
                            sample.estimatedCardinality,//
                            sample.estimateEnum.getCode(),//
                            sumEstCard//
//                          e.cumulativeEstimatedCardinality//
                            );
                } else {
                    // Show the sample for a cutoff join with the 2nd+ vertex.
                    final EdgeSample edgeSample = (EdgeSample)sample;
                    f.format("% 6d %10d%1s * % 10.2f (%6d %6d %6d) = % 10d%1s : %10d",//
                            predId,//
                            edgeSample.sourceSample.estimatedCardinality,//
                            edgeSample.sourceSample.estimateEnum.getCode(),//
                            edgeSample.f,//
                            edgeSample.inputCount,//
                            edgeSample.tuplesRead,//
                            edgeSample.outputCount,//
                            edgeSample.estimatedCardinality,//
                            edgeSample.estimateEnum.getCode(),//
                            sumEstCard//
//                          e.cumulativeEstimatedCardinality//
                            );
                }
            }
        }
        sb.append("\n");
        return sb.toString();
    }
    
} // class JGraph
