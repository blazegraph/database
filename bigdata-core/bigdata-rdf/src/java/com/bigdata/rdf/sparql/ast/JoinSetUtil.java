/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Oct 21, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.optimizers.ASTHashJoinOptimizer;

/**
 * Utility class for join analysis.
 * 
 * TODO Surely we can do some bit math which would be slimmer and faster than
 * managing the IVariable sets?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JoinSetUtil {

    private static final Logger log = Logger
            .getLogger(ASTHashJoinOptimizer.class);

    /**
     * The group.
     */
    public final GraphPatternGroup<IGroupMemberNode> group;

    /**
     * The variables known to be bound on entry to the group.
     */
    public final Set<IVariable<?>> knownBound;

    /**
     * The set of variables which are bound by the time the last required join
     * is done. This includes the {@link #knownBound} variables plus any
     * variables for any required join.
     */
    public final Set<IVariable<?>> eventuallyBoundVars;
    
    /**
     * An array of the vertices for the required joins in the group. The indices
     * into the array are the order in which the required joins were encountered
     * in the group.
     */
    public final IJoinNode[] requiredJoins;

    /** The #of required joins in the group (the {@link #requiredJoins} length). */
    public final int requiredJoinCount;

    /**
     * The FILTERS that can be run on entry to the group (and which should be
     * lifted into the parent).
     */
    public final Set<FilterNode> preFilters;

    /**
     * The FILTERS that will be attached to the required joins for this group.
     */
    public final Set<FilterNode> joinFilters;

    /**
     * The FILTERS that can not be run until the end of the join group.
     */
    public final Set<FilterNode> postFilters;

    /**
     * The set of variables which are bound be each of the vertices for the
     * required joins. The indices into the array are the order in which the
     * requried joins were encountered in the group.
     */
    public final Set<IVariable<?>>[] vars;

    // final Set<IVariable<?>>[] varsWithFilters;

    /**
     * Diagonal matrix for the required joins. A cells having a positive value
     * gives the #of directly shared variables. This will be ZERO (0) if there
     * is no direct join. Only the upper diagonal of the matrix is populated.
     */
    public final int[][] canJoin;

    /**
     * The #of direct joins found for each vertex (summed across
     * {@link #canJoin} matrix.
     */
    public final int[] directJoinCount;

    /**
     * The distinct sets of vertices which are composed solely of (the
     * transitive closure of) joins on directly shared variables. For example
     * 
     * <pre>
     * p(x,y) X p(y,z) x p(z,t)
     * </pre>
     * 
     * Would form a direct join set each join will share a variable directly.
     * Even though there are some vertices which do not share variables
     * directly, they can be joined directly once other joins have been
     * processed. For example <code>p(z,t)</code> can be joined once we have
     * <code>p(y,z)</code>.
     * <p>
     * The number of such direct join sets varies with the query. If all
     * vertices fit into one direct join set, then the query can be pipelined
     * efficiently. If there is more than one such set then we need to look
     * further to see if we can identify joins on indirectly shared variables
     * (shared through a FILTER) which can be used to piece these join sets
     * together. If not, there is an unconstrained cross product in the query.
     */
    public final Set<VertexJoinSet> directJoinSets;

    /**
     * A collection of vertices and the join variables they bind.
     */
    public static class VertexJoinSet {

        /**
         * The set of verticies in this join set. The indices are the order in
         * which the vertices were encountered in the group.
         */
        final public Set<Integer> vertices;

        /**
         * The set of variables bound by at least one join in the join set.
         */
        final public Set<IVariable<?>> joinvars;

        public VertexJoinSet() {
            vertices = new LinkedHashSet<Integer>();
            joinvars = new LinkedHashSet<IVariable<?>>();
        }
        
        public VertexJoinSet(final Set<Integer> vertices,
                final Set<IVariable<?>> joinvars) {
            this.vertices = vertices;
            this.joinvars = joinvars;
        }

        public String toString() {
            return getClass().getSimpleName() + "{vertices=" + vertices
                    + ",joinvars=" + joinvars + "}";
        }
        
        public int hashCode() {
            return vertices.hashCode();
        }
        
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof VertexJoinSet))
                return false;
            final VertexJoinSet t = (VertexJoinSet) o;
            if (!vertices.equals(t.vertices))
                return false;
            if (!joinvars.equals(t.joinvars))
                return false;
            return true;
        }

    }
    
    /**
     * 
     * @param sa
     * @param knownBound Any variables known to be bound on entry to the group.
     * @param group The group.
     */
    public JoinSetUtil(final StaticAnalysis sa,
            Set<IVariable<?>> knownBound, 
            final GraphPatternGroup<IGroupMemberNode> group) {

        this.group = group;
        
        if (knownBound == null) {
            knownBound = new LinkedHashSet<IVariable<?>>();
        }
        this.knownBound = knownBound;
        
        /*
         * Create an array of the vertices for the required joins.
         */

        // Locate all the required joins.
        {
            final List<IJoinNode> list = new LinkedList<IJoinNode>();
            for (IGroupMemberNode m : group) {
                if (m instanceof IJoinNode) {
                    final IJoinNode j = (IJoinNode) m;
                    if (!j.isOptional())
                        list.add(j);
                }
            }
            requiredJoinCount = list.size();
            requiredJoins = list.toArray(new IJoinNode[requiredJoinCount]);
        }

        /*
         * Create an array of the variables for each of the required join
         * vertices.
         */
        eventuallyBoundVars = new LinkedHashSet<IVariable<?>>();
        {
            vars = new Set[requiredJoinCount];
            for (int i = 0; i < requiredJoinCount; i++) {
                final IJoinNode j = requiredJoins[i];
                // anything bound by this join.
                final Set<IVariable<?>> tmp = sa.getSpannedVariables((BOp) j,
                        new LinkedHashSet<IVariable<?>>());
                tmp.addAll(knownBound); // plus anything bound on entry to the group.
                eventuallyBoundVars.addAll(vars[i] = tmp);
            }
        }

        /*
         * Identify the FILTERs which can run against the required joins. This
         * will include both pre-filters (which really should be lifted out) and
         * join-filters (which are satisified by the time we have run all the
         * required joins).
         */
        {
            preFilters = new LinkedHashSet<FilterNode>();
            joinFilters = new LinkedHashSet<FilterNode>();
            postFilters = new LinkedHashSet<FilterNode>();
            for (IGroupMemberNode m : group) {
                if (m instanceof FilterNode) {
                    final FilterNode f = (FilterNode) m;
                    if (sa.isFullyBound(f, knownBound)) {
                        /*
                         * The variables for this filter are already bound on
                         * entry.
                         */
                        preFilters.add(f);
                        continue;
                    }
                    if (sa.isFullyBound(f, eventuallyBoundVars)) {
                        /*
                         * The variables for this filter will be fully bound by
                         * the time we are done with the required joins.
                         */
                        joinFilters.add(f);
                        continue;
                    }
                    /*
                     * The variables for this filter will not be bound by the
                     * end of the required joins. If there are optional joins,
                     * then the filter might be able to succeed if it is run
                     * after the last optional join which could bind a variable
                     * which it needs and which is not bound by a required join
                     * in this group.
                     */
                    postFilters.add(f);
                }
            }
        }
        
        /*
         * Diagonal matrix for the required joins
         * 
         * In the first pass, we set a cell to the #of directly shared join
         * variables between each pair of vertices. This will be ZERO (0) if
         * there is no direct join. Only the upper diagonal of the matrix is
         * populated.
         */
        canJoin = new int[requiredJoinCount][requiredJoinCount];
        // The #of direct joins found for each vertex (summed across canJoin
        // matrix).
        directJoinCount = new int[requiredJoinCount];
        {
            for (int i = 0; i < requiredJoinCount; i++) {
                for (int j = i + 1; j < requiredJoinCount; j++) {
                    final Set<IVariable<?>> sharedVars = new HashSet<IVariable<?>>();
                    sharedVars.addAll(vars[i]);
                    sharedVars.retainAll(vars[j]);
                    if ((canJoin[i][j] = sharedVars.size()) > 0) {
                        // #of times there is a direct join for this vertex.
                        directJoinCount[i]++;
                        directJoinCount[j]++;
                    }
                }
            }
        }
        if (log.isDebugEnabled()) {
            // TODO Could be moved into a toString() for this class.
            log.debug("\ncanJoin:\n" + toString(canJoin));
            log.debug("\ndirectJoinCount:\n" + Arrays.toString(directJoinCount));
        }

        /*
         * The distinct sets of vertices which composed solely of (the
         * transitive closure of) direct joins.
         */
        directJoinSets = calcDirectJoinSets();
              
    }

    /**
     * Identify the subsets of the vertices which can join directly.
     * <p>
     * Start with each row and create a set containing the variables for that
     * vertex and a set of the vertices with which it has joined (initially this
     * contains just that row). For each column that it joins with, add in all
     * variables for that column and add the column to the set of joined
     * vertices. Once all columns have been processed for the initial row, we
     * have the first join set of vertices.
     * <p>
     * Repeat for the first row not found in that join set until all joins have
     * been incorporated into a join set. This gives us the #of join sets which
     * do not overlap based on directly shared variables. It would be ideal to
     * have one such join set. Where there is more than one there is clearly a
     * question concerning which join set we should handle first.
     */
    private Set<VertexJoinSet> calcDirectJoinSets() {
        
        final Set<VertexJoinSet> joinSets = new LinkedHashSet<VertexJoinSet>();

        // The set of vertices which have been consumed.
        final Set<Integer> used = new LinkedHashSet<Integer>();
        for (int i = 0; i < requiredJoinCount; i++) {
            if (used.contains(i))
                continue;
            // This join set.
            final VertexJoinSet joinSet = new VertexJoinSet();
            joinSet.vertices.add(i);
            used.add(i);
            // The initial vertex for this join set.
            joinSet.joinvars.addAll(vars[i]);
            expandJoinSet(i, joinSet.vertices, used, joinSet.joinvars);
            joinSets.add(joinSet);
            if (log.isInfoEnabled())
                log.info("joinSet: " + joinSet.vertices + " on "
                        + joinSet.joinvars);
        }
        // All vertices must have been used.
        assert used.size() == requiredJoinCount : "used=" + used
                + ", but requiredJoinCount=" + requiredJoinCount;
        return joinSets;
    }
    
    private void expandJoinSet(final int i, final Set<Integer> joinSet,
            final Set<Integer> used, final Set<IVariable<?>> joinvars) {
        // Build up the join set
        for (int j = 0; j < requiredJoinCount; j++) {
            if (i == j)
                continue;
            if (used.contains(j))
                continue;
            if ((i < j && canJoin[i][j] > 0) || (i > j && canJoin[j][i] > 0)) {
                joinSet.add(j);
                used.add(j);
                joinvars.addAll(vars[j]);
                // And visit anything which canJoin() with j (recursion).
                expandJoinSet(j, joinSet, used, joinvars);
            }
        }
    }

    static private String toString(final int[][] data) {

        final StringBuilder sb = new StringBuilder();

        final int n = data.length;

        for (int i = 0; i < n; i++) {

            for (int j = 0; j < n; j++) {

                if (j <= i)
                    sb.append("-");
                else
                    sb.append(data[i][j]);

                sb.append(" ");

            }

            sb.append("\n");

        }

        return sb.toString();

    }

}
