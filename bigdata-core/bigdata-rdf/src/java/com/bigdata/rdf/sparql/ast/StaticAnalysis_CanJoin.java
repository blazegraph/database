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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.sparql.ast.ssets.ISolutionSetManager;

/**
 * Class provides methods to determine if two {@link IJoinNode}s can join on a
 * shared variable and if they can join on an variable which is indirectly
 * shared through constraints which can be attached to that join.
 * <p>
 * Note: This is a port of com.bigdata.bop.joinGraph.PartitionedJoinGroup
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: StaticAnalysis_CanJoin.java 5378 2011-10-20 20:37:08Z
 *          thompsonbry $
 */
public abstract class StaticAnalysis_CanJoin extends StaticAnalysisBase {

    private static final Logger log = Logger.getLogger(StaticAnalysis.class);

    /**
     * 
     * @param queryRoot
     *            The root of the query. We need to have this on hand in order
     *            to resolve {@link NamedSubqueryInclude}s during static
     *            analysis.
     * @param evaluationContext
	 *            The evaluation context provides access to the
	 *            {@link ISolutionSetStats} and the {@link ISolutionSetManager} for
	 *            named solution sets.
     */
    public StaticAnalysis_CanJoin(final QueryRoot queryRoot,
            final IEvaluationContext evaluationContext) {

        super(queryRoot, evaluationContext);
        
    }
    
    /**
     * Return <code>true</code> iff two predicates can join on the basis of at
     * least one variable which is shared directly by those predicates. Only the
     * operands of the predicates are considered.
     * <p>
     * Note: This method will only identify joins where the predicates directly
     * share at least one variable. However, joins are also possible when the
     * predicates share variables via one or more constraint(s). Use
     * {@link canJoinUsingConstraints} to identify such joins.
     * <p>
     * Note: Any two predicates may join regardless of the presence of shared
     * variables. However, such joins will produce the full cross product of the
     * binding sets selected by each predicate. As such, they should be run last
     * and this method will not return <code>true</code> for such predicates.
     * 
     * @param p1
     *            A predicate.
     * @param p2
     *            Another predicate.
     * 
     * @return <code>true</code> iff the predicates share at least one variable
     *         as an operand.
     * 
     * @throws IllegalArgumentException
     *             if the two either reference is <code>null</code>.
     */
    public boolean canJoin(final IJoinNode p1, final IJoinNode p2) {

        if (p1 == null)
            throw new IllegalArgumentException();

        if (p2 == null)
            throw new IllegalArgumentException();

        final Set<IVariable<?>> set1 = getSpannedVariables((BOp) p1,
                false/* filters */, new LinkedHashSet<IVariable<?>>());

        final Set<IVariable<?>> set2 = getSpannedVariables((BOp) p2,
                false/* filters */, new LinkedHashSet<IVariable<?>>());
        
        // The difference gives us the shared variables.
        set1.retainAll(set2);

        final boolean nothingShared = set1.isEmpty();

        final boolean canJoin = !nothingShared;

        if (log.isDebugEnabled()) {
            if (!nothingShared) {
                log.debug("No directly shared variables: p1=" + p1 + ", p2="
                        + p2);
            } else {
                log.debug("Can join: sharedVars=" + set1);
            }
        }

        return canJoin;

    }

    /**
     * Return <code>true</code> iff a predicate may be used to extend a join
     * path on the basis of at least one variable which is shared either
     * directly or via one or more constraints which may be attached to the
     * predicate when it is added to the join path. The join path is used to
     * decide which variables are known to be bound, which in turn decides which
     * constraints may be run. Unlike the case when the variable is directly
     * shared between the two predicates, a join involving a constraint requires
     * us to know which variables are already bound so we can know when the
     * constraint may be attached.
     * <p>
     * Note: Use {@link StaticAnalysis#canJoin(IJoinNode, IJoinNode)} instead to
     * identify joins based on a variable which is directly shared.
     * <p>
     * Note: Any two predicates may join regardless of the presence of shared
     * variables. However, such joins will produce the full cross product of the
     * binding sets selected by each predicate. As such, they should be run last
     * and this method will not return <code>true</code> for such predicates.
     * 
     * @param path
     *            A join path containing at least one predicate.
     * @param vertex
     *            A predicate which is being considered as an extension of that
     *            join path.
     * @param constraints
     *            A set of zero or more constraints (optional). Constraints are
     *            attached dynamically once the variables which they use are
     *            bound. Hence, a constraint will always share a variable with
     *            any predicate to which it is attached. If any constraints are
     *            attached to the given vertex and they share a variable which
     *            has already been bound by the join path, then the vertex may
     *            join with the join path even if it does not directly bind that
     *            variable.
     * 
     * @return <code>true</code> iff the vertex can join with the join path via
     *         a shared variable.
     * 
     * @throws IllegalArgumentException
     *             if the join path is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the join path is empty.
     * @throws IllegalArgumentException
     *             if any element in the join path is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the vertex is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the vertex is already part of the join path.
     * @throws IllegalArgumentException
     *             if any element in the optional constraints array is
     *             <code>null</code>.
     */
    public boolean canJoinUsingConstraints(final IJoinNode[] path,
            final IJoinNode vertex, final FilterNode[] constraints) {

        /*
         * Check arguments.
         */
        if (path == null)
            throw new IllegalArgumentException();
        if (vertex == null)
            throw new IllegalArgumentException();
        // constraints MAY be null.
        if (path.length == 0)
            throw new IllegalArgumentException();
        {
            for (IJoinNode p : path) {
                if (p == null)
                    throw new IllegalArgumentException();
                if (vertex == p)
                    throw new IllegalArgumentException();
            }
        }

        /*
         * Find the set of variables which are known to be bound because they
         * are referenced as operands of the predicates in the join path.
         */
        final Set<IVariable<?>> knownBound = new LinkedHashSet<IVariable<?>>();
        {
        
            for (IJoinNode p : path) {

                getSpannedVariables((BOp) p, false/* filters */, knownBound);

            }

        }

        /*
         * If the given predicate directly shares a variable with any of the
         * predicates in the join path, then we can return immediately.
         */
        {

            final Set<IVariable<?>> vset = getSpannedVariables((BOp) vertex,
                    new LinkedHashSet<IVariable<?>>());

            vset.retainAll(knownBound);

            if (!vset.isEmpty()) {

                if (log.isDebugEnabled())
                    log.debug("Can join: sharedVars=" + vset + ", path="
                            + Arrays.toString(path) + ", vertex=" + vertex);

                return true;

            }

        }

        if (constraints == null) {

            // No opportunity for a constraint based join.

            if (log.isDebugEnabled())
                log.debug("No directly shared variable: path="
                        + Arrays.toString(path) + ", vertex=" + vertex);

            return false;

        }

        /*
         * Find the set of constraints which can run with the vertex given the
         * join path.
         */
        {

            // Extend the new join path.
            final IJoinNode[] newPath = new IJoinNode[path.length + 1];

            System.arraycopy(path/* src */, 0/* srcPos */, newPath/* dest */,
                    0/* destPos */, path.length);

            newPath[path.length] = vertex;

            /*
             * Find the constraints that will run with each vertex of the new
             * join path.
             */
            final FilterNode[][] constraintRunArray = getJoinGraphConstraints(
                    newPath, constraints, null/*knownBound*/,
                    true/*pathIsComplete*/
                    );

            /*
             * Consider only the constraints attached to the last vertex in the
             * new join path. All of their variables will be bound since (by
             * definition) a constraint may not run until its variables are
             * bound. If any of the constraints attached to that last share any
             * variables which were already known to be bound in the caller's
             * join path, then the vertex can join (without of necessity being a
             * full cross product join).
             */
            final FilterNode[] vertexConstraints = constraintRunArray[path.length];

            for (FilterNode c : vertexConstraints) {

                // consider all variables spanned by the constraint.
                final Set<IVariable<?>> vset = getSpannedVariables(c,
                        true/* filters */, new LinkedHashSet<IVariable<?>>());

                vset.retainAll(knownBound);

                if (!vset.isEmpty()) {

                    if (log.isDebugEnabled())
                        log.debug("Can join: sharedVars=" + vset + ", path="
                                + Arrays.toString(path) + ", vertex=" + vertex
                                + ", constraint=" + c);

                    return true;

                }

            }

        }

        if (log.isDebugEnabled())
            log.debug("No shared variable: path=" + Arrays.toString(path)
                    + ", vertex=" + vertex + ", constraints="
                    + Arrays.toString(constraints));

        return false;

    }

    /**
     * Given a join path, return the set of constraints to be associated with
     * each join in that join path. Only those constraints whose variables are
     * known to be bound will be attached.
     * 
     * @param path
     *            The join path.
     * @param joinGraphConstraints
     *            The constraints to be applied to the join path (optional).
     * @param knownBoundVars
     *            Variables that are known to be bound as inputs to this join
     *            graph (parent queries).
     * @param pathIsComplete
     *            <code>true</code> iff the <i>path</i> represents a complete
     *            join path. When <code>true</code>, any constraints which have
     *            not already been attached will be attached to the last predicate
     *            in the join path.
     * 
     * @return The constraints to be paired with each element of the join path.
     * 
     * @throws IllegalArgumentException
     *             if the join path is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the join path is empty.
     * @throws IllegalArgumentException
     *             if any element of the join path is <code>null</code>.
     * @throws IllegalArgumentException
     *             if any element of the join graph constraints is
     *             <code>null</code>.
     */
    public FilterNode[][] getJoinGraphConstraints(
            final IJoinNode[] path,//
            final FilterNode[] joinGraphConstraints,//
            Set<IVariable<?>> boundVars,// 
            final boolean pathIsComplete//
            ) {

        if (path == null)
            throw new IllegalArgumentException();
        
        if (path.length == 0)
            throw new IllegalArgumentException();

        if (boundVars == null)
            boundVars = new LinkedHashSet<IVariable<?>>();
        
        // the set of constraints for each predicate in the join path.
        final FilterNode[][] ret = new FilterNode[path.length][];

        /*
         * For each predicate in the path in the given order, figure out which
         * constraint(s) would attach to that predicate based on which variables
         * first become bound with that predicate. For the last predicate in the
         * given join path, we return that set of constraints.
         */

        // the set of constraints which have been consumed.
        final Set<FilterNode> used = new LinkedHashSet<FilterNode>();
        
        for (int i = 0; i < path.length; i++) {

            // a predicate in the path.
            final IJoinNode p = path[i];

            if (p == null)
                throw new IllegalArgumentException();
            
            // the constraints for the current predicate in the join path.
            final List<FilterNode> constraints = new LinkedList<FilterNode>();
            
            
//            /*
//             * Visit the variables used by the predicate (and bound by it since
//             * it is not an optional predicate) and add them into the total set
//             * of variables which are bound at this point in the join path.
//             */
//            getSpannedVariables((BOp) p, boundVars);
            // above does not work if p is a Union nor, I suspect, a Minus - jjc
            // tring this next line as an alternative - jjc.
            getDefinitelyProducedBindings(p, boundVars, true);

            if (joinGraphConstraints != null) {

                // consider each constraint.
                for (FilterNode c : joinGraphConstraints) {

                    if (c == null)
                        throw new IllegalArgumentException();

                    if (used.contains(c)) {
                        /*
                         * Skip constraints which were already assigned to
                         * predicates before this one in the join path.
                         */
                        continue;
                    }

                    boolean attach = false;
                    
                    if (pathIsComplete && i == path.length - 1) {
                        
                        // attach all unused constraints to last predicate
                        attach = true;
                        
                    } else {
                    
                        /*
                         * true iff all variables used by this constraint are
                         * bound at this point in the join path.
                         */
                        final boolean allVarsBound;
    
                        // all variables used by the constraint.
                        final Set<IVariable<?>> vset = getSpannedVariables(c,
                                true/* filters */,
                                new LinkedHashSet<IVariable<?>>());

                        // remove all bound variables.
                        vset.removeAll(boundVars);
                        
                        // the constraint can be attached if [vset] is empty.
                        allVarsBound = vset.isEmpty();
                        
                        attach = allVarsBound;
                        
                    }

                    if (attach) {

                        /*
                         * All variables have become bound for this constraint,
                         * so add it to the set of "used" constraints.
                         */

                        used.add(c);

                        if (log.isDebugEnabled()) {
                            log.debug("Constraint attached at index " + i
                                    + " of " + path.length + ", constraint=" + c);
                        }

                        constraints.add(c);

                    } // if(allVarsBound)

                } // next constraint

            } // joinGraphConstraints != null;

            // store the constraint[] for that predicate.
            ret[i] = constraints.toArray(new FilterNode[constraints.size()]);
            
        } // next predicate in the join path.

        /*
         * Return the set of constraints associated with each predicate in the
         * join path.
         */
        return ret;
        
    }    
    
    /**
     * Return the set of variables which MUST be bound for solutions after the
     * evaluation of this group. A group will produce "MUST" bindings for
     * variables from its statement patterns and a LET based on an expression
     * whose variables are known bound.
     * <p>
     * The returned collection reflects "bottom-up" evaluation semantics. This
     * method does NOT consider variables which are already bound on entry to
     * the group.
     * <p>
     * Note: When invoked for an OPTIONAL or MINUS join group, the variables
     * which would become bound during the evaluation of the join group are
     * reported. Caller's who wish to NOT have variables reported for OPTIONAL
     * or MINUS groups MUST NOT invoke this method for those groups.
     * <p>
     * Note: The recursive analysis does not throw out variables when part of
     * the tree will provably fail to bind anything. It is the role of query
     * optimizers to identify those situations and prune the AST appropriately.
     * <p>
     * The class hierarchy is a little untidy at this point.
     * This method is defined in the only subclass of this abstract class.
     * Initially it was thought to not be needed here.
     * 
     * @param node
     *            The node to be analyzed.
     * @param vars
     *            Where to store the "MUST" bound variables.
     * @param recursive
     *            When <code>true</code>, the child groups will be recursively
     *            analyzed. When <code>false</code>, only <i>this</i> group will
     *            be analyzed.
     * 
     * @return The argument.
     */
    public abstract Set<IVariable<?>> getDefinitelyProducedBindings(
            final IBindingProducerNode node, final Set<IVariable<?>> vars,
            final boolean recursive);

}
