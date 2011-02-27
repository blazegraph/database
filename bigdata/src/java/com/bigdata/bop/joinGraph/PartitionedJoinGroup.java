package com.bigdata.bop.joinGraph;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpIdFactory;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.joinGraph.rto.JoinGraph;
import com.bigdata.bop.solutions.SliceOp;

/**
 * Class accepts a join group and partitions it into a join graph and a tail
 * plan.
 * <p>
 * A join group consists of an ordered collection of {@link IPredicate}s and an
 * unordered collection of {@link IConstraint}s. {@link IPredicate} representing
 * non-optional joins are extracted into a {@link JoinGraph} along with any
 * {@link IConstraint}s whose variables are guaranteed to be bound by the
 * implied joins.
 * <p>
 * The remainder of the {@link IPredicate}s and {@link IConstraint}s form a
 * "tail plan". {@link IConstraint}s in the tail plan are attached to the last
 * {@link IPredicate} at which their variable(s) MIGHT have become bound.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @todo Things like LET can also bind variables. So can a subquery. Analysis of
 *       those will tell us whether the variable will definitely or
 *       conditionally become bound (I am assuming that a LET can conditionally
 *       leave a variable unbound). See {@link Bind}.
 * 
 * @todo runFirst flag on the expander (for free text search). this should be an
 *       annotation. this can be a [headPlan]. [There can be constraints which
 *       are evaluated against the head plan. They need to get attached to the
 *       joins generated for the head plan. MikeP writes: There is a free text
 *       search access path that replaces the actual access path for the
 *       predicate, which is meaningless in an of itself because the P is
 *       magical.]
 */
public class PartitionedJoinGroup {

    private static final transient Logger log = Logger
            .getLogger(PartitionedJoinGroup.class);

    /**
     * The set of variables bound by the non-optional predicates.
     */
    private final Set<IVariable<?>> joinGraphVars = new LinkedHashSet<IVariable<?>>();

    /**
     * The set of non-optional predicates which have been flagged as
     * "run first". These are usually special access paths created using an
     * expander which replaces a mock access path. For example, free text
     * search.
     */
    private final List<IPredicate<?>> headPlan = new LinkedList<IPredicate<?>>();

    /**
     * The set of non-optional predicates which represent the join graph.
     */
    private final List<IPredicate<?>> joinGraph = new LinkedList<IPredicate<?>>();

    /**
     * The set of constraints which can be evaluated with the head plan and/or
     * join graph predicates because the variables appearing in those
     * constraints are known to become bound within the join graph. ( The
     * {@link #headPlan} and the {@link #joinGraph} share the same
     * pool of constraints.)
     */
    private final List<IConstraint> joinGraphConstraints = new LinkedList<IConstraint>();

    /**
     * A set of optional predicates which will be run after the join graph.
     */
    private final List<IPredicate<?>> tailPlan = new LinkedList<IPredicate<?>>();

    /**
     * An unordered list of those constraints containing at least one variable
     * known be bound (and optionally) bound within the tail plan.
     */
    private final List<IConstraint> tailPlanConstraints = new LinkedList<IConstraint>();

    /**
     * A map indicating which constraints are run for which predicate in the
     * tail plan. The keys are the bopIds of the predicates in the tail plan.
     * The values are the sets of constraints to run for that tail.
     * 
     * @todo This assumes that the tail plan is not reordered.
     */
    private final Map<Integer/* predId */, List<IConstraint>> tailPlanConstraintMap = new LinkedHashMap<Integer, List<IConstraint>>();
    
    /**
     * The set of variables bound by the non-optional predicates (either the
     * head plan or the join graph).
     */
    public Set<IVariable<?>> getJoinGraphVars() {
        return joinGraphVars;
    }

    /**
     * The {@link IPredicate}s in the join graph (required joins).
     */
    public IPredicate<?>[] getJoinGraph() {
        return joinGraph.toArray(new IPredicate[joinGraph.size()]);
    }

    /**
     * The {@link IConstraint}s to be applied to the {@link IPredicate}s in the
     * join graph. Each {@link IConstraint} should be applied as soon as all of
     * its variable(s) are known to be bound. The constraints are not attached
     * to the {@link IPredicate}s in the join graph because the evaluation order
     * of those {@link IPredicate}s is not yet known (it will be determined by a
     * query optimizer when it decides on an evaluation order for those joins).
     */
    public IConstraint[] getJoinGraphConstraints() {
        return joinGraphConstraints
                .toArray(new IConstraint[joinGraphConstraints.size()]);
    }

	/**
	 * Return the set of constraints which should be attached to the last join
	 * in the given the join path. All joins in the join path must be
	 * non-optional joins (that is, part of either the head plan or the join
	 * graph).
	 * <p>
	 * The rule followed by this method is that each constraint will be attached
	 * to the first non-optional join at which all of its variables are known to
	 * be bound. It is assumed that constraints are attached to each join in the
	 * join path by a consistent logic, e.g., as dictated by this method.
	 * 
	 * @param joinPath
	 *            An ordered array of predicate identifiers representing a
	 *            specific sequence of non-optional joins.
	 * @param pathIsComplete
	 *            <code>true</code> iff the <i>path</i> represents a complete
	 *            join path. When <code>true</code>, any constraints which have
	 *            not already been attached will be attached to the last
	 *            predicate in the join path.
	 * 
	 * @return The constraints which should be attached to the last join in the
	 *         join path.
	 * 
	 * @throws IllegalArgumentException
	 *             if the join path is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if the join path is empty.
	 * @throws IllegalArgumentException
	 *             if any element of the join path is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if any predicate specified in the join path is not known to
	 *             this class.
	 * @throws IllegalArgumentException
	 *             if any predicate specified in the join path is optional.
	 * 
	 * @todo Implement (or refactor) the logic to decide which variables need to
	 *       be propagated and which can be dropped. This decision logic will
	 *       need to be available to the runtime query optimizer.
	 * 
	 * @todo This does not pay attention to the head plan. If there can be
	 *       constraints on the head plan then either this should be modified
	 *       such that it can decide where they attach or we need to have a
	 *       method which does the same thing for the head plan.
	 */
    public IConstraint[] getJoinGraphConstraints(final int[] pathIds,
    		final boolean pathIsComplete) {

        /*
         * Verify arguments and resolve bopIds to predicates.
         */
        if (pathIds == null)
            throw new IllegalArgumentException();

        final IPredicate<?>[] path = new IPredicate[pathIds.length];

        for (int i = 0; i < pathIds.length; i++) {

            final int id = pathIds[i];
            
            IPredicate<?> p = null;

            for (IPredicate<?> tmp : joinGraph) {

                if (tmp.getId() == id) {

                    p = tmp;

                    break;

                }

            }

            if (p == null)
                throw new IllegalArgumentException("Not found: id=" + id);

            if (p.isOptional())
                throw new AssertionError(
                        "Not expecting an optional predicate: " + p);

            path[i] = p;

        }
     
		final IConstraint[] constraints = joinGraphConstraints
				.toArray(new IConstraint[joinGraphConstraints.size()]);

		final IConstraint[][] attachedConstraints = getJoinGraphConstraints(
				path, constraints, null/* knownBound */, pathIsComplete);
        
        return attachedConstraints[pathIds.length - 1];
        
    }

//    static public IConstraint[][] getJoinGraphConstraints(
//            final IPredicate<?>[] path, final IConstraint[] joinGraphConstraints) {
//
//    	return getJoinGraphConstraints(path, joinGraphConstraints, null/*knownBound*/);
//    	
//    }

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
    static public IConstraint[][] getJoinGraphConstraints(
            final IPredicate<?>[] path,//
            final IConstraint[] joinGraphConstraints,//
            final IVariable<?>[] knownBoundVars,//
            final boolean pathIsComplete//
            ) {

        if (path == null)
            throw new IllegalArgumentException();
        
        if (path.length == 0)
            throw new IllegalArgumentException();

        // the set of constraints for each predicate in the join path.
        final IConstraint[][] ret = new IConstraint[path.length][];

        // the set of variables which are bound.
        final Set<IVariable<?>> boundVars = new LinkedHashSet<IVariable<?>>();
        
        // add the already known bound vars
        if (knownBoundVars != null) {
        	for (IVariable<?> v : knownBoundVars)
        		boundVars.add(v);
        }

        /*
         * For each predicate in the path in the given order, figure out which
         * constraint(s) would attach to that predicate based on which variables
         * first become bound with that predicate. For the last predicate in the
         * given join path, we return that set of constraints.
         */

        // the set of constraints which have been consumed.
        final Set<IConstraint> used = new LinkedHashSet<IConstraint>();
        
        for (int i = 0; i < path.length; i++) {

//            // true iff this is the last join in the path.
//            final boolean lastJoin = i == path.length - 1;
            
            // a predicate in the path.
            final IPredicate<?> p = path[i];

            if (p == null)
                throw new IllegalArgumentException();
            
            // the constraints for the current predicate in the join path.
            final List<IConstraint> constraints = new LinkedList<IConstraint>();
            
            {
                /*
                 * Visit the variables used by the predicate (and bound by it
                 * since it is not an optional predicate) and add them into the
                 * total set of variables which are bound at this point in the
                 * join path.
                 */
                final Iterator<IVariable<?>> vitr = BOpUtility
                        .getArgumentVariables(p);

                while (vitr.hasNext()) {

                    final IVariable<?> var = vitr.next();

                    boundVars.add(var);

                }
            }

            if (joinGraphConstraints != null) {

                // consider each constraint.
                for (IConstraint c : joinGraphConstraints) {

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
	                     * true iff all variables used by this constraint are bound
	                     * at this point in the join path.
	                     */
	                    boolean allVarsBound = true;
	
	                    // visit the variables used by this constraint.
	                    final Iterator<IVariable<?>> vitr = BOpUtility
	                            .getSpannedVariables(c);
	
	                    while (vitr.hasNext()) {
	
	                        final IVariable<?> var = vitr.next();
	
	                        if (!boundVars.contains(var)) {
	
	                            allVarsBound = false;
	
	                            break;
	
	                        }
	
	                    }
	                 
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
                                    + " of " + path.length + ", bopId="
                                    + p.getId() + ", constraint=" + c);
                        }

                        constraints.add(c);

                    } // if(allVarsBound)

                } // next constraint

            } // joinGraphConstraints != null;

            // store the constraint[] for that predicate.
            ret[i] = constraints.toArray(new IConstraint[constraints.size()]);
            
        } // next predicate in the join path.

        /*
         * Return the set of constraints associated with each predicate in the
         * join path.
         */
        return ret;
        
    }    

    /**
     * The {@link IPredicate}s representing optional joins. Any
     * {@link IConstraint}s having variable(s) NOT bound by the required joins
     * will already have been attached to the last {@link IPredicate} in the
     * tail plan in which their variable(S) MIGHT have been bound.
     */
    public IPredicate<?>[] getTailPlan() {
        return tailPlan.toArray(new IPredicate[tailPlan.size()]);
    }

    /**
     * Return the set of {@link IConstraint}s which should be evaluated when an
     * identified predicate having SPARQL optional semantics is evaluated. For
     * constraints whose variables are not known to be bound when entering the
     * tail plan, the constraint should be evaluated at the last predicate for
     * which its variables MIGHT become bound.
     * 
     * @param bopId
     *            The identifier for an {@link IPredicate} appearing in the tail
     *            plan.
     * 
     * @return The set of constraints to be imposed by the join which evaluates
     *         that predicate. This will be an empty array if there are no
     *         constraints which can be imposed when that predicate is
     *         evaluated.
     * 
     * @throws IllegalArgumentException
     *             if there is no such predicate in the tail plan.
     */
    public IConstraint[] getTailPlanConstraints(final int bopId) {

        boolean found = false;

        for (IPredicate<?> p : tailPlan) {

            if (p.getId() == bopId) {

                found = true;

                break;

            }

        }
        
        if (!found)
            throw new IllegalArgumentException(
                    "No such predicate in tail plan: bopId=" + bopId);
        
        final List<IConstraint> constraints = tailPlanConstraintMap.get(bopId);

        if (constraints == null) {

            return new IConstraint[0];
            
        }

        return constraints.toArray(new IConstraint[constraints.size()]);
        
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
     * <p>
     * Note: This method is more efficient than
     * {@link BOpUtility#getSharedVars(BOp, BOp)} because it does not
     * materialize the sets of shared variables. However, it only considers the
     * operands of the {@link IPredicate}s and is thus more restricted than
     * {@link BOpUtility#getSharedVars(BOp, BOp)} as well.
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
    static public boolean canJoin(final IPredicate<?> p1, final IPredicate<?> p2) {

        if (p1 == null)
            throw new IllegalArgumentException();

        if (p2 == null)
            throw new IllegalArgumentException();

        // iterator scanning the operands of p1.
        final Iterator<IVariable<?>> itr1 = BOpUtility.getArgumentVariables(p1);

        while (itr1.hasNext()) {

            final IVariable<?> v1 = itr1.next();

            // iterator scanning the operands of p2.
            final Iterator<IVariable<?>> itr2 = BOpUtility
                    .getArgumentVariables(p2);

            while (itr2.hasNext()) {

                final IVariable<?> v2 = itr2.next();

                if (v1 == v2) {

                    if (log.isDebugEnabled())
                        log.debug("Can join: sharedVar=" + v1 + ", p1=" + p1
                                + ", p2=" + p2);

                    return true;

                }

            }

        }

        if (log.isDebugEnabled())
            log.debug("No directly shared variable: p1=" + p1 + ", p2=" + p2);

        return false;

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
     * Note: Use {@link PartitionedJoinGroup#canJoin(IPredicate, IPredicate)}
     * instead to identify joins based on a variable which is directly shared.
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
    static public boolean canJoinUsingConstraints(final IPredicate<?>[] path,
            final IPredicate<?> vertex, final IConstraint[] constraints) {

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
            for (IPredicate<?> p : path) {
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

        for (IPredicate<?> p : path) {

            final Iterator<IVariable<?>> vitr = BOpUtility
                    .getArgumentVariables(p);

            while (vitr.hasNext()) {

                knownBound.add(vitr.next());

            }

        }

        /*
         * 
         * If the given predicate directly shares a variable with any of the
         * predicates in the join path, then we can return immediately.
         */
        {

            final Iterator<IVariable<?>> vitr = BOpUtility
                    .getArgumentVariables(vertex);

            while (vitr.hasNext()) {

                final IVariable<?> var = vitr.next();

                if (knownBound.contains(var)) {

                    if (log.isDebugEnabled())
                        log.debug("Can join: sharedVar=" + var + ", path="
                                + Arrays.toString(path) + ", vertex=" + vertex);

                    return true;

                }

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
            final IPredicate<?>[] newPath = new IPredicate[path.length + 1];

            System.arraycopy(path/* src */, 0/* srcPos */, newPath/* dest */,
                    0/* destPos */, path.length);

            newPath[path.length] = vertex;

            /*
             * Find the constraints that will run with each vertex of the new
             * join path.
             */
            final IConstraint[][] constraintRunArray = getJoinGraphConstraints(
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
            final IConstraint[] vertexConstraints = constraintRunArray[path.length];

            for (IConstraint c : vertexConstraints) {

                // consider all variables spanned by the constraint.
                final Iterator<IVariable<?>> vitr = BOpUtility
                        .getSpannedVariables(c);

                while (vitr.hasNext()) {

                    final IVariable<?> var = vitr.next();

                    if (knownBound.contains(var)) {

                        if (log.isDebugEnabled())
                            log.debug("Can join: sharedVar=" + var + ", path="
                                    + Arrays.toString(path) + ", vertex="
                                    + vertex + ", constraint=" + c);

                        return true;

                    }

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
     * Analyze a set of {@link IPredicate}s representing "runFirst", optional
     * joins, and non-optional joins which may be freely reordered together with
     * a collection of {@link IConstraint}s and partition them into a join graph
     * and a tail plan. The resulting data structure can efficiently answer a
     * variety of queries regarding joins, join paths, and constraints and can
     * be used to formulate a complete query when combined with a desired join
     * ordering.
     * 
     * @param knownBound
     *            A set of variables which are known to be bound on entry.
     * @param sourcePreds
     *            The predicates.
     * @param constraints
     *            The constraints.
     * 
     * @return A data structure representing both the join graph and the tail
     *         plan.
     * 
     * @throws IllegalArgumentException
     *             if the source predicates array is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the source predicates array is empty.
     * @throws IllegalArgumentException
     *             if any element of the source predicates array is
     *             <code>null</code>.
     */
    public PartitionedJoinGroup(//
//            final Set<IVariable<?>> knownBound,
            final IPredicate<?>[] sourcePreds,//
            IConstraint[] constraints) {

        if (sourcePreds == null)
            throw new IllegalArgumentException();
        if (sourcePreds.length == 0)
            throw new IllegalArgumentException();

        if (constraints == null) {
            // replace with an empty array.
            constraints = new IConstraint[0];
        }
        
        /*
         * First identify the predicates which correspond to non-optional joins.
         * All other pipeline operators are inserted into the tail plan in the
         * order in which they are given.
         */
        for (IPredicate<?> p : sourcePreds) {
            if (p == null)
                throw new IllegalArgumentException();
            if (p.isOptional()) {
                if (p.getAccessPathExpander() != null
                        && p.getAccessPathExpander().runFirst())
                    throw new IllegalStateException(
                            "runFirst is not compatible with optional: " + p);
                // an optional predicate
                tailPlan.add(p);
            } else {
                // non-optional predicate.
                if (p.getAccessPathExpander() != null
                        && p.getAccessPathExpander().runFirst()) {
                    headPlan.add(p);
                } else {
                    // part of the join graph.
                    joinGraph.add(p);
                }
                /*
                 * Add to the set of variables which will be bound by the time
                 * the join graph is done executing.
                 */
                final Iterator<IVariable<?>> vitr = BOpUtility
                        .getArgumentVariables(p);
                while (vitr.hasNext()) {
                    joinGraphVars.add(vitr.next());
                }
            }
        }

        /*
         * Now break the constraints into different groups based on their
         * variables and when those variables are known to be bound (required
         * joins) or might be bound (optionals).
         */
        for (IConstraint c : constraints) {
            boolean allFound = true;
            final Iterator<IVariable<?>> vitr = BOpUtility
                    .getSpannedVariables(c);
            if (!vitr.hasNext()) {
                /*
                 * All constraints should have at least one variable.
                 */
                throw new RuntimeException("No variables in constraint: " + c);
            }
            while (vitr.hasNext()) {
                final IVariable<?> var = vitr.next();
                if (!joinGraphVars.contains(var)) {
                    /*
                     * This constraint will be evaluated against the tail plan.
                     */
                    allFound = false;
                    tailPlanConstraints.add(c);
                    break;
                }
            }
            if (allFound) {
                /*
                 * This constraint will be evaluated by the join graph for the
                 * first join in in which all of the variables used by the
                 * constraint are known to be bound.
                 */
                joinGraphConstraints.add(c);
            }
        }

        /*
         * If a variable is not bound by a required predicate, then we attach
         * any constraint using that variable to the last optional predicate in
         * which that variable MIGHT become bound.
         */
        {
            /*
             * Populate a map from each variable not bound in the join graph to
             * the last index in the tail plan at which it MIGHT become bound.
             */
            final Map<IVariable<?>, Integer/* lastIndexOf */> lastIndexOf = new LinkedHashMap<IVariable<?>, Integer>();
            int indexOf = 0;
            for (IPredicate<?> p : tailPlan) {
                final Iterator<IVariable<?>> vitr = BOpUtility
                        .getArgumentVariables(p);
                while (vitr.hasNext()) {
                    final IVariable<?> var = vitr.next();
                    lastIndexOf.put(var, Integer.valueOf(indexOf));
                }
                indexOf++;
            }
            /*
             * For each constraint using at least one variable NOT bound by the
             * join graph, find the maximum value of lastIndexOf for the
             * variable(s) in that constraint. That is the index of the operator
             * in the tail plan to which the constraint should be attached.
             */
            for (IConstraint c : tailPlanConstraints) {
                final Iterator<IVariable<?>> vitr = BOpUtility
                        .getSpannedVariables(c);
                Integer maxLastIndexOf = null;
                while (vitr.hasNext()) {
                    final IVariable<?> var = vitr.next();
                    if (joinGraphVars.contains(var)) {
                        // This variable is bound by the join graph.
                        continue;
                    }
                    final Integer tmp = lastIndexOf.get(var);
                    if (tmp == null) {
                        // This variable is never bound by the query.
                        throw new NoSolutionsException(
                                "Variable is never bound: " + var);
                    }
                    if (maxLastIndexOf == null
                            || tmp.intValue() > maxLastIndexOf.intValue()) {
                        maxLastIndexOf = tmp;
                    }
                } // next variable.
                if (maxLastIndexOf == null) {
                    // A logic error.
                    throw new AssertionError("maxLastIndexOf is undefined: "
                            + c);
                }
                /*
                 * Add the constraint to the last predicate at which any of its
                 * variables MIGHT have become bound.
                 */
                {
                    /*
                     * The bopId for the predicate in the tail plan for which,
                     * when that predicate is evaluated, we will run this
                     * constraint.
                     */
                    final int predId = tailPlan.get(maxLastIndexOf).getId();
                    /*
                     * The constraint(s) (if any) already associated with that
                     * predicate.
                     */
                    List<IConstraint> tmp = tailPlanConstraintMap.get(predId);
                    if (tmp == null) {
                        tmp = new LinkedList<IConstraint>();
                        tailPlanConstraintMap.put(predId, tmp);
                    }
                    tmp.add(c);
                }
            } // next tail plan constraint.

        }

    }

    /**
     * Generate a query plan from an ordered collection of predicates.
     * 
     * @param p
     *            The join path.
     * 
     * @return The query plan.
     * 
     *         FIXME Select only those variables required by downstream
     *         processing or explicitly specified by the caller (in the case
     *         when this is a subquery, the caller has to declare which
     *         variables are selected and will be returned out of the subquery).
     * 
     *         FIXME For scale-out, we need to either mark the join's evaluation
     *         context based on whether or not the access path is local or
     *         remote (and whether the index is key-range distributed or hash
     *         partitioned).
     * 
     *         FIXME Add a method to generate a runnable query plan from the
     *         collection of predicates and constraints on the
     *         {@link PartitionedJoinGroup} together with an ordering over the
     *         join graph. This is a bit different for the join graph and the
     *         optionals in the tail plan. The join graph itself should either
     *         be a {@link JoinGraph} operator which gets evaluated at run time
     *         or reordered by whichever optimizer is selected for the query
     *         (query hints).
     * 
     * @todo The order of the {@link IPredicate}s in the tail plan is currently
     *       unchanged from their given order (optional joins without
     *       constraints can not reduce the selectivity of the query). However,
     *       it could be worthwhile to run optionals with constraints before
     *       those without constraints since the constraints can reduce the
     *       selectivity of the query. If we do this, then we need to reorder
     *       the optionals based on the partial order imposed what variables
     *       they MIGHT bind (which are not bound by the join graph).
     * 
     * @todo multiple runFirst predicates can be evaluated in parallel unless
     *       they have shared variables. When there are no shared variables,
     *       construct a TEE pattern such that evaluation proceeds in parallel.
     *       When there are shared variables, the runFirst predicates must be
     *       ordered based on those shared variables (at which point, it is
     *       probably an error to flag them as runFirst).
     */
    static public PipelineOp getQuery(final BOpIdFactory idFactory,
            final IPredicate<?>[] preds, final IConstraint[] constraints) {

        // figure out which constraints are attached to which predicates.
        final IConstraint[][] assignedConstraints = PartitionedJoinGroup
                .getJoinGraphConstraints(preds, constraints, null/*knownBound*/,
                		true/*pathIsComplete*/);
        
        final PipelineJoin<?>[] joins = new PipelineJoin[preds.length];

        PipelineOp lastOp = null;

        for (int i = 0; i < preds.length; i++) {

            // The next vertex in the selected join order.
            final IPredicate<?> p = preds[i];

            final List<NV> anns = new LinkedList<NV>();

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, p));

            anns.add(new NV(PipelineJoin.Annotations.BOP_ID, idFactory
                    .nextId()));

//          anns.add(new NV(PipelineJoin.Annotations.EVALUATION_CONTEXT, BOpEvaluationContext.ANY));
//
//          anns.add(new NV(PipelineJoin.Annotations.SELECT, vars.toArray(new IVariable[vars.size()])));

            if (assignedConstraints[i] != null
                    && assignedConstraints[i].length > 0)
                anns
                        .add(new NV(PipelineJoin.Annotations.CONSTRAINTS,
                                assignedConstraints[i]));

            final PipelineJoin<?> joinOp = new PipelineJoin(
                    lastOp == null ? new BOp[0] : new BOp[] { lastOp }, anns
                            .toArray(new NV[anns.size()]));

            joins[i] = joinOp;

            lastOp = joinOp;

        }

//      final PipelineOp queryOp = lastOp;

        /*
         * FIXME Why does wrapping with this slice appear to be
         * necessary? (It is causing runtime errors when not wrapped).
         * Is this a bopId collision which is not being detected?
         * 
         * [This should perhaps be moved into the caller.]
         */
        final PipelineOp queryOp = new SliceOp(new BOp[] { lastOp }, NV
                .asMap(new NV[] {
                        new NV(JoinGraph.Annotations.BOP_ID, idFactory.nextId()), //
                        new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true),//
                        }) //
        );

//        final PipelineOp queryOp = lastOp;
        
        return queryOp;

    }

}
