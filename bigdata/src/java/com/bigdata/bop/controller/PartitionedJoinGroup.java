package com.bigdata.bop.controller;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.controller.JoinGraph.NoSolutionsException;

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
 * @todo The order of the {@link IPredicate}s in the tail plan is currently
 *       unchanged from their given order (optional joins without constraints
 *       can not reduce the selectivity of the query). However, it could be
 *       worthwhile to run optionals with constraints before those without
 *       constraints since the constraints can reduce the selectivity of the
 *       query. If we do this, then we need to reorder the optionals based on
 *       the partial order imposed what variables they MIGHT bind (which are not
 *       bound by the join graph).
 * 
 * @todo Things like LET can also bind variables. So can a subquery. Analysis of
 *       those will tell us whether the variable will definitely or
 *       conditionally become bound (I am assuming that a LET can conditionally
 *       leave a variable unbound).
 * 
 * @todo runFirst flag on the expander (for free text search). this should be an
 *       annotation. this can be a [headPlan]. [There can be constraints which
 *       are evaluated against the head plan. They need to get attached to the
 *       joins generated for the head plan. MikeP writes: There is a free text
 *       search access path that replaces the actual access path for the
 *       predicate, which is meaningless in an of itself because the P is
 *       magical.]
 * 
 * @todo write a method which returns the set of constraints which should be run
 *       for the last predicate in a given join path (a join path is just an
 *       ordered array of predicates).
 * 
 *       FIXME Add a method to generate a runnable query plan from a collection
 *       of predicates and constraints. This is a bit different for the join
 *       graph and the optionals in the tail plan. The join graph itself should
 *       either be a {@link JoinGraph} operator which gets evaluated at run time
 *       or reordered by whichever optimizer is selected for the query (query
 *       hints).
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
     *             FIXME implement and unit tests.
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
    public IConstraint[] getJoinGraphConstraints(final int[] pathIds) {

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

        /*
         * For each predicate in the path in the given order, figure out which
         * constraint(s) would attach to that predicate based on which variables
         * first become bound with that predicate. For the last predicate in the
         * given join path, we return that set of constraints.
         */

        // the set of variables which are bound.
        final Set<IVariable<?>> boundVars = new LinkedHashSet<IVariable<?>>(); 
        
        // the set of constraints which have been consumed.
        final Set<IConstraint> used = new LinkedHashSet<IConstraint>();
        
        // the set of constraints for the last predicate in the join path.
        final List<IConstraint> ret = new LinkedList<IConstraint>();
        
        for(int i = 0; i<path.length; i++) {

            // true iff this is the last join in the path.
            final boolean lastJoin = i == path.length - 1;
            
            // a predicate in the path.
            final IPredicate<?> p = path[i];

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
            
            // consider each constraint.
            for(IConstraint c : joinGraphConstraints) {

                if (used.contains(c)) {
                    /*
                     * Skip constraints which were already assigned to
                     * predicates before this one in the join path.
                     */
                    continue;
                }

                /*
                 * true iff all variables used by this constraint are bound at
                 * this point in the join path.
                 */
                boolean allVarsBound = true;

                // visit the variables used by this constraint.
                final Iterator<IVariable<?>> vitr = BOpUtility
                        .getSpannedVariables(c);

                while (vitr.hasNext()) {

                    final IVariable<?> var = vitr.next();
                    
                    if(!boundVars.contains(var)) {
                        
                        allVarsBound = false;

                        break;

                    }

                }

                if (allVarsBound) {

                    /*
                     * All variables have become bound for this constraint, so
                     * add it to the set of "used" constraints.
                     */
                    
                    used.add(c);

//                    if (log.isDebugEnabled()) {
//                        log.debug
//                    }
                    System.err.println("Constraint attached at index " + i + " of "
                            + path.length + ", bopId=" + p.getId()
                            + ", constraint=" + c);
                    
                    if (lastJoin) {

                        /*
                         * If we are on the last join in the join path, then
                         * this constraint is one of the ones that we will
                         * return.
                         */
                        
                        ret.add(c);

                    }

                } // if(allVarsBound)
                    
            } // next constraint
            
        } // next predicate in the join path.

        /*
         * Return the set of constraints to be applied as of the last predicate
         * in the join path.
         */
        return ret.toArray(new IConstraint[ret.size()]);
        
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
     * Analyze a set of {@link IPredicate}s representing optional and
     * non-optional joins and a collection of {@link IConstraint}s, partitioning
     * them into a join graph and a tail plan.
     * 
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
    
}
