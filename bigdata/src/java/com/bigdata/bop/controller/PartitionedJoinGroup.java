package com.bigdata.bop.controller;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * @todo However, how do we manage when there are things like conditional
 *       routing operators? [Answer - the CONDITION is raised onto the subquery
 *       such that we only conditionally run the subquery rather than routing
 *       out of the subquery if the condition is not satisfied - MikeP is making
 *       this change.]
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

    /**
     * The set of variables bound by the non-optional predicates.
     */
    private final Set<IVariable<?>> joinGraphVars = new LinkedHashSet<IVariable<?>>();
    
    /**
     * An unordered list of constraints which do not involve ANY variables.
     * These constraints should be run first, before the join graph.
     * 
     * @todo integrate into evaluation.
     */
    private final List<IConstraint> runFirstConstraints = new LinkedList<IConstraint>();

    /**
     * The set of the {@link IPredicate}s which have been flagged as
     * "run first". These must all be non-optional predicates. They are usually
     * special access paths created using an expander which replaces a mock
     * access path. For example, free text search.
     */
    private final List<IPredicate<?>> headPlan = new LinkedList<IPredicate<?>>();

    /**
     * The set of constraints which can be evaluated with the head plan
     * predicates because the variables appearing in those constraints are known
     * to become bound within the head plan.
     */
    private final List<IConstraint> headPlanConstraints = new LinkedList<IConstraint>();

    /**
     * The set of non-optional predicates which represent the join graph.
     */
    private final List<IPredicate<?>> joinGraphPredicates = new LinkedList<IPredicate<?>>();

    /**
     * The set of constraints which can be evaluated with the join graph
     * predicates because the variables appearing in those constraints are known
     * to become bound within the join graph.
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
     * The set of variables bound by the non-optional predicates.
     */
    public Set<IVariable<?>> getJoinGraphVars() {
        return joinGraphVars;
    }

    /**
     * The {@link IPredicate}s in the join graph (required joins).
     */
    public IPredicate<?>[] getJoinGraphPredicates() {
        return joinGraphPredicates.toArray(new IPredicate[joinGraphPredicates
                .size()]);
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
                    joinGraphPredicates.add(p);
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
                 * This is a constraint which does not involve any variable so
                 * we should evaluate it as soon as possible. I.e., before the
                 * join graph.
                 */
                runFirstConstraints.add(c); // @todo unit test.
                continue;
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
