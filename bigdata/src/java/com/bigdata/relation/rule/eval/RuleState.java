package com.bigdata.relation.rule.eval;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;

/**
 * State for a rule execution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleState {

    protected static final Logger log = Logger.getLogger(RuleState.class);

    /**
     * The {@link Rule} being evaluated.
     */
    final protected IRule rule;

    /**
     * Helper class for managing predicates, bindings, and solutions.
     */
    final private IJoinNexus joinNexus;

    /**
     * The {@link Rule} that is being executed.
     */
    final public IRule getRule() {

        return rule;

    }

    /**
     * A helper class that provides interoperability for the {@link IPredicate}s,
     * {@link IBindingSet}s, and {@link ISolution}s for the evaluation of the
     * <i>rule</i>.
     */
    final public IJoinNexus getJoinNexus() {

        return joinNexus;

    }

    /**
     * The {@link IAccessPath} corresponding to each {@link IPredicate} in the
     * tail of the {@link Rule}.
     * <p>
     * Note: The corresponding access path in this array is invalidated by
     * whenever a variable is (re-)bound since that will in general change the
     * selectivity of the access path (it may have a different range count).
     * <p>
     * Note: {@link #resetBindings()} clears all elements of this array.
     */
    final private IAccessPath[] accessPath;

    /**
     * The evaluation order for the predicates in the tail of the {@link Rule}.
     */
    final protected int[] order;

    /**
     * Map from the variable to the index of the predicate in tail of the
     * {@link Rule} that first binds that variable in terms of the evaluation
     * {@link #order}. This information is used by
     * {@link #clearDownstreamBindings(int)} to decide which variables should be
     * preserved. If a variable is listed here with a lower index than the
     * starting index to be cleared then its binding is NOT cleared.
     */
    final protected Map<Var, Integer> depends;

    /**
     * 
     * @param rule
     *            The rule.
     * @param joinNexus
     *            A helper class that provides interoperability for the
     *            {@link IPredicate}s, {@link IBindingSet}s, and
     *            {@link ISolution}s for the evaluation of the <i>rule</i>.
     */
    public RuleState(IRule rule, IJoinNexus joinNexus) {

        if (rule == null)
            throw new IllegalArgumentException();

        if (joinNexus == null)
            throw new IllegalArgumentException();

        this.rule = rule;

        this.joinNexus = joinNexus;

        final int tailCount = rule.getTailCount();

        // Allocate access path cache.
        this.accessPath = new IAccessPath[tailCount];

        // The evaluation order.
        this.order = new DefaultEvaluationPlan(joinNexus, rule).getOrder();

        /*
         * The dependency graph for the variables in the tail based on the
         * selected evaluation order.
         */
        this.depends = Collections
                .unmodifiableMap(computeVariableDependencyMap(order));

    }

    /**
     * Externalizes the rule and the evaluation order.
     */
    public String toString() {

        return toString(null);

    }

    /**
     * Externalizes the rule displaying variable names, their bindings,
     * constants, and the evaluation order.
     * 
     * @param bindingSet
     *            When non-<code>null</code>, the current variable bindings
     *            will be displayed. Otherwise, the names of variables will be
     *            displayed rather than their bindings.
     * 
     * @todo There should be a way to resolve the bindings against a dictionary.
     *       perhaps for {@link Rule} but definately for {@link RuleState}.
     *       This makes it MUCH easier to figure out what is going on when the
     *       data are keys in a dictionary, which is the case for the RDF DB. Of
     *       course, doing that efficiently is a JOIN :-) but it can be done
     *       with a cache in front of a point lookup as well.
     */
    public String toString(IBindingSet bindingSet) {

        final StringBuilder sb = new StringBuilder();

        sb.append(rule.getName());

        sb.append(" : ");

        // write out bindings for the tail.

        int i = 0;

        for (Iterator<IPredicate> itr = rule.getTail(); itr.hasNext(); i++) {

            final IPredicate pred = itr.next();

            sb.append(pred.toString(bindingSet));

            // if (bindingSet == null) {

            // displays the evaluation order as an index on the predicate.

            sb.append("[" + order[i] + "]");

            // }

            if (itr.hasNext()) {

                sb.append(", ");

            }

        }

        sb.append(" -> ");

        // write out bindings for the head.
        if (rule.getHead() != null) {

            sb.append(rule.getHead().toString(bindingSet));

        }

        return sb.toString();

    }

    /**
     * Given an evaluation <i>order</i>, construct the dependency graph for the
     * variables in the tail of the rule. The dependency graph is a map from
     * each variable to the index of the first predicate in evaluation order
     * where that variable becomes bound. The values of this map are used as
     * indices into <i>order</i>.
     * 
     * @param order
     *            The evaluation order.
     * 
     * @return The dependency graph.
     */
    public Map<Var, Integer> computeVariableDependencyMap(final int[] order) {

        if (order == null)
            throw new IllegalArgumentException();

        if (order.length != rule.getTailCount())
            throw new IllegalArgumentException();

        final Map<Var, Integer> depends = new HashMap<Var, Integer>();

        final int tailCount = rule.getTailCount();

        for (int i = 0; i < tailCount; i++) {

            final IPredicate pred = rule.getTail(order[i]);

            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant t = pred.get(j);

                if (t.isVar()) {

                    final Var var = (Var) t;

                    if (!depends.containsKey(var)) {

                        depends.put(var, i);

                    }

                }

            }

        }

        if (log.isDebugEnabled()) {

            log.debug("dependenyGraph=" + depends);

        }

        return depends;

    }

    /**
     * Initialize the bindings from the constants and variables in the rule.
     * <p>
     * Note: This MUST be invoked before the rule is executed. If you do not
     * clear the bindings then the old bindings will make it appear as if your
     * variables are all bound to constants and the rule will NOT select the
     * correct data.
     */
    protected void resetBindings(IBindingSet bindings) {

        bindings.clearAll();

        for (int i = 0; i < accessPath.length; i++) {

            accessPath[i] = null;

        }

    }

    // /**
    // * <p>
    // * Return the current binding for the variable or constant.
    // * </p>
    // *
    // * @param var
    // * The variable or constant.
    // *
    // * @return Its binding. The binding will be <code>null</code> if a
    // * variable is not currently bound.
    // *
    // * @throws NullPointerException
    // * if <i>var</i> is <code>null</code>.
    // * @throws IllegalArgumentException
    // * if the variable is not used in the rule.
    // * @throws IllegalArgumentException
    // * if var is a constant.
    // *
    // * @see #set(Var, Object)
    // * @see #bind(int, SPO)
    // */
    // public Object get(IBindingSet bindings,IVariableOrConstant var) {
    //    
    // return get(bindings,var, true);
    //        
    // }
    //    
    // /**
    // * <p>
    // * Return the current binding for the variable or constant.
    // * </p>
    // *
    // * @param var
    // * The variable or constant.
    // * @param required
    // * When <code>true</code> an exception is reported if the
    // * variable is not used in the rule. Note that specializing a
    // * rule often results in variables being replaced by constants
    // * such that an {@link IConstraint} might no longer be evaluable
    // * for the rule in terms of that variable.
    // *
    // * @return Its binding. The binding will be <code>null</code> if a
    // * variable is not currently bound or if the variable is not used in
    // * the rule and <code>required := false</code>.
    // *
    // * @throws NullPointerException
    // * if <i>var</i> is <code>null</code>.
    // * @throws IllegalArgumentException
    // * if <code>required := true</code> and the variable is not
    // * used in the rule.
    // * @throws IllegalArgumentException
    // * if var is a constant.
    // *
    // * @see #set(Var, Object)
    // * @see #bind(int, SPO)
    // */
    // public Object get(IBindingSet bindings,IVariableOrConstant var, boolean
    // required) {
    //        
    // // if (var == null)
    // // throw new NullPointerException();
    //
    // if (var.isConstant()) {
    //
    // return var.get();
    //            
    // }
    //
    // if (required && !rule.isDeclared((IVariable)var)) {
    //
    // throw new IllegalArgumentException("Not declared: " + var + " by "
    // + rule);
    //            
    // }
    //        
    // return bindings.get((IVariable) var);
    //                
    // }

    /**
     * Binds the variable.
     * <p>
     * Note: The cached {@link IAccessPath} for a predicate is cleared if a
     * variable binding is set on that predicate.
     * 
     * @param var
     *            A variable that appears in that predicate.
     * @param val
     *            The value to be bound on the variable -or- <code>null</code>
     *            to clear the binding for the variable.
     * 
     * @throws NullPointerException
     *             if the variable is null.
     * @throws IndexOutOfBoundsException
     *             if the predicate index is out of range.
     * 
     * @see #bind(int, Object)
     */
    // * @throws IllegalArgumentException
    // * if the variable does not appear in the rule.
    public void set(IVariable var, IConstant val, IBindingSet bindings) {

        // // verify variable declared by the rule.
        // if(!rule.isDeclared(var)) throw new IllegalArgumentException();

        // bind the variable.
        if (val == null) {

            bindings.clear(var);

        } else {

            bindings.set(var, val);

        }

        final int tailCount = rule.getTailCount();

        // clear cached access path for preds using that variable.
        for (int i = 0; i < tailCount; i++) {

            final IPredicate pred = rule.getTail(i);

            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                if (pred.get(j) == var) {

                    accessPath[i] = null;

                }

            }

        }

    }

    /**
     * Binds variables from a visited element.
     * <p>
     * Note: The bindings are propagated before the constraints are verified so
     * this method will have a side-effect on the bindings even if the
     * constraints were not satisified.
     * 
     * @param index
     *            The index of the {@link IPredicate} in the body of the
     *            {@link Rule}.
     * @param e
     *            An element materialized by the {@link IAccessPath} for that
     *            {@link IPredicate}.
     * 
     * @return <code>true</code> unless the new bindings would violate any of
     *         the {@link IConstraint}s declared for the {@link Rule}).
     * 
     * @throws NullPointerException
     *             if <i>e</i> is <code>null</code>.
     * @throws IndexOutOfBoundsException
     *             if the <i>index</i> is out of bounds.
     * 
     * @see #clearDownstreamBindings(IBindingSet bindingSet, int index)
     */
    @SuppressWarnings("unchecked")
    public boolean bind(int index, Object e, IBindingSet bindings) {

        // propagate bindings from the visited object into the binding set.
        joinNexus.copyValues(e, rule.getTail(index), bindings);

        // verify constraints.
        return rule.isConsistent(bindings);

    }

    /**
     * Clear downstream bindings in the evaluation {@link #order}[]. If a
     * variable in a downstream predicate was 1st bound by an upstream predicate
     * as identified by {@link #depends} then its value is NOT cleared.
     * <p>
     * Note: You MUST {@link #resetBindings()} before you evaluate a rule.
     * <p>
     * Note: You MUST {@link #clearDownstreamBindings(IBindingSet,int)} bindings
     * before you (re-)evaluate a subquery. Failure to do so will leave stale
     * bindings in place which will cause {@link #getAccessPath(int)} to
     * identify the wrong access path, and hence select the wrong data.
     * 
     * @param index
     *            The index of the predicate whose values you intend to
     *            {@link #bind(IBindingSet,int, Object)}.
     */
    protected void clearDownstreamBindings(int index, IBindingSet bindingSet) {

        if (log.isDebugEnabled()) {

            log.debug("index=" + index + ", bindingSet=" + bindingSet);

        }

        final int tailCount = rule.getTailCount();

        for (int i = index; i < tailCount; i++) {

            final IPredicate pred = rule.getTail(order[index]);

            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant t = pred.get(j);

                if (t.isVar()) {

                    final int k = depends.get((Var) t);

                    if (k >= index) {

                        set((Var) t, null, bindingSet);

                    }

                }

            }

        }

    }

    /**
     * Return the {@link IAccessPath} that would be used to read from the
     * selected tail {@link IPredicate}.
     * <p>
     * Note: a cache is maintained by the rule for the access paths. If the
     * cache does not have an entry for the desired access path then one is
     * obtained and placed into the cache before being returned to the caller.
     * The cache is invalidated by {@link #resetBindings()} and (on a selective
     * basis) by {@link #bind(int, Object)} and {@link #set(IVariable, Object)}.
     * 
     * @param index
     *            The index into tail of the {@link Rule}.
     * 
     * @return The {@link IAccessPath}.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     * 
     * @throws IllegalArgumentException
     *             if <i>bindingSet</i> is <code>null</code>.
     * 
     * FIXME The side-effect on {@link #accessPath}[] means that this class is
     * NOT thread-safe. A thread-local weak reference to an access path cache
     * might fix that. However, I am not yet sure if this is a problem.
     * 
     * @todo A more generalized access path cache might be located on the
     *       {@link IRelation} impl. it could either scan an LRU of predicates
     *       for recently returned access paths or it could use the hash of the
     *       predicate to test a weak reference map backed by an LRU. However,
     *       measure the cost of obtaining an access path. It is done without IO
     *       so it can't be too much! (Note that if the cache is specific to the
     *       bindings on the predicate then there are no side-effects from the
     *       propagation of bindings and we can simplify how we set, clear, and
     *       reset the bindings!)
     */
    public IAccessPath getAccessPath(final int index, IBindingSet bindingSet) {

        if (bindingSet == null)
            throw new IllegalArgumentException();

        // check the cache.
        IAccessPath accessPath = this.accessPath[index];

        if (accessPath == null) {

            accessPath = getAccessPathNoCache(index, bindingSet);

            // update the cache.
            this.accessPath[index] = accessPath;

        }

        return accessPath;

    }

    /**
     * Return the {@link IAccessPath} that would be used to read from the
     * selected {@link IPredicate} (no caching).
     * 
     * @param index
     *            The index of the {@link IPredicate} in the tail of the
     *            {@link Rule}.
     * @param bindingSet
     *            The bindings used to generate the {@link IAccessPath}.
     * 
     * @return The {@link IAccessPath}.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     * @throws IllegalArgumentException
     *             if <i>bindingSet</i> is <code>null</code>.
     * @throws RuntimeException
     *             if the name of the relation can not be resolved by the
     *             {@link IJoinNexus} to an {@link IRelation} instance.
     */
    protected IAccessPath getAccessPathNoCache(final int index,
            final IBindingSet bindingSet) {

        // based on the given bindings.
        final IPredicate predicate = rule.getTail(index).asBound(bindingSet);

        return joinNexus.getTailAccessPath(predicate);

    }

}
