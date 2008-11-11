package com.bigdata.relation.rule.eval;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.striterator.IKeyOrder;

/**
 * State for a rule execution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleState implements IRuleState {

    protected static final transient Logger log = Logger.getLogger(RuleState.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final transient boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final transient boolean DEBUG = log.isDebugEnabled();

    /**
     * The {@link Rule} being evaluated.
     */
    final private IRule rule;

    /**
     * The plan (data from which the evaluation order was selected).
     */
    final private IEvaluationPlan plan;
    
    /**
     * The #of unbound variables for the predicates in the tail of the
     * {@link Rule} given the {@link #plan evaluation plan}. The array is
     * correlated with the predicates index in the tail of the rule NOT with its
     * evaluation order.
     */
    final private int[] nvars;
    
    /**
     * An array of the {@link IKeyOrder} that will be used for each predicate in
     * the tail of the rule. The array is correlated with the predicates index
     * in the tail of the rule NOT with its evaluation order. This information
     * may be used to re-order a chunk into the {@link IKeyOrder} for the next
     * join dimension. This increases performance by making sure that ordered
     * reads are used by the subqueries for any given chunk on some join
     * dimension.
     */
    final private IKeyOrder[] keyOrder;
    
    public IRule getRule() {

        return rule;
        
    }
    
    public IEvaluationPlan getPlan() {
        
        return plan;
        
    }
    
    public int[] getNVars() {
        
        return nvars;
        
    }
    
    public IKeyOrder[] getKeyOrder() {
        
        return keyOrder;
        
    }
    
    /**
     * 
     * @param rule
     *            The rule.
     * @param joinNexus
     *            The {@link IJoinNexus}.
     */
    public RuleState(final IRule rule, final IJoinNexus joinNexus) {

        if (rule == null)
            throw new IllegalArgumentException();

        if (joinNexus == null)
            throw new IllegalArgumentException();

        this.rule = rule;

        // The plan (data from which the evaluation order was selected).
        this.plan = joinNexus.getPlanFactory().newPlan(joinNexus, rule);
        
//        // The evaluation order.
//        this.order = plan.getOrder();
        
        this.nvars = new int[rule.getTailCount()];
        
        // The key order that will be used for each join dimension.
        this.keyOrder = computeKeyOrderForEachTail(rule, joinNexus, plan.getOrder(), nvars);
       
        if (INFO)
            log.info("\nrule=" + rule + "\nplan=" + plan + "\nkeyOrder="
                + Arrays.toString(keyOrder));
        
    }

    /**
     * Externalizes the rule and the evaluation order.
     */
    public String toString() {

        return toString(null);

    }

    /**
     * Shows the bindings (if given), the computed evaluation order, and the
     * computed {@link IKeyOrder} for each {@link IPredicate} in the rule.
     * 
     * @param bindingSet
     *            When non-<code>null</code>, the current variable bindings
     *            will be displayed. Otherwise, the names of variables will be
     *            displayed rather than their bindings.
     */
    public String toString(IBindingSet bindingSet) {

        return rule.toString(bindingSet)+ ", order="
                + Arrays.toString(plan.getOrder()) + ", keyOrder="
                + Arrays.toString(keyOrder) + ", nvars="
                + Arrays.toString(nvars);

    }

    /**
     * Return an array indicating the {@link IKeyOrder} that will be used when
     * reading on each of the tail predicates. The array is formed using a
     * private {@link IBindingSet} and propagating
     * {@link IJoinNexus#fakeBinding(IPredicate, Var) fake bindings} to each
     * predicate in turn using the given evaluation order.
     * 
     * @param order
     *            The evaluation order.
     * @param nvars
     *            The #of unbound variables for each tail predicate is assigned
     *            by side-effect.
     * 
     * @return An array of the {@link IKeyOrder}s for each tail predicate. The
     *         array is correlated with the predicates index in the tail of the
     *         rule NOT its evaluation order.
     */
    protected IKeyOrder[] computeKeyOrderForEachTail(final IRule rule,
            final IJoinNexus joinNexus, final int[] order, final int[] nvars) {

        if (order == null)
            throw new IllegalArgumentException();

        if (order.length != rule.getTailCount())
            throw new IllegalArgumentException();

        final int tailCount = rule.getTailCount();

        final IKeyOrder[] a = new IKeyOrder[tailCount];
        
        final IBindingSet bindingSet = joinNexus.newBindingSet(rule);
        
        for (int orderIndex = 0; orderIndex < tailCount; orderIndex++) {

            final int tailIndex = order[orderIndex];

            final IPredicate pred = rule.getTail(tailIndex);

            final IPredicate asBound = pred.asBound(bindingSet);
            
            final IKeyOrder keyOrder = joinNexus.getTailAccessPath(
                    asBound).getKeyOrder();

            if (DEBUG)
                log.debug("keyOrder=" + keyOrder + ", orderIndex=" + orderIndex
                        + ", tailIndex=" + orderIndex + ", pred=" + pred
                        + ", bindingSet=" + bindingSet + ", rule=" + rule);

            // save results.
            a[tailIndex] = keyOrder;
            nvars[tailIndex] = asBound.getVariableCount();
            
            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant t = pred.get(j);

                if (t.isVar()) {

                    final Var var = (Var) t;

                    if (DEBUG) {

                        log.debug("Propagating binding: pred=" + pred
                                        + ", var=" + var + ", bindingSet="
                                        + bindingSet);
                        
                    }
                    
                    bindingSet.set(var, joinNexus.fakeBinding(pred, var));

                }

            }

        }

        if (DEBUG) {

            log.debug("keyOrder[]=" + Arrays.toString(a) + ", nvars="
                    + Arrays.toString(nvars) + ", rule=" + rule);

        }

        return a;

    }

}
