package com.bigdata.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * State for a rule execution.
 * <p>
 * A variety of data is localized by this class in order to allow: (a) instances
 * of the same rule to execute concurrently, e.g., when evaluating different
 * subqueries; and (b) instances of a rule to be mapped across (new, new+db),
 * where "new" is a focusStore.
 * 
 * @see RuleUtil
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo factor out the focusStore stuff into a derived class specific to the
 *       RDF DB?
 *       <p>
 *       Another way to handle this is to associated each predicate with a
 *       relation and to have the focus store predicates belong to a different
 *       relation than the database predicates. For RDF DB only query, all JOINs
 *       are clearly self-joins since they are against the same relation but
 *       they often use different access paths (aka indices). When running a
 *       db+focusStore rule, the focusStore is a different relation having the
 *       same schema as the db (and typically having the same access paths
 *       declared).
 *       <p>
 *       This suggests that the focusStore + db stuff might be handled by an
 *       automatic rule re-write.
 * 
 * @todo I have pulled out the [justify] flag as it is not general purpose. It
 *       will need to be layered back into the {@link RuleState} for the RDF KB.
 *       It was also present in the {@link RuleUtil} classes since it is
 *       metadata that gets mapped across a set of rules that is being applied
 *       or brought to a fixed point.
 */
public class RuleState {

    protected static final Logger log = Logger.getLogger(RuleState.class);
    
    /**
     * The {@link Rule} being evaluated.
     */
    final protected Rule rule;
    
    /**
     * Statistics for the rule evaluation.
     */
    final public RuleStats stats;

    /**
     * The index of the term that will read from the {@link #focusStore}
     * (ignored when {@link #focusStore} is <code>null</code>).
     */
    final protected int focusIndex;
    
    /**
     * The optional focus store used for truth maintenance.
     */
    final protected IAccessPathFactory focusStore;
    
    /**
     * The persistent database.
     */
    final protected IAccessPathFactory database;
    
    /**
     * Used to buffer solutions for better performance. The buffer will
     * either write on the {@link #database} (if we are inserting, updating,
     * or deleting data) or it will feed an iterator (if you are computing
     * solutions without making them persistent).
     */
    final protected IBuffer<IBindingSet> buffer;
    
    /**
     * Instance bindings.
     * <p>
     * Note: You MUST {@link #resetBindings()} each time you execute a rule. If
     * the variable binding patterns are not restored (overwriting whatever
     * constants were last in those positions) then the wrong access path will
     * be selected and you will NOT get the expected entailments.
     */
    final private IBindingSet bindings;

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
    final protected Map<Var,Integer> depends;
    
    /**
     * Variant when not using a focusStore.
     * 
     * @param database
     * @param buffer
     * @return
     */
    public RuleState(Rule rule, IAccessPathFactory database,
            IBuffer<IBindingSet> buffer) {

        this(rule, 0/* index */, null/* focusStore */, database,
                buffer);

    }
    
    /**
     * 
     * @param focusIndex
     *            The index of the predicate in the tail of the {@link Rule}
     *            that will read from the "focusStore" (ignored if "focusStore"
     *            is <code>null</code>).
     * @param focusStore
     * @param database
     *            The persistent database.
     * @param buffer
     */
    public RuleState(Rule rule, int focusIndex, IAccessPathFactory focusStore,
            IAccessPathFactory database, IBuffer<IBindingSet> buffer) {

        if (rule == null)
            throw new IllegalArgumentException();
        
        final int tailCount = rule.getTailCount();
        
        if (focusIndex < 0 || focusIndex >= tailCount)
            throw new IllegalArgumentException();
        
        if (database == null)
            throw new IllegalArgumentException();
        
        if (buffer == null)
            throw new IllegalArgumentException();
        
        this.rule = rule;
        
        this.focusIndex = focusIndex;
        
        // MAY be null.
        this.focusStore = focusStore;
        
        // always defined.
        this.database = database;
        
        // always defined.
        this.buffer = buffer;
        
        // Allocate bindings for the rule.
        this.bindings = new ArrayBindingSet(rule.getVariableCount());

        // Allocate access path cache.
        this.accessPath = new IAccessPath[rule.getTailCount()];

        /*
         * @todo you can override this class to customize the evaluation order
         * but the evaluation order determination could also be moved into its
         * own interface.
         * 
         * @todo there is some stuff in here that might be specific to the RDF
         * DB.
         */
        
        // The evaluation order.
        this.order = computeEvaluationOrder();
        
        if (focusStore != null && focusIndex > 0) {
            
            /*
             * The rule of thumb is to always evaluate the predicate that
             * will read from the focusStore 1st since we presume that any
             * triple pattern reading on the [focusStore] will be
             * significantly smaller than any other triple pattern reading
             * on the fused view [focusStore + database].
             * 
             * However, there are counter examples. Loading schema and then
             * data into a database is one, e.g., the wordnet ontology
             * followed by the wordnet nouns data. Another is when the 2nd
             * predicate in the body is none bound - you are often better
             * off NOT running the all none bound predicate 1st.
             * 
             * Note: All of this logic is executed IFF the focusIndex is >
             * 0. When the focusIndex is 0, the focusStore is already being
             * read by the predicate that is first in the evaluation order.
             */

            /*
             * In this alternative, we simply refuse to order a 3-unbound
             * predicate to the 1st position in the evaluation order.
             */
            
            if (false && rule.getTailPredicate(order[focusIndex])
                    .getVariableCount() < 3) {

                /*
                 * Note: This does not work since we may have already done a
                 * rangeCount (w/o bindings) when computing the evaluation
                 * order so we can not undo that purely by considering the
                 * variable bindings. An alternative is to mark as "pinned"
                 * any part of the evaluation order that have already been
                 * tested in the data, e.g., by retaining the rangeCounts if
                 * computed by computeEvaluationOrder.  If we do that then
                 * we could try this simpler test again.
                 */
                
//                    /*
//                     * Swap the places of those predicates in the evaluation
//                     * order such that we will evaluate the predicate at the
//                     * focusIndex 1st.
//                     */
//
//                    int tmp = order[0];
//
//                    order[0] = order[focusIndex];
//
//                    order[focusIndex] = tmp;

            } else {

                /*
                 * In order to catch those counter examples we range count
                 * the predicate that is already 1st in the evaluate order
                 * against [focusStore+database] and then the predicate at
                 * the focusIndex against the [focusStore]. We then reorder
                 * the predicate at the focusIndex 1st iff it has a smaller
                 * range count that the predicate that is already first in
                 * the evaluation order.
                 * 
                 * @todo range counts here could be expensive when the index
                 * is remote or partitioned.
                 */
                
                /*
                 * Range count for the predicate that is 1st in the
                 * evaluation order. This will read against [focusStore +
                 * database].
                 */

                final long rangeCount1 = getAccessPath(order[0], null/* bindingSet */)
                        .rangeCount();

                /*
                 * Range count for the predicate at the focusIndex. This
                 * will read against [focusStore].
                 */

                final long rangeCount2 = getAccessPath(order[focusIndex],
                        null/* bindingSet */).rangeCount();

                if (rangeCount2 < rangeCount1) {

                    /*
                     * Swap the places of those predicates in the evaluation
                     * order such that we will evaluate the predicate at the
                     * focusIndex 1st.
                     */

                    int tmp = order[0];

                    order[0] = order[focusIndex];

                    order[focusIndex] = tmp;

                }

            }
            
        }
        

        /*
         * The dependency graph for the variables in the tail based on the
         * selected evaluation order.
         */
        this.depends = Collections
                .unmodifiableMap(computeVariableDependencyMap(order));

//        // initialize the bindings from the predicate declarations.
//        resetBindings();

        // collects statistics on the rule.
        this.stats = new RuleStats(this);
       
    }

    /**
     * The {@link Rule} that is being executed. 
     */
    public Rule getRule() {
        
        return rule;
        
    }
    
//    /**
//     * Apply the rule.
//     */
//    public void apply() {
//
//        /*
//         * @todo since we do this in this method we do not need to flush
//         * elsewhere.
//         * 
//         * @todo This is responsible for computing the #of entailments added to
//         * the focusStore or database as a side-effect of applying the rule.
//         * That number is can be obtained either
//         * {@link ISPOBuffer#flush(boolean)} or
//         * {@link AbstractTripleStore#getBufferCount()} but it will only be
//         * accurate when the buffer has been flushed before and after the rule.
//         * While it is a bit more efficient if we do not always force the
//         * buffers to be flushed since a few rules can be run in a sequence and
//         * do not depend on each other, but that is very rare. Also note that
//         * re-flushing an empty buffer is always a NOP.
//         */
//        
////        buffer.flush(true);
//        
//////            final int nbefore = focusStore != null ? focusStore.getStatementCount()
//////                    : database.getStatementCount();
//        
//        evaluator.apply(this);
//       
////        stats.numAdded = buffer.flush(true);
//        
//////            final int nafter = focusStore != null ? focusStore.getStatementCount()
//////                    : database.getStatementCount();
//        
//////            stats.numAdded = nafter - nbefore;
//        
//    }
    
    /**
     * Emits the selected bindings from the current {@link IBindingSet} onto the
     * buffer.
     * 
     * @todo the justification needs to be an optional part of the emitted
     *       tuple.
     * 
     * @todo the {explicit,inferred,axiom} marker should be a binding on the
     *       emitted tuple.
     * 
     * @todo the buffer should know how to select the variables from the current
     *       binding set. right now the "select" is described by the head.
     */
    protected void emit() {
        
//            SPO stmt = new SPO(get(head.s),get(head.p),get(head.o),StatementEnum.Inferred);
        
//            Justification jst = null;
//            
//            if(justify) {
//                
//                jst = new Justification(Rule.this, stmt, bindings.clone());
//                
//            }
        
        if(buffer.add(bindings)) {

            /*
             * @todo a counter here for the #of entailments that were
             * accepted by the filter.
             */
            
        }

        // #of entailments computed.
        stats.numComputed++;

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
     *       data are keys in a dictionary, which is the case for the RDF DB.
     */
    public String toString(IBindingSet bindingSet) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(rule.getName());
        
        sb.append(" : ");
        
        // write out bindings for the tail.
        
        int i = 0;
        
        for(Iterator<IPredicate> itr = rule.getTailPredicates(); itr.hasNext(); i++) {

            final IPredicate pred = itr.next();
            
            sb.append(pred.toString(bindingSet));
            
//            if (bindingSet == null) {

                // displays the evaluation order as an index on the predicate.

                sb.append("[" + order[i] + "]");
                
//            }
            
            if (itr.hasNext()) {
                
                sb.append(", ");
                
            }
            
        }

        sb.append(" -> ");
        
        // write out bindings for the head.
        {
            
            sb.append(rule.getHead().toString(bindingSet));

        }
        
        return sb.toString();

    }

    /**
     * Computes and returns the evaluation order (no side-effects).
     * <p>
     * The evaluation {@link #order}[] is determined by analysis of the
     * propagation of bindings. The most selective predicate is choosen first
     * (having the fewest unbound variables with ties broken by a range count on
     * the data) and "fake" bindings are propagated to the other predicates in
     * the tail. This process is repeated until all variables are bound and an
     * evaluation order has been determined.
     * 
     * @return The evaluation order. The indices in this array are correlated
     *         1:1 with the predicates in tail of the {@link Rule}.
     */
    public int[] computeEvaluationOrder() {

        // use private bindingSet to avoid side-effects.
        final IBindingSet bindingSet = new ArrayBindingSet(rule.getVariableCount());

        final int tailCount = rule.getTailCount();

        final int[] order = new int[tailCount];

        final boolean[] used = new boolean[tailCount];

        // clear array. -1 is used to detect logic errors.
        for (int i = 0; i < order.length; i++)
            order[i] = -1;

        for (int i = 0; i < tailCount; i++) {

            int index = -1;
            int minVarCount = Integer.MAX_VALUE;
            long minRangeCount = Long.MAX_VALUE;
            
            for( int j=0; j<tailCount; j++) {
                
                if(used[j]) continue; // already in the evaluation order. 
                
                final int varCount = rule.getTailPredicate(j)
                        .getVariableCount();

                if (varCount < minVarCount) {

                    index = j;

                    minVarCount = varCount;

                } else if (true && varCount == minVarCount) {

                    /*
                     * Tweaks the evaluation order for predicates where the
                     * #of variable bindings is the same by examining the
                     * range counts.
                     * 
                     * Note: In doing this, we disregard the bindings that
                     * were propagated since they are -1 and will NOT match
                     * anything anywhere!
                     * 
                     * Note: In the case where some other predicate is
                     * already first in the evaluation order by the virtue
                     * of having more variables bound this tweak is purely
                     * heuristic regardless of the fact that it considers
                     * the data. The reason is that the actual bindings that
                     * are propagated during the execution of the rule will
                     * determine which of the two predicates under
                     * consideration is, in fact, more selective in the
                     * data. However, in the special case where the two
                     * predicates are competing for the 1st position in the
                     * evaluation order, this "tweak" is exact.
                     * 
                     * @todo Some tails use the same triple pattern in both
                     * predicates. E.g., rdfs11 (u subClassOf v) (v
                     * subClassOf x). In these cases comparing range counts
                     * is pointless and could be avoided by testing for this
                     * pattern.
                     */
                    
                    if(minRangeCount == Integer.MAX_VALUE) {
                        
                        // range count of the current best choice (computed
                        // lazily).
                        minRangeCount = getAccessPath(index, null/* bindingSet */)
                                .rangeCount();

                    }

                    // range count of the current predicate under
                    // examination.
                    final long rangeCount = getAccessPath(j, null/* bindingSet */)
                            .rangeCount();
                    
                    if (rangeCount < minRangeCount) {

                        /*
                         * choose the predicate that is more selective given
                         * the variable bindings.
                         */
                        
                        index = j;
                        
                        minVarCount = varCount;
                        
                        minRangeCount = rangeCount;
                        
                    }
                    
                }
                
            }

            if (index == -1)
                throw new AssertionError();
            
            if (used[index])
                throw new AssertionError("Attempting to reuse predicate: index="+i+"\n"+this);
            
            order[i] = index;
            
            used[index] = true;
            
            // set fake bindings for this predicate.
            {
                
                final IPredicate pred = rule.getTailPredicate(index);

                final int arity = pred.arity();

                for (int z = 0; z < arity; z++) {

                    final IVariableOrConstant v = pred.get(z);

                    if (v.isVar()) {

                        bindingSet.set((IVariable) v, FAKE);

                    }

                }
                
            }
            
        }

        if (log.isInfoEnabled())
            log.info(rule.getName() + ": order=" + Arrays.toString(order));
        
        return order;
        
    }
    /**
     * Used to mark fake bindings by {@link #computeEvaluationOrder()}.
     */
    private static final transient IConstant<String> FAKE = new Constant<String>(
            "Fake Binding");
    
    /**
     * Given an evaluation <i>order</i>, construct the dependency graph for the
     * variables in the tail of the rule.
     * 
     * @param order
     *            The evaluation order.
     * 
     * @return The map from variable to the 1st predicate in evaluation order
     *         where it is bound. The values of this map are indices into
     *         <i>order</i>.
     */
    public Map<Var,Integer> computeVariableDependencyMap(int[] order) {
        
        if (order == null)
            throw new IllegalArgumentException();
        
        if (order.length != rule.getTailCount())
            throw new IllegalArgumentException();
        
        final Map<Var,Integer> depends = new HashMap<Var,Integer>();
        
        final int tailCount = rule.getTailCount();
        
        for(int i=0; i<tailCount; i++) {
            
            final IPredicate pred = rule.getTailPredicate(order[i]);
            
            final int arity = pred.arity();
            
            for(int j=0; j<arity; j++) {

                final IVariableOrConstant t = pred.get(j);
                
                if(t.isVar()) {
                
                    final Var var = (Var)t;
                
                    if(!depends.containsKey(var)) {
                        
                        depends.put(var, i);
                        
                    }
                
                }
            
            }
                            
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
    protected void resetBindings() {

        bindings.clearAll();

        for (int i = 0; i < accessPath.length; i++) {

            accessPath[i] = null;

        }

    }
    
    /**
     * <p>
     * Return the current binding for the variable or constant.
     * </p>
     * 
     * @param var
     *            The variable or constant.
     * 
     * @return Its binding. The binding will be {@link #NULL} if a variable is
     *         not currently bound.
     * 
     * @throws NullPointerException
     *             if <i>var</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the variable is not used in the rule.
     * @throws IllegalArgumentException
     *             if var is a constant.
     * 
     * @see #set(Var, Object)
     * @see #bind(int, SPO)
     */
    public Object get(IVariableOrConstant var) {
    
        return get(var, true);
        
    }
    
    /**
     * <p>
     * Return the current binding for the variable or constant.
     * </p>
     * 
     * @param var
     *            The variable or constant.
     * @param required
     *            When <code>true</code> an exception is reported if the
     *            variable is not used in the rule. Note that specializing a
     *            rule often results in variables being replaced by
     *            constants such that an {@link IConstraint} might no longer
     *            be evaluable for the rule in terms of that variable.
     * 
     * @return Its binding. The binding will be <code>null</code> if a
     *         variable is not currently bound or if the variable is not
     *         used in the rule and <code>required := false</code>.
     * 
     * @throws NullPointerException
     *             if <i>var</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <code>required := true</code> and the variable is
     *             not used in the rule.
     * @throws IllegalArgumentException
     *             if var is a constant.
     * 
     * @see #set(Var, Object)
     * @see #bind(int, SPO)
     * 
     * FIXME it is not entirely satisfactory to have variable names
     * disappear from rules as they are specialized. It might be better to
     * define a BoundVar (extends Var) whose id was the bound value and
     * which reported true for both isVariable() and isConstant(). However
     * this would mean that var(x) != var(x,id) where the latter is bound to
     * a constant. Yet another alternative is to store a map of the bound
     * variables on the Rule (not the State) when the rule is specialized
     * and to use that information when clearing setting, getting, or
     * clearing bindings in State.
     */
    public Object get(IVariableOrConstant var, boolean required) {
        
//        if (var == null)
//            throw new NullPointerException();

        if (var.isConstant()) {

            return var.get();
            
        }

        if (required && !rule.isDeclared((IVariable)var)) {

            throw new IllegalArgumentException("Not declared: " + var + " by "
                    + rule);
            
        }
        
        return bindings.get((IVariable) var);
                
    }
    
    /**
     * Binds the variable.
     * <p>
     * Note: The cached {@link IAccessPath} for a predicate is cleared if a
     * variable binding is set on that predicate.
     * 
     * @param var
     *            A variable that appears in that predicate.
     * @param val
     *            The value to be bound on the variable.
     * 
     * @throws NullPointerException
     *             if the variable is null.
     * @throws IndexOutOfBoundsException
     *             if the predicate index is out of range.
     * 
     * @see #bind(int, Object)
     */
//    * @throws IllegalArgumentException
//    *             if the variable does not appear in the rule.
    public void set(IVariable var, Object val) {

//        // verify variable declared by the rule.
//        if(!rule.isDeclared(var)) throw new IllegalArgumentException();

        // bind the variable.
        bindings.set(var, new Constant(val));

        final int tailCount = rule.getTailCount();
        
        // clear cached access path for preds using that variable.
        for (int i = 0; i < tailCount; i++) {

            final IPredicate pred = rule.getTailPredicate(i);

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
     *            The index of the predicate in the body of the rule.
     * @param spo
     *            A statement materialized by the triple pattern that predicate.
     * 
     * @return <code>true</code> iff the binding is allowed (<code>false</code>
     *         if it would violate any of the {@link IConstraint}s declared for
     *         the {@link Rule}).
     * 
     * @throws NullPointerException
     *             if the statement is <code>null</code>.
     * @throws IndexOutOfBoundsException
     *             if the index is out of bounds.
     * 
     * @see #clearDownstreamBindings(int index)
     */
    @SuppressWarnings("unchecked")
    public boolean bind(int index, Object e) {
       
        // propagate bindings.
        rule.getTailPredicate(index).copyValues(e, bindings);

        // verify constraints.
        return rule.isLegal(bindings);
        
    }
    
    /**
     * Clear downstream bindings in the evaluation {@link #order}[]. If a
     * variable in a downstream predicate was 1st bound by an upstream predicate
     * as identified by {@link #depends} then its value is NOT cleared.
     * <p>
     * Note: You MUST {@link #resetBindings()} before you evaluate a rule.
     * <p>
     * Note: You MUST {@link #clearDownstreamBindings(int)} bindings before you
     * (re-)evaluate a subquery. Failure to do so will leave stale bindings in
     * place which will cause {@link #getAccessPath(int)} to identify the wrong
     * access path, and hence select the wrong data.
     * 
     * @param index
     *            The index of the predicate whose values you intend to
     *            {@link #bind(int, Object)}.
     */
    protected void clearDownstreamBindings(int index) {

        final int tailCount = rule.getTailCount();
        
        for (int i = index; i < tailCount; i++) {

            final IPredicate pred = rule.getTailPredicate(order[index]);

            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant t = pred.get(j);

                if (t.isVar()) {

                    final int k = depends.get((Var) t);

                    if (k >= index) {

                        set((Var) t, null);

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
     * cache does not have an entry for the desired access path the one is
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
     */
    public IAccessPath getAccessPath(int index) {
        
        // check the cache.
        IAccessPath accessPath = this.accessPath[index];
        
        if (accessPath == null) {

            accessPath = getAccessPath(index, null/* bindingSet */);

            // update the cache.
            this.accessPath[index] = accessPath;
        
        }
    
        return accessPath;

    }

    /**
     * Return the iterator that should be used to read from the selected
     * {@link IPredicate}.
     * 
     * @param index
     *            The index of the {@link IPredicate} in the tail of the
     *            {@link Rule}.
     * 
     * @return The iterator.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     */
    public IChunkedIterator iterator(int index) {

        return getAccessPath(index).iterator();
        
    }
    
    /**
     * Return the {@link IAccessPath} that would be used to read from the
     * selected {@link IPredicate} (no caching).
     * 
     * @param index
     *            The index of the {@link IPredicate} in the tail of the
     *            {@link Rule}.
     * @param bindingSet
     *            When non-<code>null</code>, the bindings will be used to
     *            generate the {@link IAccessPath}. When <code>null</code>
     *            the {@link IAccessPath} will use wildcards in every position
     *            where the predicate declares a variable.
     * 
     * @return The {@link IAccessPath}.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     */
    public IAccessPath getAccessPath(int index, IBindingSet bindingSet) {

        final IAccessPath accessPath;
        final IPredicate predicate;

        if (bindingSet != null) {

            // based on the current bindings.

            //                final VarOrConstant s = bindings[index * N + 0];
            //                final VarOrConstant p = bindings[index * N + 1];
            //                final VarOrConstant o = bindings[index * N + 2];

            predicate = rule.getTailPredicate(index).asBound(bindingSet);

        } else {

            // as declared by the predicate (no bindings).

            predicate = rule.getTailPredicate(index);

        }

        if (focusStore == null) {

            accessPath = database.getAccessPath(predicate);

        } else {

            if (index == focusIndex) {

                accessPath = focusStore.getAccessPath(predicate);

            } else {

                /*
                 * Return a read-only access path for the fused view
                 * [focusStore + database].
                 */

                return new AccessPathFusedView(focusStore
                        .getAccessPath(predicate), database
                        .getAccessPath(predicate));

            }

        }

        return accessPath;

    }

}
