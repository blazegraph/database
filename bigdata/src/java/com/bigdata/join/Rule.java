/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.join;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A rule selects variables from solutions computed for the conjunctive query
 * (aka JOIN) of the {@link IPredicate}s in the body of the {@link Rule}. Each
 * solution is an {@link IBindingSet} and the variables in that
 * {@link IBindingSet} are those specified in the head of the {@link Rule}.
 * Each query pattern is an {@link IPredicate}. The {@link Rule} may express
 * {@link IConstraint}s on the allowable solutions. In addition, the
 * {@link IPredicate}s in the body of the {@link Rule} may have constraints
 * that will be evaluated close to the data.
 * <p>
 * Note: You MUST {@link #resetBindings()} each time you execute a rule. If the
 * variable binding patterns are not restored (overwriting whatever constants
 * were last in those positions) then the wrong access path will be selected and
 * you will NOT get the expected entailments.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo integration with package providing magic set rewrites of rules in order
 *       to test whether or not a statement is still provable when it is
 *       retracted during TM.
 * 
 * @todo add an XML serialization and parser for rules so that the rule sets may
 *       be declared. some very specialized rules might not be handled in this
 *       manner but the vast majority are executed as nested subqueries and can
 *       be just declared.
 *       <p>
 *       This will make it possible for people to extend the rule sets, but
 *       there are interactions in the rules choosen for evaluation during
 *       forward closure and those choosen for evaluation at query time.
 */
abstract public class Rule {

    final static public Logger log = Logger.getLogger(Rule.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Singleton factory for {@link Var}s.
     * <p>
     * Note: While only a single instance of a variable object will be created
     * for any given variable name, the "scope" of the variable is always
     * constrained by the rule within which it is used. The purpose of the
     * singleton factory is to let us test for the same variable using "=="
     * (reference testing) and also to have a shorthand for variable creation.
     * 
     * @param name
     *            The variable name.
     * 
     * @return The singleton variable for that name.
     * 
     * @see Var#var(String)
     */
    static protected Var var(String name) {
    
        return Var.var(name);

    }
    
    /**
     * Name of the rule.
     */
    final private String name;
    
    /**
     * The head of the rule.
     */
    final protected IPredicate head;

    /**
     * The body of the rule -or- <code>null</code> if the body of the rule
     * is empty.
     */
    final protected IPredicate[] body;

    /**
     * Optional constraints on the bindings.
     */
    final protected IConstraint[] constraints;
    
    /**
     * The #of terms in the tail of the rule.
     */
    public int getTailCount() {
        
        return body.length;
        
    }
    
    /**
     * By default the simple name of the class.
     */
    public String getName() {

        if(name==null) {
        
            return getClass().getSimpleName();
            
        } else {
            
            return name;
            
        }

    }
    
    /**
     * Externalizes the rule displaying variable names and term identifiers for
     * constants.
     * 
     * @see #toString(AbstractTripleStore)
     */
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getName());
        
        sb.append(" : ");
        
        for(int i=0; i<body.length; i++) {
            
            sb.append(body[i].toString());

            if(i+1<body.length) {
                
                sb.append(", ");
                
            }
            
        }

        sb.append(" -> ");
        
        sb.append(head.toString());
        
        return sb.toString();
        
    }

    /**
     * Rule ctor.
     * 
     * @param name
     *            A label for the rule (optional).
     * @param head
     *            The subset of bindings that are selected by the rule.
     * @param body
     *            The body of the rule.
     * @param constraints
     *            An array of constaints on the legal states of the bindings
     *            materialized for the rule.
     */
    public Rule(String name, IPredicate head, IPredicate[] body, IConstraint[] constraints) {
        
        this.name = name;
        
        assert head != null;

        assert body != null;
        
        // the head of the rule.
        this.head = head;

        // the predicate declarations for the body.
        this.body = body;
        
        for(int i=0; i<body.length; i++) {
            
            assert body[i] != null;
            
        }
        
        // constraint(s) on the variable bindings (MAY be null). 
        this.constraints = constraints;

        if(constraints!=null) {

            for(int i=0; i<constraints.length; i++) {
            
                assert constraints[i] != null;
            
            }
            
        }

    }

    /**
     * Specialize a rule by binding zero or more variables and adding zero or
     * more constraints.
     * <p>
     * Note: The new rule will always be an instance of {@link SpecializedRule}.
     * The name of the new rule will be derived from the name of the old rule
     * with an appended single quote to indicate that it is a derived variant.
     * <p>
     * Note: An attempt to bind a variable not declared by the rule will be
     * ignored.
     * 
     * @param bindingSet
     *            Bindings for zero or more free variables in this rule. The
     *            rule will be rewritten such that the variable is replaced by
     *            the binding throughout the rule.
     * @param constraints
     *            An array of additional constraints to be imposed on the rule
     *            (optional).
     * @return The specialized rule.
     * 
     * @exception IllegalArgumentException
     *                if <i>bindings</i> is <code>null</code>.
     * 
     * @todo factor {@link SpecializedRule} into a declaration and delegate to
     *       the {@link AbstractRuleNestedSubquery} as an evaluation strategy.
     */
    public Rule specialize(IBindingSet bindingSet, IConstraint[] constraints ) {

        if (bindingSet == null)
            throw new IllegalArgumentException();

        if(!(AbstractRuleNestedSubquery.class.isAssignableFrom(this.getClass()))) {
            
            /*
             * Rules that do not extend the nested subquery execution strategy
             * can not be specialized at this time. In order to do so we would
             * have to dynamically create an instance of the appropriate class.
             * This is not possible right now owing to the variety of
             * constructor signatures throughout the subclass hierarchy of Rule.
             */

//            throw new UnsupportedOperationException();
            
            log.warn("Rule does not extend: "+AbstractRuleNestedSubquery.class);
            
        }
        
        /*
         * The name of the specialized rule.
         */

        final String newName = getName()+"'";
        
        /*
         * Setup the new head and the body for the new rule by applying the
         * bindings.
         */
        
        final IPredicate newHead = head.asBound(bindingSet);
        
        final IPredicate[] newTail = bind(body,bindingSet);
        
        /*
         * Setup the new constraints. We do not test for whether or not two
         * constraints are the same, we just append the new constraints to the
         * end of the old constraints. The rest of the logic just covers the
         * edge cases where one or the other of the constraint arrays is null or
         * empty.
         */

        final IConstraint[] newConstraint;

        if (constraints == null || constraints.length == 0) {

            newConstraint = this.constraints;
            
        } else if (this.constraints == null || this.constraints.length == 0) {
            
            newConstraint = constraints;
            
        } else {

            int len = constraints.length + this.constraints.length;

            newConstraint = new IConstraint[len];

            System.arraycopy(this.constraints, 0, newConstraint, 0,
                    this.constraints.length);

            System.arraycopy(constraints, 0, newConstraint,
                    this.constraints.length, constraints.length);
            
        }

//        /*
//         * Use reflection to instantiate the rule.
//         */
//        
//        Class<?extends Rule> cls = this.getClass();
//        
//        final Rule newRule;
//
//        try {
//            /*
//             * Note: this require a uniform constructor for all rule classes.
//             */
//            Constructor<? extends Rule> ctor = cls.getConstructor(new Class[] {
//                    AbstractTripleStore.class,// database
//                    Pred.class,// head
//                    (new Pred[0]).getClass(),// tail
//                    (new IConstraint[0]).getClass() // constraints.
//                    });
//
//            newRule = ctor.newInstance(new Object[] {
//                    null/* database(not used) */, newHead, newTail,
//                    newConstraint });
//            
//        } catch (Throwable t) {
//
//            throw new RuntimeException("Could not specialize rule: " + t);
//
//        }
        
        Rule newRule = new SpecializedRule(newName, newHead, newTail,
                newConstraint);

        return newRule;

    }

    private IPredicate[] bind(IPredicate[] predicates, IBindingSet bindingSet) {
        
        IPredicate[] tmp = new IPredicate[predicates.length];
        
        for(int i=0; i<predicates.length; i++) {
            
            tmp[i] = predicates[i].asBound(bindingSet);
            
        }
        
        return tmp;
        
    }
    
    /**
     * Variant when not using a focusStore.
     * 
     * @param justify
     * @param database
     * @param buffer
     * @return
     */
    /*public*/ State newState(boolean justify,
                IAccessPathFactory database,
                IBindingSetBuffer buffer) {
        
        return newState(0/* index */, justify, null/* focusStore */,
                database, buffer);

    }
    
    public State newState(int index, boolean justify,
                IAccessPathFactory focusStore,
                IAccessPathFactory database,
                IBindingSetBuffer buffer) {
        
        return new State(index, justify, focusStore, database, buffer);
        
    }
    
    /**
     * Return the variables in common for two {@link IPredicate}s.
     * 
     * @param index1
     *            The index of a predicate in the {@link #body}.
     * 
     * @param index2
     *            The index of a different predicate in the {@link #body}.
     * 
     * @return The variables in common -or- <code>null</code> iff there are no
     *         variables in common.
     * 
     * @throws IllegalArgumentException
     *             if the two predicate indices are the same.
     * @throws IndexOutOfBoundsException
     *             if either index is out of bounds.
     */
    public Set<Var> getSharedVars(int index1, int index2) {

        if (index1 == index2) {

            throw new IllegalArgumentException();
            
        }
        
        final Set<Var> vars = new HashSet<Var>();
        
        final int arity1 = body[index1].arity(); 

        final int arity2 = body[index2].arity();
        
        for(int i=0; i<arity1; i++ ) {
            
            IVariableOrConstant avar = body[index1].get(i);
            
            if(avar.isConstant()) continue;
                
            for (int j = 0; j < arity2; j++) {

                if(body[index2].get(j).equals(avar)) {
                    
                    vars.add((Var)avar);
                    
                }
                
            }

        }
        
        return vars;

    }

    /**
     * An inner class comprising the state for a rule execution.
     * <p>
     * A variety of data is localized by this class in order to allow: (a)
     * instances of the same rule to execute concurrently, e.g., when evaluating
     * different subqueries; and (b) instances of a rule to be mapped across
     * (new, new+db), where "new" is a focusStore.
     * 
     * @see Rule#fixedPoint(ClosureStats, Rule[], boolean, AbstractTripleStore,
     *      AbstractTripleStore, SPOAssertionBuffer)
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo consider the use of an optional bloom filter for the emitted
     *       triples on either the closure stats, the rule, or the rule
     *       execution state. The bloom filter provides efficient fast rejection
     *       of keys that are not in the index. If the bloom filter reports that
     *       a key is in the index then we either have to test the index to
     *       verify that the result is not a false positive or just emit the
     *       result anyway and let the statement indices filter out duplicates.
     *       A bloom filter is only additive, so it can not be reused across any
     *       context which allows delete. So it could be attached to the
     *       inference engine as long as it was discarded on remove, or to a
     *       TMStatementBuffer, etc.
     */
    public class State {

        /**
         * Statistics for the rule evaluation.
         */
        final public RuleStats stats;

        /**
         * True iff {@link Justification}s are to be generated.
         */
        public final boolean justify;
        
        /**
         * The index of the term that will read from the {@link #focusStore}
         * (ignored when {@link #focusStore} is <code>null</code>).
         */
        public final int focusIndex;
        
        /**
         * The optional focus store used for truth maintenance.
         */
        public final IAccessPathFactory focusStore;
        
        /**
         * The persistent database.
         */
        public final IAccessPathFactory database;
        
        /**
         * Used to buffer solutions for better performance. The buffer will
         * either write on the {@link #database} (if we are inserting, updating,
         * or deleting data) or it will feed an iterator (if you are computing
         * solutions without making them persistent).
         */
        public final IBindingSetBuffer buffer;
        
        /**
         * Instance bindings.
         */
        final private IBindingSet bindings;

        /**
         * The {@link IAccessPath} corresponding to each {@link IPredicate} in the
         * {@link #body} of the {@link Rule}.
         * <p>
         * Note: The corresponding access path in this array is invalidated by
         * {@link #bind(int, SPO)} and
         * {@link #set(com.bigdata.rdf.inf.Rule.Var, long)} since binding a variable
         * (or changing the binding of a variable) will in general change the
         * selectivity of the access path (it may have a different range count).
         * <p>
         * Note: {@link #resetBindings()} clears all elements of this array.
         */
        final private IAccessPath[] accessPath;
        
        /**
         * The evaluation order for the predicates in the {@link #body} of the rule.
         */
        final protected int[] order;
        
        /**
         * Map from the variable to the index of the predicate in {@link #body} that
         * first binds that variable in terms of the evaluation {@link #order}. This
         * information is used by {@link #clearDownstreamBindings(int)} to decide
         * which variables should be preserved.  If a variable is listed here with
         * a lower index than the starting index to be cleared then its binding is
         * NOT cleared.
         */
        final protected Map<Var,Integer> depends;
        
        /**
         * 
         * @param focusIndex
         *            The index of the predicate in {@link Rule#body} that will
         *            read from the "focusStore" (ignored if "focusStore" is
         *            <code>null</code>).
         * @param focusStore
         * @param database
         *            The persistent database.
         * @param buffer
         */
        protected State(int focusIndex, boolean justify,
                IAccessPathFactory focusStore, IAccessPathFactory database,
                IBindingSetBuffer buffer) {

            assert focusIndex >= 0;
            assert focusIndex <= body.length;
            
            assert database != null;
            
            assert buffer != null;
            
            this.justify = justify;
            
            this.focusIndex = focusIndex;
            
            // MAY be null.
            this.focusStore = focusStore;
            
            // always defined.
            this.database = database;
            
            this.buffer = buffer;
            
            // Allocate bindings for the tail : @todo use factory for specialized impls, pre-size, etc.
            this.bindings = new BindingSet();

            // Allocate access path cache.
            this.accessPath = new IAccessPath[body.length];

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
                
                if (false && body[order[focusIndex]].getVariableCount() < 3) {

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
            

            // The 1st dependency map for each variable in the tail.
            this.depends = computeVariableDependencyMap();

            // initialize the bindings from the predicate declarations.
            resetBindings();

            // collects statistics on the rule.
            this.stats = new RuleStats(this);
            
        }

        /**
         * The {@link Rule} that is being executed. 
         */
        public Rule getRule() {
            
            return Rule.this;
            
        }
        
        /**
         * Apply the rule.
         * <p>
         * Note: This is responsible for computing the #of entailments added to
         * the focusStore or database as a side-effect of applying the rule.
         * That number is can be obtained either
         * {@link ISPOBuffer#flush(boolean)} or
         * {@link AbstractTripleStore#getStatementCount()} but it will only be
         * accurate when the buffer has been flushed before and after the rule.
         * While it is a bit more efficient if we do not always force the
         * buffers to be flushed since a few rules can be run in a sequence and
         * do not depend on each other, but that is very rare. Also note that
         * re-flushing an empty buffer is always a NOP.
         */
        public void apply() {

            /*
             * @todo since we in this method we do not need to flush elsewhere.
             */
            
            buffer.flush(true);
            
//            final int nbefore = focusStore != null ? focusStore.getStatementCount()
//                    : database.getStatementCount();
            
            Rule.this.apply(this);
           
            stats.numAdded = buffer.flush(true);
            
//            final int nafter = focusStore != null ? focusStore.getStatementCount()
//                    : database.getStatementCount();
            
//            stats.numAdded = nafter - nbefore;
            
        }
        
        /**
         * Emits the selected bindings from the current {@link IBindingSet} onto
         * the {@link IBindingSetBuffer}.
         * 
         * @todo the justification needs to be an optional part of the emitted
         *       tuple.
         * 
         * @todo the {explicit,inferred,axiom} marker should be a binding on the
         *       emitted tuple.
         *       
         * @todo the buffer should know how to select the variables from the
         *       current binding set. right now the "select" is described by the
         *       head.
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
         * Externalizes the rule.
         * 
         * @param db
         *            The database.
         */
        public String toString() {

            return toString(null);
            
        }
        
        /**
         * Externalizes the rule using the database to resolve constants.
         * 
         * @param db
         *            The database.
         * @param bindingSet
         *            When non-<code>null</code>, the current variable
         *            bindings will be displayed. Otherwise, the names of
         *            variables will be displayed rather than their bindings.
         */
        public String toString(IBindingSet bindingSet) {

            final StringBuilder sb = new StringBuilder();
            
            sb.append(getName());
            
            sb.append(" : ");
            
            // write out bindings for the tail.
            
            for(int i=0; i<body.length; i++) {

                sb.append(body[i].toString(bindingSet));
                
                if (bindingSet == null) {

                    // displays the evaluation order as an index on the predicate.

                    sb.append("[" + order[i] + "]");
                    
                }
                
                if(i+1<body.length) {
                    
                    sb.append(", ");
                    
                }
                
            }

            sb.append(" -> ");
            
            // write out bindings for the head.
            {
                
                IPredicate pred = head;

                sb.append(pred.toString(bindingSet));

            }
            
            return sb.toString();

        }
                
        /**
         * The evaluation {@link #order}[] is determined by analysis of the
         * propagation of bindings. The most selective predicate is choosen
         * first (having the fewest unbound variables with ties broken by a
         * range count on the data) and "fake" bindings are propagated to the
         * other predicates in the tail. This process is repeated until all
         * variables are bound and an evaluation order has been determined. The
         * evaluation order is written into the {@link #order}[]. The indices
         * in that array are correlated 1:1 with the predicates in {@link #body}.
         * 
         * @return The evaluation {@link #order}[].
         */
        private int[] computeEvaluationOrder() {

            // use private bindingSet to avoid side-effects.
            final IBindingSet bindingSet = new BindingSet();

            final int[] order = new int[body.length];
            
            final boolean[] used = new boolean[body.length];
            
            // clear array.  -1 is used to detect logic errors.
            for( int i=0; i<order.length; i++ ) order[i] = -1;
            
            for(int i=0; i<body.length; i++) {

                int index = -1;
                int minVarCount = Integer.MAX_VALUE;
                long minRangeCount = Long.MAX_VALUE;
                
                for( int j=0; j<body.length; j++) {
                    
                    if(used[j]) continue; // already in the evaluation order. 
                    
                    final int varCount = body[j].getVariableCount();

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

                if(index==-1) throw new AssertionError();
                
                if (used[index])
                    throw new AssertionError("Attempting to reuse predicate: index="+i+"\n"+this);
                
                order[i] = index;
                
                used[index] = true;
                
                // set fake bindings for this predicate.
                {
                    
                    final IPredicate pred = body[index];
                    
                    final int arity = pred.arity();
                    
                    for(int z=0; z<arity; z++) {
                        
                        final IVariableOrConstant v = pred.get(z);
                        
                        if (v.isVar()) {

                            bindingSet.set(name, FAKE);
                            
                        }
                        
                    }
                    
                }
                
            }

            if (log.isInfoEnabled())
                log.info(getName() + ": order="+Arrays.toString(order));
            
            return order;
            
        }
        /**
         * Used to mark fake bindings by {@link #computeEvaluationOrder()}.
         */
        private static final transient String FAKE = "Fake";
        
        /**
         * Records the index of the 1st predicate in evaluation {@link #order} where
         * each variable is mentioned in the tail of the rule. This is the predicate
         * that will bind the variable during evaluation.
         * 
         * @return The map from variable to the 1st predicate in evaluation order
         *         where it is bound. The values of this map are indices into
         *         {@link #order}.
         */
        private Map<Var,Integer> computeVariableDependencyMap() {
            
            final Map<Var,Integer> depends = new HashMap<Var,Integer>();
            
            for(int i=0; i<body.length; i++) {
                
                final IPredicate pred = body[order[i]];
                
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
            
        }
        
        /**
         * Return true iff the selected predicate is fully bound by {@link #bindings}.
         * <p>
         * Note: This may be used as part of a strategy to determine the evaluate
         * order of the rule against some data. However, once you begin to bind
         * variables the predicate(s) will remain bound until you
         * {@link #resetBindings()}. For this reason the bindings for a
         * {@link Rule} MUST be reset you can select an evaluation strategy for that
         * {@link Rule}.
         * 
         * @param index
         *            The index of a predicate declared in the {@link #body} of the
         *            rule.
         * 
         * @return True iff it is fully bound (a mixture of constants and/or bound
         *         variables).
         * 
         * @throws IndexOutOfBoundsException
         *             if the index is out of bounds.
         */
        public boolean isFullyBound(int index) {
            
            final IPredicate pred = body[index];

            final int arity = pred.arity();
            
            for(int j=0; j<arity; j++) {

                final IVariableOrConstant t = pred.get(j);
                
                // check any variables.
                if (t.isVar()) {
                    
                    // if a variable is unbound then return false.
                    if(!bindings.isBound(t.getName())) {
                        
                        return false;
                        
                    }
                    
                }
                
            }
            
            return true;
            
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
            
//            if (var == null)
//                throw new NullPointerException();

            if (var.isConstant()) {

                return var.get();
                
            }

            final String name = var.getName();

            if (bindings.isBound(name)) {

                return bindings.getLong(name);
                
            }
            
            return null;
                    
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
         * @todo throws IllegalArgumentException
         *             if the variable does not appear in the rule.
         * 
         * @see #bind(int, SPO)
         */
        public void set(Var var, Object val) {

            // @todo verify variable declared by the rule.

            bindings.set(var.getName(), val);

            // clear cached access path for preds using that variable.
            for (int i = 0; i < body.length; i++) {

                final IPredicate pred = body[i];

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
         * Note: The bindings are propagated before the constraints are verified
         * so this method will have a side-effect on the bindings even if the
         * constraints were not satisified.
         * 
         * @param index
         *            The index of the predicate in the body of the rule.
         * @param spo
         *            A statement materialized by the triple pattern that
         *            predicate.
         * 
         * @return <code>true</code> iff the binding is allowed (<code>false</code>
         *         if it would violate any of the {@link IConstraint}s declared
         *         for the {@link Rule}).
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
            body[index].copyValues(e, bindings);

            // verify constraints.
            return checkConstraints(bindings);
            
        }
        
        /**
         * Return <code>true</code> unless the {@link IBindingSet} violates a
         * {@link IConstraint}s declared for this {@link Rule}.
         * 
         * @param bindingSet
         *            The binding set.
         *            
         * @return <code>true</code> unless a constraint is violated.
         * 
         * @see #bind(int, Object)
         */
        public boolean checkConstraints(IBindingSet bindingSet) {
            
            if (constraints != null) {

                // check constraints.
                
                for (int i = 0; i < constraints.length; i++) {

                    IConstraint constraint = constraints[i];
                    
                    if (!constraint.accept(bindingSet)) {

                        if(DEBUG) {
                            
                            log.debug("Rejected by "
                                    + constraint.getClass().getSimpleName()
                                    + "\n" + toString(bindings));
                            
                        }
                        
                        return false;

                    }

                }

            }
            
            return true;
            
        }
        
        /**
         * Clear downstream bindings in the evaluation {@link #order}[]. If a
         * variable in a downstream predicate was 1st bound by an upstream
         * predicate as identified by {@link #depends} then its value is NOT
         * cleared.
         * <p>
         * Note: You MUST {@link #resetBindings()} before you evaluate a rule.
         * <p>
         * Note: You MUST {@link #clearDownstreamBindings(int)} bindings before
         * you (re-)evaluate a subquery. Failure to do so will leave non-{@link #NULL}
         * bindings in place which will cause {@link #getAccessPath(int)} to
         * identify the wrong access path, and hence select the wrong data.
         * 
         * @param index
         *            The index of the predicate whose values you intend to
         *            {@link #bind(int, SPO)}.
         */
        protected void clearDownstreamBindings(int index) {

            for (int i = index; i < body.length; i++) {

                final IPredicate pred = body[order[index]];

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
         * obtained and placed into the cache before being returned to the
         * caller. The cache is invalidated by {@link #resetBindings()} and (on
         * a selective basis) by {@link #bind(int, SPO)} and
         * {@link #set(com.bigdata.rdf.inf.Rule.Var, long)}.
         * 
         * @param index
         *            The index into {@link #body}.
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
         * tail {@link IPredicate}. If the {@link IPredicate} is a {@link Triple} and is
         * associated with an {@link ISPOFilter} then that filter will be
         * incorporated by the returned {@link ISPOIterator}.
         * 
         * @param index
         *            The index into {@link #body}.
         * 
         * @return The {@link ISPOIterator}.
         * 
         * @throws IndexOutOfBoundsException
         *             if index is out of bounds.
         */
        public IChunkedIterator iterator(int index) {

            return getAccessPath(index).iterator();
            
        }
        
        /**
         * Return the {@link IAccessPath} that would be used to read from the
         * selected tail {@link IPredicate} (no caching).
         * 
         * @param index
         *            The index into {@link #body}.
         * @param bindingSet
         *            When non-<code>null</code>, the bindings will be used
         *            to generate the {@link IAccessPath}. When
         *            <code>null</code> the {@link IAccessPath} will use
         *            wildcards in every position where the predicate declares a
         *            variable.
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
                
                predicate = body[index].asBound(bindingSet);
                
            } else {
                
                // as declared by the predicate (no bindings).
                
                predicate = body[index];
                
            }

            if (focusStore == null) {
                 
                accessPath = database.getAccessPath( predicate );

            } else {

                if (index == focusIndex) {

                    accessPath = focusStore.getAccessPath( predicate );

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
    
    /**
     * Apply the rule.
     * 
     * @param state
     *            The rule execution state.
     */
    abstract protected void apply( State state );

    /**
     * Map N executions of rule over the terms in the tail. In each pass term[i]
     * will read from <i>tmp</i> and the other term(s) will read from a fused
     * view of <i>tmp</i> and the {@link #db}.
     * <p>
     * Within each pass, the decision on the order in which the terms will be
     * evaluated is made based on the rangeCount() for the {@link IAccessPath}
     * associated with the triple pattern for that term.
     * <p>
     * The N passes themselves are executed concurrently.
     * 
     * @param rule
     *            The rule to execute.
     * @param justify
     *            True iff justifications should be generated.
     * @param focusStore
     *            An optional {@link AbstractTripleStore}.
     * @param database
     *            The persistent database.
     * @param buffer
     *            The rules will write the entailments (and optionally the
     *            justifications) on the buffer.
     * 
     * @todo We can in fact run the variations of the rule in parallel using an
     *       {@link ExecutorService}. The {@link InferenceEngine} or perhaps
     *       the database should declare this service. The service could be used
     *       for both map parallelism and for parallelism of subqueries within
     *       rules.
     *       <p>
     *       Make the {@link SPOAssertionBuffer} thread-safe so that the N passes may be
     *       concurrent and they all write onto the same buffer, hence their
     *       union is automatically visible in the iterator wrapping that
     *       buffer.
     *       <p>
     *       The {@link #bindings}, {@link #order}, and {@link #depends} all
     *       need to be per-thread (done).
     *       <p>
     *       The {@link RuleStats} needs to be thread-safe (they are now per-{@link State),
     *       but {@link RuleStats#add(RuleStats)} also needs to be thread-safe.
     */
    public RuleStats apply(boolean justify, IAccessPathFactory focusStore,
            IAccessPathFactory database, IBindingSetBuffer buffer) { 
    
        if (focusStore == null) {

            /*
             * Just close the database.
             */

            State state = newState(0/*focusIndex*/, justify, focusStore, database, buffer);

            state.apply();

            return state.stats;
            
        }
        
        /*
         * When focusStore != null we need to run the rule N times, where N is
         * the #of predicates in the body. In each pass we choose body[i] as the
         * focusIndex - the predicate that will read from the [focusStore]. All
         * other predicates will read from the fused view of the [focusStore]
         * and the [database].
         * 
         * Note: when the rule has a single predicate in the tail, the predicate
         * is only run against [focusStore] rather than [datbase] or [focusStore +
         * database].
         * 
         * Note: all of these passes write on the same SPOBuffer. This has the
         * same effect as a UNION over the entailments of the individual passes.
         * 
         * @todo run the N passes in parallel.
         */

        // statistics aggregated across the rule variants that we will run.
        RuleStats stats = new RuleStats(this);

        State[] state = new State[body.length];
        
        for(int i=0; i<body.length; i++) {
            
            state[i] = newState(i/*focusIndex*/,justify,focusStore,database,buffer);

            state[i].apply();
            
            stats.add( state[i].stats );
            
        }
        
        return stats;
        
    }

    /**
     * Computes the "fixed point" of a specified rule set, the persistent
     * database, and an optional "focus" data set using set-at-a-time
     * processing.
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * to all data in turn. Entailments computed in each round are fed back into
     * either the "focusStore" or the database (depending on how things are
     * setup) so that derived entailments may be computed in a succession of
     * rounds. The process halts when no new entailments are computed in a given
     * round.
     * <p>
     * Note: When loading a new data set into the database, the "focusStore"
     * should contain the statements that were read from the data source, e.g.,
     * some RDF/XML file and the "buffer" should be configured to write on the
     * "database".
     * 
     * @param closureStats
     *            Used to aggregate statistics across the fixed point for a
     *            series of rule rules (the fast closure method does this).
     * @param rules
     *            The rules to be executed.
     * @param justify
     *            True iff justifications should be generated (use only when
     *            entailments are being written into the database and then iff
     *            the truth maintenance strategy requires justification chains).
     * @param focusStore
     *            An optional store containing statements. This is typically
     *            used in one of two ways: (1) incremental loading of new data
     *            into the database; and (2) collecting entailments of
     *            statements that are being removed during truth maintenance.
     *            When <code>null</code> the closure of the <i>database</i>
     *            will be performed in the absence of any new information.
     * @param database
     *            The persistent database.
     * @param buffer
     *            Used to buffer generated entailments so that we can do
     *            efficient ordered writes on the statement indices. The buffer
     *            MUST write which ever of the optional <i>focusStore</i> or
     *            the <i>database</i> you want to fix point. The buffer is
     *            flushed periodically during processing and will be empty when
     *            this method returns. All entailments will be in whichever
     *            store the buffer was configured to write upon.
     * 
     * @return Some statistics about the fixed point computation.
     */
    static public ClosureStats fixedPoint(ClosureStats closureStats,
            Rule[] rules, boolean justify, IAccessPathFactory focusStore,
            IAccessPathFactory database, IBindingSetBuffer buffer) {
        
        final int nrules = rules.length;

        /*
         * We will fix point whichever store the buffer is writing on.
         * 
         * FIXME the focus store access path factory needs to get in here
         * somehow.
         */
        
//        final AbstractTripleStore fixPointStore = buffer.getFocusStore();
        final IAccessPath fixPointStore = null;
        
        final long firstStatementCount = fixPointStore.rangeCount();

        final long begin = System.currentTimeMillis();

        if (DEBUG)
            log.debug("Closing kb with " + firstStatementCount + " statements");

        int round = 0;

        while (true) {

            final long numEntailmentsBefore = fixPointStore.rangeCount();
            
            for (int i = 0; i < nrules; i++) {

                Rule rule = rules[i];

                RuleStats stats = rule.apply( justify, focusStore, database, buffer );
                
                closureStats.add(stats);
                
                if (DEBUG || true) {

                    log.debug("round# " + round + ":" + stats);
                    
                }
                
                closureStats.nentailments += stats.numComputed;
                
                closureStats.elapsed += stats.elapsed;
                
            }

            /*
             * Flush the statements in the buffer
             * 
             * @todo When should we flush the buffer? In between each pass in
             * apply, after apply, or after the round? Each of these is
             * "correct" but there may be performance tradeoffs. Deferring the
             * flush can increase batch size and index write performance. If any
             * rule generates an entailment, then we are going to do another
             * round anyway so maybe it is best to defer to the end of the
             * round?
             * 
             * FIXME each round can use a historical read from the timestamp
             * associated with the commit point of the prior round. in an
             * extended transaction model those could be "save points" such
             * that the total result was either committed or aborted.  If a
             * transaction is used, then the closure actually takes place within
             * the transaction so the commit for the tx is always atomic and the
             * rounds in which we compute the closure are "committed" against the
             * tx's write set (either for the focus store or for the database if
             * we are doing database at once closure).
             */
            buffer.flush();

            final long numEntailmentsAfter = fixPointStore.rangeCount();
            
            if ( numEntailmentsBefore == numEntailmentsAfter ) {
                
                // This is the fixed point.
                break;
                
            }

            if(INFO) {

                log.info("round #"+round+"\n"+closureStats.toString());
                
            }

            round++;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        final long lastStatementCount = fixPointStore.rangeCount();

        if (INFO) {

            log.info("\n"+closureStats.toString());

            final long inferenceCount = lastStatementCount - firstStatementCount;
            
            // names of the rules that we ran.
            final String names; 
            {
            
                StringBuilder sb = new StringBuilder();
                
                sb.append("[");
                
                for(int i=0; i<rules.length; i++) {
                    
                    if(i>0) sb.append(",");

                    sb.append(rules[i].getName());
                    
                }

                sb.append("]");

                names = sb.toString();
                
            }
            
            log.info("\nComputed closure of "+rules.length+" rules in "
                            + (round+1) + " rounds and "
                            + elapsed
                            + "ms yeilding "
                            + lastStatementCount
                            + " statements total, "
                            + (inferenceCount)
                            + " inferences"
                            + ", entailmentsPerSec="
                            + (elapsed == 0 ? "N/A" : ""
                            + ((long) (inferenceCount * 1000d) / elapsed)));

            log.info("Rules: "+names);

//            Collection<RuleStats> ruleStats = closureStats.getRuleStats();
//            
//            for (RuleStats tmp : ruleStats) {
//                
//                log.info(tmp.toString());
//                
//            }
                        
        }

        return closureStats;

    }

}
