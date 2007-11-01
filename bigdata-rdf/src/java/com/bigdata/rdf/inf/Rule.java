/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AccessPathFusedView;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * A rule.
 * <p>
 * Note: You MUST {@link #resetBindings()} each time you execute a rule. If the
 * variable binding patterns are not restored (overwriting whatever constants
 * were last in those positions) then the wrong access path will be selected and
 * you will NOT get the expected entailments.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The {@link Rule} is expressed as a sequence of conjunctive predicates.
 *       this is clearly sufficient for RDFS entailments and probably can be
 *       made to serve for a magic sets variant (as an alternative to backward
 *       chaining during TM).
 *       <p>
 *       If there is a requirement to be more general, then this should be
 *       refactored in terms of a boolean AND operator, a triple pattern, and
 *       rewrites for evaluation into a variety of specialized JOIN operations
 *       (self-join, distinct term scan, and nested subquery).
 *       <p>
 *       There is also the odd case for
 *       {@link AbstractRuleFastClosure_3_5_6_7_9} where the rule accepts a set
 *       of term identifers as on of its inputs.
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
     * The 64-bit long integer that represents an unassigned term identifier
     */
    static final protected transient long NULL = IRawTripleStore.NULL;
    
    /**
     * The arity of a "triple" pattern.
     */
    static final protected transient int N = IRawTripleStore.N;

    /**
     * Canonicalizing map for {@link Var}s.
     */
    static private final Map<String,Var> vars = new ConcurrentHashMap<String,Var>();
    
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
     */
    static protected Var var(String name) {
    
        if (name == null)
            throw new IllegalArgumentException();
        
        if (name.length() == 0)
            throw new IllegalArgumentException();
        
        Var var = vars.get(name);
        
        if(var == null) {
            
            synchronized(vars) {

                // Only one thread gets to create the variable for that name.
                
                var = new Var(name);
            
                vars.put(name, var);
                
            }
            
        }
        
        return var;
        
    }
    
    /**
     * The head of the rule.
     */
    final protected Pred head;

    /**
     * The body of the rule -or- <code>null</code> if the body of the rule
     * is empty.
     */
    final protected Pred[] body;
    
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

        return getClass().getSimpleName();

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

    public Rule(AbstractTripleStore db, Pred head, Pred[] body) {

        assert db != null;
        
        assert head != null;

        // the head of the rule.
        this.head = head;

        // the predicate declarations for the body.
        this.body = body;
        
    }

    /**
     * Variant when not using a focusStore.
     * @param justify
     * @param database
     * @param buffer
     * @return
     */
    public State newState(boolean justify,
                AbstractTripleStore database,
                SPOBuffer buffer) {
        
        return newState(0/* index */,justify,null/* focusStore */,database,buffer);
        
    }
    
    public State newState(int index, boolean justify,
                AbstractTripleStore focusStore,
                AbstractTripleStore database,
                SPOBuffer buffer) {
        
        return new State(index, justify, focusStore, database, buffer);
        
    }
    
    /**
     * Return the variables in common for two {@link Pred}s.
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
        
        Set<Var> vars = new HashSet<Var>();
        
        for(int i=0; i<N; i++ ) {
            
            VarOrId avar = body[index1].get(i);
            
            if(avar.isConstant()) continue;
                
            for (int j = 0; j < N; j++) {

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
     *      AbstractTripleStore, SPOBuffer)
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
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
         * The optional focus store.
         */
        public final AbstractTripleStore focusStore;
        
        /**
         * The persistent database.
         */
        public final AbstractTripleStore database;
        
        /**
         * Used to buffer entailments so that we can perform efficient ordered
         * writes on the statement indices.
         */
        public final SPOBuffer buffer;
        
        /**
         * Instance array of bindings for each term in the tail in the order in
         * which those terms were declared to the ctor. The constants are copied
         * into the bindings from the {@link #head} and then {@link #body} when the
         * rule is constructed and the other bindings are set to {@link #NULL}. The
         * {@link #head} is always at index ZERO(0). The {@link #body} begins at
         * index ONE (1).
         * <p>
         * An attempt to overwrite a constant can be detected using
         * {@link #checkBindings()}.
         */
        final private long[] bindings;

        /**
         * The {@link IAccessPath} corresponding to each {@link Pred} in the
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
        private State(int focusIndex, boolean justify,
                AbstractTripleStore focusStore,
                AbstractTripleStore database,
                SPOBuffer buffer) {

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
            
            // collects statistics on the rule.
            this.stats = new RuleStats(Rule.this);
            
            // Allocate bindings for the tail.
            this.bindings = new long[body.length*N];

            // Allocate access path cache.
            this.accessPath = new IAccessPath[body.length];

            // The evaluation order.
            this.order = computeEvaluationOrder();
            
            if(focusStore!=null) {
                
                /*
                 * Always evaluate the predicate that will read from the
                 * focusStore 1st.
                 * 
                 * Note: This presumes that any predicate reading from the
                 * [focusStore] will be more selective than any predicate
                 * reading from the fused view [focusStore + database]. This
                 * should be true in all except a few edge cases -- e.g., when
                 * the database is empty.
                 */

                int tmp = order[0];
                
                order[0] = order[focusIndex];
                
                order[focusIndex] = tmp;
                
            }
            

            // The 1st dependency map for each variable in the tail.
            this.depends = computeVariableDependencyMap();

            // initialize the bindings from the predicate declarations.
            resetBindings();

        }
        
        /**
         * Emits an {@link SPO} entailment based on the current variable bindings
         * for the {@link #head}, and its {@link Justification} iff required, to
         * the {@link SPOBuffer}.
         */
        protected void emit() {
            
            SPO stmt = new SPO(get(head.s),get(head.p),get(head.o),StatementEnum.Inferred);
            
            Justification jst = null;
            
            if(justify) {
                
                jst = new Justification(Rule.this, stmt, bindings.clone());
                
            }
            
            if(buffer.add(stmt, jst)) {

                /*
                 * @todo a counter here for the #of entailments that were
                 * accepted by the filter.
                 */
                
            }

            // #of entailments computed.
            stats.numComputed++;

        }
        
        /**
         * Externalizes the rule using the database to resolve constants.
         * 
         * @param db
         *            The database.
         */
        public String toString() {

            return toString(false);
            
        }
        
        /**
         * Externalizes the rule using the database to resolve constants.
         * 
         * @param db
         *            The database.
         * @param asBound
         *            When true, the current variable bindings will be displayed.
         *            Otherwise, the names of variables will be displayed rather
         *            than their bindings.
         */
        public String toString(boolean asBound) {

            final StringBuilder sb = new StringBuilder();
            
            sb.append(getName());
            
            sb.append(" : ");
            
            // write out bindings for the tail.
            
            for(int i=0; i<body.length; i++) {
                
//                // Note: show in the evaluation order.
//                Pred pred = body[order[i]];
                Pred pred = body[i];
                
                sb.append("(");
                
                sb.append( pred.s.isVar()&&asBound?database.toString(bindings[i*N+0]):pred.s.toString(database) );
                
                sb.append(", ");

                sb.append( pred.p.isVar()&&asBound?database.toString(bindings[i*N+1]):pred.p.toString(database) );
                
                sb.append(", ");
                
                sb.append( pred.o.isVar()&&asBound?database.toString(bindings[i*N+2]):pred.o.toString(database) );
                
                sb.append(")");
                
                if (!asBound) {

                    // displays the evaluation order as an index on the predicate.

                    sb.append("[" + order[i] + "]");
                    
                }
                
                if(pred.magic) {
                    
                    sb.append("[magic]");
                    
                }
                
                if(i+1<body.length) {
                    
                    sb.append(", ");
                    
                }
                
            }

            sb.append(" -> ");
            
            // write out bindings for the head.
            {
                
                Pred pred = head;

                sb.append("(");

                sb.append(pred.s.isVar()&&asBound ? database.toString(get((Var)pred.s)) : pred.s
                        .toString(database));

                sb.append(", ");

                sb.append(pred.p.isVar()&&asBound ? database.toString(get((Var)pred.p)) : pred.p
                        .toString(database));

                sb.append(", ");

                sb.append(pred.o.isVar()&&asBound ? database.toString(get((Var)pred.o)) : pred.o
                        .toString(database));

                sb.append(")");

                if (pred.magic) {

                    sb.append("[magic]");

                }
                
            }
            
            return sb.toString();

        }
                
        /**
         * The evaluation {@link #order}[] is determined by analysis of the
         * propagation of bindings. The most selective predicate is choosen
         * first and "fake" bindings are propagated to the other predicates in
         * the tail. This process is repeated until all variables are bound and
         * an evaluation order has been determined. The evaluation order is
         * written into the {@link #order}[]. The indices in that array are
         * correlated 1:1 with the predicates in {@link #body}.
         * 
         * @return The evaluation {@link #order}[].
         * 
         * @todo If we only considered the #of unbound variables then this could
         *       computed when the rule is instantiated since the range counts
         *       against the data are not being considered. However, if we do
         *       that then we will make the wrong choices when considering
         *       (new+db) since new will generally have far fewer statements for
         *       any triple pattern, even when nothing is bound.
         *       <p>
         *       When we consider (new+db) during the evaluation of a rule,
         *       there is always one predicate that is evaluated against new
         *       while the other predicates are evaluated against database.
         *       Therefore we need to "tweak" the statically determined order by
         *       explicitly considering whether the predicate to be evaluated
         *       against (new) is more selective in the data than the predicate
         *       that is first in the evaluate {@link #order}[]. If it is (and
         *       it generally will be) then we swap the indices of the two
         *       predicates in a local copy of {@link #order}[].
         *       <p>
         *       Note that {@link #depends} MUST also be updated for this case.
         *       <p>
         *       Note that both {@link #order} and {@link #depends} MUST be
         *       restored for normal evaluation, so maybe this should be done
         *       when the rule is to be evaluated.
         */
        private int[] computeEvaluationOrder() {
         
            resetBindings();

            int[] order = new int[body.length];
            boolean[] used = new boolean[body.length];
            
            // clear array.  -1 is used to detect logic errors.
            for( int i=0; i<order.length; i++ ) order[i] = -1;
            
            // used to propagate bindings.
            SPO tmp = new SPO(-1,-1,-1,StatementEnum.Axiom);
            
            for(int i=0; i<body.length; i++) {

                int minVarCount = Integer.MAX_VALUE;
                int index = -1;
                
                for( int j=0; j<body.length; j++) {
                    
                    if(used[j]) continue; // already in the evaluation order. 
                    
                    int varCount = body[j].getVariableCount();
                    
                    if(varCount<minVarCount) {
                        
                        minVarCount = varCount;
                        
                        index = j;
                        
                    }
                    
                }

                if(index==-1) throw new AssertionError();
                
                if (used[index])
                    throw new AssertionError("Attempting to reuse predicate: index="+i+"\n"+this);
                
                order[i] = index;
                
                used[index] = true;
                
                // propagate bindings.
                bind(index,tmp);
                
            }
            
            assert checkBindings();
           
            log.info("Evaluation order: "+Arrays.toString(order));
            
            return order;
            
        }
        
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
            
            Map<Var,Integer> depends = new HashMap<Var,Integer>();
            
            for(int i=0; i<body.length; i++) {
                
                Pred pred = body[order[i]];
                
                if(pred.s.isVar()) {
                    
                    Var var = (Var)pred.s;
                    
                    if(!depends.containsKey(var)) {
                        
                        depends.put(var, i);
                        
                    }
                    
                }
                
                if(pred.p.isVar()) {
                    
                    Var var = (Var)pred.p;
                    
                    if(!depends.containsKey(var)) {
                        
                        depends.put(var, i);
                        
                    }
                    
                }
                
                if(pred.o.isVar()) {
                    
                    Var var = (Var)pred.o;
                    
                    if(!depends.containsKey(var)) {
                        
                        depends.put(var, i);
                        
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
            
            for (int i = 0; i < body.length; i++) {

                Pred pred = body[i];

                accessPath[i] = null;
                
                for (int j = 0; j < IRawTripleStore.N; j++) {

                    VarOrId binding = pred.get(j);

                    // Note: Will be NULL IFF binding is a variable.
                    bindings[i * N + j] = binding.id;

                }

            }
                
        }
        
        /**
         * Asserts that the non-variable bindings have their constant values by
         * comparing them with the values in {@link #body}.  Use as follows:
         * <pre>
         * assert checkBindings();
         * </pre>
         * 
         * @return true iff the bindings are Ok.
         */
        public boolean checkBindings() {
            
            for(int i=0; i<body.length; i++) {

                final Pred pred = body[i];
                
                for(int j=0; j<IRawTripleStore.N; j++) {

                    // you are supposed to overwrite the variables, but not the
                    // constants.
                    if (pred.get(j).isConstant()) {
                        
                        // verify constant not overwritten.
                        if(bindings[i * N + j] != pred.get(j).id) {
                            
                            return false;
                            
                        }
                        
                    }
                    
                }
                
            }

            return true;
            
        }

        /**
         * Return true iff the selected predicate is fully bound.
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
            
            Pred pred = body[index];
            
            for(int j=0; j<IRawTripleStore.N; j++) {

                // check any variables.
                if (pred.get(j).isVar()) {
                    
                    // if a variable is unbound then return false.
                    if(bindings[index * N + j] == NULL) {
                        
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
         * @see #set(Var, long)
         * @see #bind(int, SPO)
         */
        public long get(VarOrId var) {
            
            if (var == null)
                throw new NullPointerException();

            if (var.isConstant()) {

                return var.id;
                
            }
            
            /*
             * scan the body for the variable.
             */
            
            for(int i=0; i<body.length; i++) {
                
                int indexOf = body[i].indexOf(var);
                
                if(indexOf != -1) {
                    
                    final long id = bindings[i * N + indexOf];

                    // MAY be NULL.
                    return id;
                    
                }
                
            }
            
            throw new IllegalArgumentException("Not used: " + var + ", rule="
                    + this);
                    
        }
        
        /**
         * Binds the variable. The variable will be bound at each position in which
         * it occurs in the rule.
         * <p>
         * Note: The cached {@link IAccessPath} for a predicate is cleared if a
         * variable binding is set on that predicate.
         * 
         * @param var
         *            A variable that appears in that predicate.
         * @param id
         *            The value to be bound on the variable.
         * 
         * @throws NullPointerException
         *             if the variable is null.
         * @throws IndexOutOfBoundsException
         *             if the predicate index is out of range.
         * @throws IllegalArgumentException
         *             if the variable does not appear in the rule.
         * 
         * @see #bind(int, SPO)
         */
        public void set(Var var, long id) {
          
            if (var == null) {

                throw new NullPointerException();
                
            }
            
            boolean exists = false;
            
            for(int i=0; i<body.length; i++) {
                
                Pred pred = body[i];
                
                if(pred.s == var) {

                    bindings[i * N + 0] = id;

                    accessPath[i] = null;
                    
                    exists = true;

                }

                if (pred.p == var) {

                    bindings[i * N + 1] = id;

                    accessPath[i] = null;
                    
                    exists = true;
                    
                }

                if(pred.o == var) {

                    bindings[i * N + 2] = id;

                    accessPath[i] = null;
                    
                    exists = true;
                    
                }

            }
            
            if(!exists) {
                
                throw new IllegalArgumentException(var.toString());
                
            }
            
        }

        /**
         * Instruct the rule to bind its variables by mapping the specified
         * statement onto the specified body predicate. For each position in the
         * predicate that is a variable (as opposed to a constant), the
         * corresponding value is copied from the statement and will become the new
         * binding for that variable in each position in which it occurs in the
         * rule.
         * 
         * @param index
         *            The index of the predicate in the body of the rule.
         * @param spo
         *            A statement materialized by the triple pattern that predicate.
         * 
         * @throws NullPointerException
         *             if the statement is <code>null</code>.
         * @throws IndexOutOfBoundsException
         *             if the index is out of bounds.
         * 
         * @see #clearDownstreamBindings(int index)
         */
        public void bind(int index, SPO spo) {
            
            // Note: if you are doing nested subqueries and you are not invoking
            // this method then you have a problem!
            //
            // @todo if this proves to be a stubling block for writing rules then
            // just move the invocation into this method as:
            //
//            clearDownstreamBindings(index + 1);
            
            Pred pred = body[index];
            
            if(pred.s.isVar()) {
                
                set((Var)pred.s,spo.s);
                
            }
            
            if(pred.p.isVar()) {
                
                set((Var)pred.p,spo.p);
                
            }
            
            if(pred.o.isVar()) {
                
                set((Var)pred.o,spo.o);
                
            }
            
        }
        
        /**
         * Clear downstream bindings in the evaluation {@link #order}[]. If a
         * variable in a downstream predicate was 1st bound by an upstream predicate
         * as identified by {@link #depends} then its value is NOT cleared.
         * <p>
         * Note: You MUST {@link #resetBindings()} before you evaluate a rule.
         * <p>
         * Note: You MUST {@link #clearDownstreamBindings(int)} bindings before you
         * (re-)evaluate a subquery. Failure to do so will leave non-{@link #NULL}
         * bindings in place which will cause {@link #getAccessPath(int)} to
         * identify the wrong access path, and hence select the wrong data.
         * 
         * @param index
         *            The index of the predicate whose values you intend to
         *            {@link #bind(int, SPO)}.
         */
        protected void clearDownstreamBindings(int index) {

            for(int i=index; i<body.length; i++) {
                
                Pred pred = body[order[index]];
                
                if(pred.s.isVar()) {
                    
                    Var var = (Var)pred.s;
                    
                    int j = depends.get(var);
                    
                    if (j >= index) {

                        set(var, NULL);

                    }
                    
                }
                
                if(pred.p.isVar()) {
                    
                    Var var = (Var)pred.p;
                    
                    int j = depends.get(var);
                    
                    if (j >= index) {

                        set(var, NULL);

                    }
                    
                }
                
                if(pred.o.isVar()) {
                    
                    Var var = (Var)pred.o;
                    
                    int j = depends.get(var);
                    
                    if (j >= index) {

                        set(var, NULL);

                    }
                    
                }
                            
            }
            
        }
        
        /**
         * Return the {@link IAccessPath} that would be used to read from the
         * selected tail {@link Pred}.
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
            
                // based on the current bindings.
                
                final long s = bindings[index * N + 0];
                final long p = bindings[index * N + 1];
                final long o = bindings[index * N + 2];

                if (focusStore == null) {
                 
                    accessPath = database.getAccessPath(s, p, o);

                } else {

                    if (index == focusIndex) {

                        accessPath = focusStore.getAccessPath(s, p, o);

                    } else {

                        /*
                         * Return a read-only access path for the fused view
                         * [focusStore + database].
                         */
                        
                        return new AccessPathFusedView(
                                focusStore.getAccessPath(s, p, o),
                                database.getAccessPath(s, p, o)
                                );

                    }

                }

                // update the cache.
                this.accessPath[index] = accessPath;
                
            }
            
            return accessPath;
            
        }

//        /**
//         * The index of the {@link Rule#body}[] predicate that is the most
//         * selective given the data and the current variable bindings.
//         * 
//         * @return The index of the {@link Pred} in {@link #body} that is the
//         *         most selective.
//         */
//        public int getMostSelectiveAccessPathByRangeCount() {
//
//            int index = -1;
//
//            int minRangeCount = Integer.MAX_VALUE;
//
//            for (int i = 0; i < body.length; i++) {
//
//                final int rangeCount;
//                
//                // skip over fully bound predicates.
//                if( isFullyBound(i) ) {
//                    
//                    rangeCount = 1;
//                    
//                } else {
//                
//                    rangeCount = getAccessPath(i).rangeCount();
//                    
//                }
//
//                if (rangeCount < minRangeCount) {
//
//                    minRangeCount = rangeCount;
//
//                    index = i;
//
//                }
//
//            }
//
//            return index;
//
//        }

    }
    
    /**
     * Apply the rule.
     * 
     * @param state
     *            The rule execution state.
     */
    abstract public void apply( State state );

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
     *       Make the {@link SPOBuffer} thread-safe so that the N passes may be
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
    public RuleStats apply(boolean justify, AbstractTripleStore focusStore,
            AbstractTripleStore database, SPOBuffer buffer) { 
    
        if (focusStore == null) {

            /*
             * Just close the database.
             */

            State state = newState(0/*focusIndex*/, justify, focusStore, database, buffer);

            apply(state);

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

            apply(state[i]);
            
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
            Rule[] rules, boolean justify, AbstractTripleStore focusStore,
            AbstractTripleStore database, SPOBuffer buffer) {
        
        final int nrules = rules.length;

        /*
         * We will fix point whichever store the buffer is writing on.
         */
        
        final AbstractTripleStore fixPointStore = buffer.store;
        
        final int firstStatementCount = fixPointStore.getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");

        int round = 0;

        while (true) {

            final int numEntailmentsBefore = fixPointStore.getStatementCount();
            
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
             */
            buffer.flush();

            final int numEntailmentsAfter = fixPointStore.getStatementCount();
            
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

        final int lastStatementCount = fixPointStore.getStatementCount();

        if (INFO) {

            log.info("\n"+closureStats.toString());

            final int inferenceCount = lastStatementCount - firstStatementCount;
            
            log.info("\nComputed closure of "+rules.length+" rules in "
                            + (round+1) + " rounds and "
                            + elapsed
                            + "ms yeilding "
                            + lastStatementCount
                            + " statements total, "
                            + (inferenceCount)
                            + " inferences"
                            + ", entailmentsPerSec="
                            + ((long) (((double) inferenceCount)
                                    / ((double) elapsed) * 1000d)));

        }

        return closureStats;

    }

    /**
     * A variable.
     * 
     * @see Rule#var(String)
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    final static public class Var extends VarOrId {

        public final String name;
        
        final public boolean isVar() {
            
            return true;
            
        }
        
        final public boolean isConstant() {
            
            return false;
            
        }
        
        /**
         * Private constructor - use {@link Rule#var(String)} to obtain an instance.
         * 
         * @param name
         */
        private Var(String name) {
            
            super(NULL);
            
            assert name != null;
            
            this.name = name;
            
        }

        public final boolean equals(VarOrId o) {
            
            if(this == o) return true;
            
            if(o instanceof Var) {
                
                return name.equals(((Var)o).name);
                
            }
            
            return false;
            
        }

        public final int hashCode() {
            
            return name.hashCode();
            
        }
        
        public String toString() {
            
            return name;
            
        }

        public String toString(AbstractTripleStore db) {
            
            return name;
            
        }

        public int compareTo(VarOrId arg0) {

            // order vars before ids
            if(arg0 instanceof Id) return -1;
            
            return name.compareTo(((Var)arg0).name);
            
        }
        
    }

}
