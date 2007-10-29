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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;

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
 * @todo the rule is entirely expressed in terms of a sequence of conjunctive
 *       predicates. in order to be more general, the expression should be in
 *       terms of a boolean AND operator, a triple pattern, and rewrites for
 *       evaluation into a variety of specialized JOIN operations (self-join,
 *       distinct term scan, and nested subquery). There is also the odd case
 *       for {@link AbstractRuleFastClosure_3_5_6_7_9} where the rule accepts a
 *       set of term identifers as on of its inputs.
 */
abstract public class Rule {

    final public Logger log = Logger.getLogger(Rule.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The persistent database.
     */
    final protected AbstractTripleStore db;
    
//    /**
//     * The inference engine.
//     */
//    final protected InferenceEngine inf;
    
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

//    /**
//     * When true the rule must justify the entailments to
//     * {@link SPOBuffer#add(com.bigdata.rdf.spo.SPO, com.bigdata.rdf.spo.Justification)}
//     */
//    final protected boolean justify;
    
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
     * This is determined when the rule is constructed.
     * 
     * FIXME The evaluation order MUST be tweaked when evaluating a rule against
     * (new+db) since any triple pattern is generally more selective in (new)
     * rather than in the (db), but it does depend on the actual range counts
     * for the actual data. {@link #clearDownStreamBindings(int)} also has a
     * dependency on this data.
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
     * Statistics for the rule evaluation.
     */
    final public RuleStats stats;
    
    /**
     * Emits an {@link SPO} entailment based on the current variable bindings
     * for the {@link #head}, and its {@link Justification} iff required, to
     * the {@link SPOBuffer}.
     * 
     * @param buffer
     */
    protected void emit(boolean justify, SPOBuffer buffer) {
        
        SPO stmt = new SPO(get(head.s),get(head.p),get(head.o),StatementEnum.Inferred);
        
        Justification jst = null;
        
        if(justify) {
            
            jst = new Justification(this, stmt, bindings.clone());
            
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
     * The 64-bit long integer that represents an unassigned term identifier
     */
    static final protected transient long NULL = ITripleStore.NULL;
    
    /**
     * The arity of a "triple" pattern.
     */
    static final protected transient int N = ITripleStore.N;

    /**
     * Externalizes the rule.
     */
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getName());
        
        sb.append(" : ");
        
        for(int i=0; i<body.length; i++) {
            
//            // Note: show in the evaluation order.
//            sb.append(body[order[i]].toString(db));
            sb.append(body[i].toString(db));

            // displays the evaluation order as an index on the predicate.
            sb.append("[" + order[i] + "]");

            if(i+1<body.length) {
                
                sb.append(", ");
                
            }
            
        }

        sb.append(" -> ");
        
        sb.append(head.toString(db));
        
        return sb.toString();
        
    }

    /**
     * By default the simple name of the class.
     */
    public String getName() {

        return getClass().getSimpleName();

    }
    
    /**
     * Externalizes the rule.
     * 
     * @param asBound
     *            When true, the current variable bindings will be displayed.
     */
    public String toString(boolean asBound) {

        if (!asBound) return toString();
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append(getName());
        
        sb.append(" : ");
        
        // write out bindings for the tail.
        
        for(int i=0; i<body.length; i++) {
            
//            // Note: show in the evaluation order.
//            Pred pred = body[order[i]];
            Pred pred = body[i];
            
            sb.append("(");
            
            sb.append( pred.s.isVar()?db.toString(bindings[i*N+0]):pred.s.toString(db) );
            
            sb.append(", ");

            sb.append( pred.p.isVar()?db.toString(bindings[i*N+1]):pred.p.toString(db) );
            
            sb.append(", ");
            
            sb.append( pred.o.isVar()?db.toString(bindings[i*N+2]):pred.o.toString(db) );
            
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

            sb.append(pred.s.isVar() ? db.toString(get((Var)pred.s)) : pred.s
                    .toString(db));

            sb.append(", ");

            sb.append(pred.p.isVar() ? db.toString(get((Var)pred.p)) : pred.p
                    .toString(db));

            sb.append(", ");

            sb.append(pred.o.isVar() ? db.toString(get((Var)pred.o)) : pred.o
                    .toString(db));

            sb.append(")");

            if (pred.magic) {

                sb.append("[magic]");

            }
            
        }
        
        return sb.toString();

    }
    
    /**
     * Canonicalizing map for {@link Var}s.
     * 
     * @todo I do not imagine that a lot of variables will be declared, but if
     *       there are then make this a weak value map and use an LRU to keep
     *       recently used variables around?
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
    
    public Rule(AbstractTripleStore db, Pred head, Pred[] body) {

        assert db != null;
        
        assert head != null;

        // the database.
        this.db = db;

        // the head of the rule.
        this.head = head;

        // the predicate declarations for the body.
        this.body = body;
        
        // collects statistics on the rule.
        this.stats = new RuleStats();
        
        // Allocate bindings for the tail.
        this.bindings = new long[body.length*N];

        // Allocate access path cache.
        this.accessPath = new IAccessPath[body.length];

        // The evaluation order.
        this.order = computeEvaluationOrder();

        // The 1st dependency map for each variable in the tail.
        this.depends = computeVariableDependencyMap();

        // initialize the bindings from the predicate declarations.
        resetBindings();

    }
    
    /**
     * The evaluation {@link #order}[] is determined by analysis of the
     * propagation of bindings. The most selective predicate is choosen first
     * and "fake" bindings are propagated to the other predicates in the tail.
     * This process is repeated until all variables are bound and an evaluation
     * order has been determined. The evaluation order is written into the
     * {@link #order}[]. The indices in that array are correlated 1:1 with the
     * predicates in {@link #body}.
     * 
     * @return The evaluation {@link #order}[].
     * 
     * @todo If we only considered the #of unbound variables then this could
     *       computed when the rule is instantiated since the range counts
     *       against the data are not being considered. However, if we do that
     *       then we will make the wrong choices when considering (new+db) since
     *       new will generally have far fewer statements for any triple
     *       pattern, even when nothing is bound.
     *       <p>
     *       When we consider (new+db) during the evaluation of a rule, there is
     *       always one predicate that is evaluated against new while the other
     *       predicates are evaluated against db. Therefore we need to "tweak"
     *       the statically determined order by explicitly considering whether
     *       the predicate to be evaluated against (new) is more selective in
     *       the data than the predicate that is first in the evaluate
     *       {@link #order}[]. If it is (and it generally will be) then we swap
     *       the indices of the two predicates in a local copy of {@link #order}[].
     *       <p>
     *       Note that {@link #depends} MUST also be updated for this case.
     *       <p>
     *       Note that both {@link #order} and {@link #depends} MUST be restored
     *       for normal evaluation, so maybe this should be done when the rule
     *       is to be evaluated.
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
     * 
     * FIXME This should be automatically invoked by whatever logic is used
     * to execute rules since it is too risky to leave it up to the person
     * writing the rule to make sure that they are invoking this at the top
     * of their rule.
     */
    void resetBindings() {
        
        for (int i = 0; i < body.length; i++) {

            Pred pred = body[i];

            accessPath[i] = null;
            
            for (int j = 0; j < ITripleStore.N; j++) {

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
            
            for(int j=0; j<ITripleStore.N; j++) {

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
        
        for(int j=0; j<ITripleStore.N; j++) {

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
//        clearDownstreamBindings(index + 1);
        
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
     *            
     * FIXME If the evaluation order is being modified for a (new+db) scenario
     * then the order[] MUST be modified as well for this method to work
     * correctly!!!
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
     * Return the {@link IAccessPath} that would be used to read from the
     * selected tail {@link Pred}.
     * <p>
     * Note: a cache is maintained by the rule for the access paths. If the
     * cache does not have an entry for the desired access path the one is
     * obtained and placed into the cache before being returned to the caller.
     * The cache is invalidated by {@link #resetBindings()} and (on a selective
     * basis) by {@link #bind(int, SPO)} and
     * {@link #set(com.bigdata.rdf.inf.Rule.Var, long)}.
     * 
     * @param index
     *            The index into {@link #body}.
     * 
     * @return The {@link IAccessPath}.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     * 
     * @todo modify to accept the view (new or new+old) for the access path (it
     *       needs to be a two-dimensional array).
     */
    public IAccessPath getAccessPath(int index) {
        
        if(accessPath[index]==null) {
        
            // based on the current bindings.
            
            accessPath[index] = db.getAccessPath(//
                    bindings[index*N+0],//
                    bindings[index*N+1],//
                    bindings[index*N+2]//
                    );
            
        }
        
        return accessPath[index];
        
    }

    /**
     * The access path corresponding to the body[] predicate that is more
     * selective given the data in the store and the current variable bindings.
     * 
     * @return The index of the {@link Pred} in {@link #body} that is the most
     *         selective -or- <code>-1</code> iff no access path would read
     *         any data.
     * 
     * @todo modify to accept the view (new or new+old) for the access path
     */
    public int getMostSelectiveAccessPathByRangeCount() {

        int index = -1;

        int minRangeCount = Integer.MAX_VALUE;

        for (int i = 0; i < body.length; i++) {

            // skip over fully bound predicates.
            if( isFullyBound(i) ) continue;
            
            int rangeCount = getAccessPath(i).rangeCount();

            if (rangeCount < minRangeCount) {

                minRangeCount = rangeCount;

                index = i;

            }

        }

        return index;

    }

    /**
     * Apply the rule, creating entailments that are inserted into a
     * {@link TempTripleStore}.
     * <p>
     * Note: the {@link BTree} class is NOT safe for concurrent modification
     * under traversal so implementations of this method need to buffer the
     * statements that they will insert. See {@link SPOBuffer}
     * 
     * @param stats
     *            Returns statistics on the rule application as a side effect.
     * @param buffer
     *            Used to buffer entailments so that we can perform batch btree
     *            operations. Entailments are batch inserted into the backing
     *            store (for the buffer) when the buffer overflows.
     *            <p>
     *            Note: In general, a single buffer object is reused by a series
     *            of rules. When the buffer overflows, entailments are
     *            transfered enmass into the backing store.
     * 
     * @return The statistics object.
     * 
     * @todo parameterize with the view (new, new+db, or db). new and new+db are
     *       used by truth maintenance. db is used for query.
     * 
     * @todo parameterize the behavior on overflow. for query, results need to
     *       be inflated (into the Sesame model) and delivered back to the SAIL.
     * 
     * @todo make the RuleStats an instance variable. They are reset on the rule
     *       before it is evaluated. Provide for cumulative rule stats across
     *       invocations of the rules as they go to fixed point in the context
     *       of TM or query evaluation. Rule statistics can also be cumulated
     *       across the life span of the executable or the database or simply
     *       logged periodically for later inspection.
     */
    abstract public RuleStats apply( boolean justify, final SPOBuffer buffer );
    
    /**
     * Evaluate a rule.
     * <p>
     * The basic program handles an N predicate conjunction. The predicates are
     * evaluated in an order which is dynamically determined based on the
     * selectivitity of the predicates having remaining unbound variables.
     * <p>
     * The order is determined in advance by analysis of the propagation of
     * bindings. The most selective predicate is choosen first and "fake"
     * bindings are propagated to the other predicates in the tail. This process
     * is repeated until all variables are bound and an evaluation order has
     * been determined. The evaluation order is written into an order[] whose
     * indices are correlated 1:1 with the predicates in the tail.
     * <p>
     * Evaluation nests subqueries based on the order[]. Nested subqueries are
     * issued iff the bindings for the subquery are distinct from those used in
     * the prior subquery.
     * 
     * @param justify
     *            When true, {@link Justification}s for entailments will be
     *            written on the <i>buffer</i>.
     * @param buffer
     *            The entailments will be written on the buffer.
     * @param focus
     *            A secondary data set used to support truth maintenance
     *            (optional). When given, the index of a focus predicate MUST be
     *            specified. The identified predicate will read from the "focus"
     *            data set while the remaining predicates will read from a fused
     *            view of the "focus" and the {@link #db}.
     * @param focusPred
     *            The index of the predicate in the {@link #body} that will read
     *            from the <i>focus</i>. This is ignore iff <i>focus</i> is
     *            <code>null</code>.
     * 
     * @todo behavior on overflow of the buffer must be caller determined
     *       (insert, remove, query).
     * 
     * @todo special evaluation for a tail consisting of a single predicate
     *       where the head has only one binding. For this case always do a
     *       distinct term scan for whatever variable is required by the head.
     *       <p>
     *       This can be generalized: any time we require only one binding from
     *       a triple pattern we can do a distinct term scan rather than the
     *       standard {@link ISPOIterator}. As the other variables are unused
     *       they can remain unbound or be set to an arbitrary non-{@value #NULL}
     *       value so as to appear as if they were bound.
     * 
     * @todo Note that we must evaluate a predicate that is a constant in order
     *       to determine whether or not the predicate can be matched in the
     *       appropriate data (new, new+db, db).
     */
    public RuleStats apply2( final boolean justify, final SPOBuffer buffer ) {
        
        resetBindings();
        
        assert checkBindings();

        return stats;
        
    }

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
     * @param stats
     *            Statistics on each rule application will be appended to this
     *            list.
     * @param tmp
     *            An {@link AbstractTripleStore} containing data to be added to
     *            (or removed from) the {@link #db}.
     * @param buffer
     *            The rules will write the entailments (and optionally the
     *            justifications) on the buffer. The caller can read the
     * 
     * @return An iterator that may be used to read union of the entailments
     *         licensed by each pass over the rule.
     * 
     * @todo Whether or not to compute the justifications is an apply() time
     *       parameter, not a constant fixed when the rule is created. We only
     *       need the justifications when we are adding statements to the
     *       database. When we are removing statements they should not be
     *       computed.
     *       <p>
     *       In order to get out the justifications the caller should just pass
     *       in a JustificationBuffer.
     * 
     * @todo Make the {@link SPOBuffer} thread-safe so that the N passes may be
     *       concurrent and they all write onto the same buffer, hence their
     *       union is automatically visible in the iterator wrapping that
     *       buffer.
     * 
     * @todo In order to use a reader/write (or pipe) model we need special
     *       {@link ISPOIterator} (and IJustificationIterator) implementations
     *       that wrap the appropriate buffer, e.g.,
     * 
     * <pre>
     *       SPOBuffer buffer = new SPOBuffer(...);
     *       
     *       ISPOIterator itr = new SPOPipeIterator( buffer );
     *       
     *       rule.apply(..., buffer );
     *       
     *       while(itr.hasNext()) {
     *       
     *        SPO[] stmts = itr.nextChunk();
     *       
     *       }
     *       
     * </pre>
     * 
     * before calling apply. The caller then has a handle on the object from
     * which they can read the entailments and do whatever they like with them.
     * <P>
     * Note: The SPOBuffer would require a means to handshake with the iterator
     * so that it could signal when no more data will be made available to the
     * iterator.
     * <p>
     * For example, you can use
     * {@link IRawTripleStore#addStatements(ISPOIterator, com.bigdata.rdf.spo.ISPOFilter)}
     * if you want to jam the results into a database. If you just want to scan
     * the results, then you can do that directly using the iterator.
     * <p>
     * Normally you would want to consume the iterator in chunks, sorting each
     * chunk into the natural order for some index operation and then doing that
     * operation on each chunk in turn.
     * 
     * @todo We can in fact run the variations of the rule in parallel using an
     *       {@link ExecutorService}. The {@link InferenceEngine} should
     *       declare this service. The service will be used for both map
     *       parallelism and for parallelism of subqueries within rules.
     */
    public ISPOIterator apply( List<RuleStats> stats, AbstractTripleStore tmp, SPOBuffer buffer ) {

        throw new UnsupportedOperationException();
        
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

    /**
     * Statistics about what the Rule did.
     * 
     * @author mikep
     */
    public class RuleStats {

        private RuleStats() {
            
            nstmts = new int[body.length];

            nsubqueries = new int[body.length];
            
        }

        /**
         * The #of rounds that have been executed for this rule.
         */
        public int nrounds;
        
        /**
         * The #of statement considered for the each predicate in the body of the
         * rule (in the order in which they were declared, not the order in which
         * they were evaluated).
         */
        public int[] nstmts;

        /**
         * The #of subqueries examined for each predicate in the rule. The indices
         * are correlated 1:1 with the order in which the predicates were declared.
         * While there are N indices for a rule with N predicates, we only evaluate
         * a subquery for N-1 predicates so at least one index will always be
         * zero(0).
         */
        public int[] nsubqueries;
        
        /**
         * #of entailments computed by the rule (does not consider whether or not
         * the entailments were pre-existing in the database nor whether or not the
         * entailments filtered out such that they did not enter the database).
         */
        public int numComputed;
        
        /**
         * Time to compute the entailments (ms).
         * 
         * @todo convert this to nano seconds?
         */
        public long elapsed;

        /**
         * The #of statements considered by the rule (total over {@link #nstmts}).
         */
        public int getStatementCount() {
            
            int n = 0;
            
            for(int i=0; i<nstmts.length; i++) {
                
                n += nstmts[i];
                
            }
            
            return n;
            
        }
        
        public String getEntailmentsPerMillisecond() {

            return (elapsed == 0 ? "N/A" : "" + numComputed / elapsed);
            
        }
        
        public String toString() {

            int n = getStatementCount();
            
            String stmtsPerSec = (n == 0 ? "N/A" : (elapsed == 0L ? "0" : ""
                    + ((long) (((double) n) / ((double) elapsed) * 1000d))));

            return getName()//
                 + ", #rounds="+nrounds//
                 + ", #stmts=" + Arrays.toString(nstmts) //
                 + ", #subqueries="+Arrays.toString(nsubqueries)//
                 + ", #computed=" + numComputed//
                 + ", elapsed=" + elapsed//
                 + ", stmts/sec="+stmtsPerSec//
                 ;

        }

        /**
         * Resets all of the counters.
         */
        public void reset() {
            
            for(int i=0; i<nstmts.length; i++) {

                nstmts[i] = nsubqueries[i] = 0;

            }

            nrounds = 0;
            
            numComputed = 0;
            
            elapsed = 0L;
            
        }

        /**
         * Aggregates statistics.
         * 
         * @param o Statistics for another rule.
         */
        public void add(RuleStats o) {
            
            for(int i=0; i<nstmts.length; i++) {

                nstmts[i] += o.nstmts[i];
                
                nsubqueries[i] += o.nsubqueries[i];

            }
            
            numComputed += o.numComputed;
        
            elapsed += o.elapsed;
        
        }
        
    }

}
