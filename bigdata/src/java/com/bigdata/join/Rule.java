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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Default impl.
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
public class Rule implements IRule {

    /**
     * 
     */
    private static final long serialVersionUID = -3834383670300306143L;

    final static protected Logger log = Logger.getLogger(Rule.class);

    /**
     * Singleton factory for {@link Var}s (delegates to {@link Var#var(String)}).
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
    final private IPredicate head;

    /**
     * The body of the rule -or- <code>null</code> if the body of the rule
     * is empty.
     */
    final private IPredicate[] tail;

    /**
     * Optional constraints on the bindings.
     */
    final private IConstraint[] constraints;
    
    /**
     * The set of distinct variables declared by the rule.
     */
    final private Set<IVariable> vars;

    /**
     * The #of distinct variables declared by the rule.
     */
    public int getVariableCount() {
        
        return vars.size();
        
    }

    /**
     * The variables declared by the rule in no particular order.
     */
    public Iterator<IVariable> getVariables() {
        
        return vars.iterator();
        
    }
    
    /**
     * The #of {@link IPredicate}s in the body (aka tail) of the rule.
     */
    public int getTailCount() {
        
        return tail.length;
        
    }
    
    public IPredicate getHead() {
        
        return head;
        
    }

    /**
     * Iterator visits the {@link IPredicate}s in the body (ala tail) of the
     * rule.
     */
    public Iterator<IPredicate> getTailPredicates() {
        
        return Arrays.asList(tail).iterator();
        
    }

    /**
     * Return the predicate at the given index from the tail of the rule.
     * 
     * @param index
     *            The index.
     *            
     * @return The predicate at that index.
     */
    public IPredicate getTailPredicate(int index) {
        
        return tail[index];
        
    }
    
    public int getConstraintCount() {
        
        return constraints == null ? 0 : constraints.length;
        
    }

    /**
     * The optional constraints.
     */
    public Iterator<IConstraint> getConstraints() {
        
        return Arrays.asList(constraints).iterator();
        
    }
    
    /**
     * By default the simple name of the class.
     */
    public String getName() {

        if (name == null) {

            return getClass().getSimpleName();
            
        } else {
            
            return name;
            
        }

    }
    
    /**
     * Externalizes the rule displaying variable names and constants.
     */
    public String toString() {
        
        return toString(null);
        
    }
    
    /**
     * Externalizes the rule displaying variable names, their bindings, and
     * constants.
     * 
     * @param bindingSet
     *            When non-<code>null</code>, the current variable bindings
     *            will be displayed. Otherwise, the names of variables will be
     *            displayed rather than their bindings.
     */
    public String toString(IBindingSet bindingSet) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getName());
        
        sb.append(" : ");
        
        // write out bindings for the tail.
        
        for (int i = 0; i < tail.length; i++) {

            sb.append(tail[i].toString(bindingSet));

            if (i + 1 < tail.length) {

                sb.append(", ");
                
            }
            
        }

        sb.append(" -> ");
        
        // write out bindings for the head.
        {
            
            sb.append(head.toString(bindingSet));

        }
        
        return sb.toString();

    }
            
//    public String toString(IBindingSet bindingSet) {
//
//        final StringBuilder sb = new StringBuilder();
//        
//        sb.append(getName());
//        
//        sb.append(" : ");
//        
//        for(int i=0; i<body.length; i++) {
//            
//            sb.append(body[i].toString());
//
//            if(i+1<body.length) {
//                
//                sb.append(", ");
//                
//            }
//            
//        }
//
//        sb.append(" -> ");
//        
//        sb.append(head.toString());
//        
//        return sb.toString();
//        
//    }

    /**
     * Rule ctor.
     * 
     * @param name
     *            A label for the rule (optional).
     * @param head
     *            The subset of bindings that are selected by the rule.
     * @param tail
     *            The tail (aka body) of the rule.
     * @param constraints
     *            An array of constaints on the legal states of the bindings
     *            materialized for the rule.
     * 
     * @throws IllegalArgumentException
     *             if the <i>head</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>tail</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if any element of the <i>tail</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if any element of the optional <i>constraints</i> is
     *             <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>tail</i> is empty.
     * @throws IllegalArgumentException
     *             if the <i>head</i> declares any variables that are not
     *             declared in the tail.
     */
    public Rule(String name, IPredicate head, IPredicate[] tail,
            IConstraint[] constraints) {

        this.name = name;

        if (head == null)
            throw new IllegalArgumentException();

        if (tail == null)
            throw new IllegalArgumentException();

        final Set<IVariable> vars = new HashSet<IVariable>();
        
        // the predicate declarations for the body.
        this.tail = tail;

        for (int i = 0; i < tail.length; i++) {

            final IPredicate pred = tail[i];
            
            if (pred == null)
                throw new IllegalArgumentException();
            
            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant t = pred.get(j);

                if (t.isVar()) {

                    vars.add((IVariable) t);

                }

            }

        }
        
        // the head of the rule - all variables must occur in the tail.
        this.head = head;
        {

            final int arity = head.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant t = head.get(j);

                if (t.isVar()) {

                    if (!vars.contains((IVariable) t)) {

                        throw new IllegalArgumentException(
                                "Variable not declared in the tail: " + t);
                        
                    }

                }

            }

        }

        // make the collection immutable.
        this.vars = Collections.unmodifiableSet(vars);

        // constraint(s) on the variable bindings (MAY be null).
        this.constraints = constraints;

        if (constraints != null) {

            for (int i = 0; i < constraints.length; i++) {
            
                assert constraints[i] != null;
            
            }
            
        }

    }

    /**
     * Specialize a rule - the name of the new rule will be derived from the
     * name of the old rule with an appended single quote to indicate that it is
     * a derived variant.
     * 
     * @param bindingSet
     *            Bindings for zero or more free variables in this rule. The
     *            rule will be rewritten such that the variable is replaced by
     *            the binding throughout the rule. An attempt to bind a variable
     *            not declared by the rule will be ignored.
     * @param constraints
     *            An array of additional constraints to be imposed on the rule
     *            (optional).
     * 
     * @return The specialized rule.
     * 
     * @throws IllegalArgumentException
     *             if <i>bindingSet</i> is <code>null</code>.
     */
    public IRule specialize(IBindingSet bindingSet, IConstraint[] constraints) {

        return specialize(getName() + "'", bindingSet, constraints);

    }
    
    /**
     * Specialize a rule by binding zero or more variables and adding zero or
     * more constraints.
     * 
     * @param bindingSet
     *            Bindings for zero or more free variables in this rule. The
     *            rule will be rewritten such that the variable is replaced by
     *            the binding throughout the rule. An attempt to bind a variable
     *            not declared by the rule will be ignored.
     * @param constraints
     *            An array of additional constraints to be imposed on the rule
     *            (optional).
     * 
     * @return The specialized rule.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if <i>bindingSet</i> is <code>null</code>.
     */
    public IRule specialize(String name, IBindingSet bindingSet,
            IConstraint[] constraints) {

        if (name == null)
            throw new IllegalArgumentException();

        if (bindingSet == null)
            throw new IllegalArgumentException();

        /*
         * Setup the new head and the body for the new rule by applying the
         * bindings.
         */

        final IPredicate newHead = head.asBound(bindingSet);

        final IPredicate[] newTail = bind(tail, bindingSet);

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
        
        final IRule newRule = new Rule(name, newHead, newTail, newConstraint);

        return newRule;

    }

    private IPredicate[] bind(IPredicate[] predicates, IBindingSet bindingSet) {
        
        final IPredicate[] tmp = new IPredicate[predicates.length];
        
        for(int i=0; i<predicates.length; i++) {
            
            tmp[i] = predicates[i].asBound(bindingSet);
            
        }
        
        return tmp;
        
    }
    
    /**
     * Return the variables in common for two {@link IPredicate}s.
     * 
     * @param index1
     *            The index of a predicate in the {@link #tail}.
     * 
     * @param index2
     *            The index of a different predicate in the {@link #tail}.
     * 
     * @return The variables in common -or- <code>null</code> iff there are no
     *         variables in common.
     * 
     * @throws IllegalArgumentException
     *             if the two predicate indices are the same.
     * @throws IndexOutOfBoundsException
     *             if either index is out of bounds.
     */
    public Set<IVariable> getSharedVars(int index1, int index2) {

        if (index1 == index2) {

            throw new IllegalArgumentException();
            
        }

        return getSharedVars(tail[index1], tail[index2]);
        
    }
    
    /**
     * Return the variables in common for two {@link IPredicate}s.
     * 
     * @param p1
     *            A predicate.
     * 
     * @param p2
     *            A different predicate.
     * 
     * @return The variables in common -or- <code>null</code> iff there are no
     *         variables in common.
     * 
     * @throws IllegalArgumentException
     *             if the two predicates are the same reference.
     */
    public static Set<IVariable> getSharedVars(IPredicate p1, IPredicate p2) {
        
        final Set<IVariable> vars = new HashSet<IVariable>();
        
        final int arity1 = p1.arity(); 

        final int arity2 = p2.arity();
        
        for(int i=0; i<arity1; i++ ) {
            
            final IVariableOrConstant avar = p1.get(i);
            
            if(avar.isConstant()) continue;
                
            for (int j = 0; j < arity2; j++) {

                if (p2.get(j) == avar) {
                    
                    vars.add((Var) avar);
                    
                }
                
            }

        }
        
        return vars;

    }

    /**
     * Return true iff the selected predicate is fully bound.
     * 
     * @param index
     *            The index of a predicate declared in the {@link #tail} of the
     *            rule.
     * @param bindingSet
     *            The variable bindings.
     * 
     * @return True iff it is fully bound (a mixture of constants and/or bound
     *         variables).
     * 
     * @throws IndexOutOfBoundsException
     *             if the <i>index</i> is out of bounds.
     * @throws IllegalArgumentException
     *             if <i>bindingSet</i> is <code>null</code>.
     */
    public boolean isFullyBound(int index, IBindingSet bindingSet) {
        
//        if (index < 0 || index >= body.length)
//            throw new IndexOutOfBoundsException();

        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final IPredicate pred = tail[index];

        final int arity = pred.arity();
        
        for(int j=0; j<arity; j++) {

            final IVariableOrConstant t = pred.get(j);
            
            // check any variables.
            if (t.isVar()) {
                
                // if a variable is unbound then return false.
                if (!bindingSet.isBound((IVariable) t)) {
                    
                    return false;
                    
                }
                
            }
            
        }
        
        return true;
        
    }
    
    /**
     * If the rule is fully bound for the given bindings.
     * 
     * @param bindingSet
     *            The bindings.
     * 
     * @return true if there are no unbound variables in the rule given those
     *         bindings.
     */
    public boolean isFullyBound(IBindingSet bindingSet) {

        for (int i = 0; i < tail.length; i++) {

            if (!isFullyBound(i, bindingSet))
                return false;

        }

        return true;

    }
    
    /**
     * Return <code>true</code> unless the {@link IBindingSet} violates a
     * {@link IConstraint} declared for this {@link Rule}.
     * 
     * @param bindingSet
     *            The binding set.
     * 
     * @return <code>true</code> unless a constraint is violated by the
     *         bindings.
     */
    public boolean isLegal(IBindingSet bindingSet) {
        
        if (constraints != null) {

            // check constraints.
            
            for (int i = 0; i < constraints.length; i++) {

                final IConstraint constraint = constraints[i];
                
                if (!constraint.accept(bindingSet)) {

                    if(log.isDebugEnabled()) {
                        
                        log.debug("Rejected by "
                                + constraint.getClass().getSimpleName()
                                + "\n" + toString(bindingSet));
                        
                    }
                    
                    return false;

                }

            }

        }
        
        return true;
        
    }

    /**
     * Return <code>true</code> iff the rule declares this variable.
     * 
     * @param var
     *            Some variable.
     * 
     * @return True if the rule declares that variable.
     * 
     * @throws IllegalArgumentException
     *             if <i>var</i> is <code>null</code>.
     */
    public boolean isDeclared(IVariable var) {
        
        if (var == null)
            throw new IllegalArgumentException();

        return vars.contains(var);
        
    }

    /*
     * IProgram.
     */

    /**
     * Returns <code>false</code> (the return value does not matter since a
     * single step can not be parallelized).
     */
    public boolean isParallel() {
        
        return false;
        
    }

    /**
     * Returns <code>false</code>. If you want the closure of a single rule
     * then add it to a suitable {@link Program} instance.
     */
    public boolean isClosure() {
        
        return false;
        
    }
    
    /**
     * Always returns an empty iterator.
     */
    public Iterator<IProgram> steps() {

        return Collections.EMPTY_LIST.iterator();
        
    }
    
    public int stepCount() {
        
        return 0;
        
    }
    
    public IProgram[] toArray() {
        
        return new Rule[] {};
        
    }
    
}
