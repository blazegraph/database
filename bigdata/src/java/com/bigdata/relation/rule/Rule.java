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
package com.bigdata.relation.rule;

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
     * The optional override for rule evaluation.
     */
    final private IRuleTaskFactory taskFactory;
    
    /**
     * The set of distinct variables declared by the rule.
     */
    final private Set<IVariable> vars;

    public int getVariableCount() {
        
        return vars.size();
        
    }

    public Iterator<IVariable> getVariables() {
        
        return vars.iterator();
        
    }
    
    public int getTailCount() {
        
        return tail.length;
        
    }
    
    public IPredicate getHead() {
        
        return head;
        
    }
    
    public Iterator<IPredicate> getTail() {
        
        return Arrays.asList(tail).iterator();
        
    }

    public IPredicate getTail(int index) {
        
        return tail[index];
        
    }
    
    public int getConstraintCount() {
        
        return constraints == null ? 0 : constraints.length;
        
    }

    public IConstraint getConstraint(int index) {
        
        if (constraints == null)
            throw new IndexOutOfBoundsException();
        
        return constraints[index];
        
    }
    
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
    
    public String toString() {
        
        return toString(null);
        
    }
    
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

        this(name, head, tail, constraints, null/* task */);
        
    }

    /**
     * 
     * @param name
     * @param head
     * @param tail
     * @param constraints
     * @param taskFactory
     *            Optional override for rule evaluation (MAY be
     *            <code>null</code>).
     */
    protected Rule(String name, IPredicate head, IPredicate[] tail,
            IConstraint[] constraints, IRuleTaskFactory taskFactory) {

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

        // MAY be null
        this.taskFactory = taskFactory;

    }

    public IRule specialize(IBindingSet bindingSet, IConstraint[] constraints) {

        return specialize(getName() + "'", bindingSet, constraints);

    }
    
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

        
        /*
         * Pass on the task factory - it is up to it to be robust to
         * specialization of the rule.
         */
        
        final IRuleTaskFactory taskFactory = getTaskFactory();

        final IRule newRule = new Rule(name, newHead, newTail, newConstraint,
                taskFactory);

        return newRule;

    }

    private IPredicate[] bind(IPredicate[] predicates, IBindingSet bindingSet) {
        
        final IPredicate[] tmp = new IPredicate[predicates.length];
        
        for(int i=0; i<predicates.length; i++) {
            
            tmp[i] = predicates[i].asBound(bindingSet);
            
        }
        
        return tmp;
        
    }
    
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
    
    public boolean isFullyBound(IBindingSet bindingSet) {

        for (int i = 0; i < tail.length; i++) {

            if (!isFullyBound(i, bindingSet))
                return false;

        }

        return true;

    }
    
    public boolean isConsistent(IBindingSet bindingSet) {
        
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

    public boolean isDeclared(IVariable var) {
        
        if (var == null)
            throw new IllegalArgumentException();

        return vars.contains(var);
        
    }

    /*
     * IProgram.
     */

    final public boolean isRule() {
        
        return true;
        
    }
    
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
    @SuppressWarnings("unchecked")
    public Iterator<IProgram> steps() {

        return Collections.EMPTY_LIST.iterator();
        
    }
    
    public int stepCount() {
        
        return 0;
        
    }
    
    public IProgram[] toArray() {
        
        return new Rule[] {};
        
    }

    public IRuleTaskFactory getTaskFactory() {
        
        return taskFactory;
        
    }
    
}
