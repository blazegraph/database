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

import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IRuleTaskFactory;

/**
 * Default impl.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Rule<E> implements IRule<E> {

    /**
     * 
     */
    private static final long serialVersionUID = -3834383670300306143L;

    final static transient protected Logger log = Logger.getLogger(Rule.class);

    final static transient protected boolean INFO = log.isInfoEnabled();

    final static transient protected boolean DEBUG = log.isDebugEnabled();

    /**
     * Singleton factory for {@link Var}s (delegates to {@link Var#var(String)}).
     * 
     * @see Var#var(String)
     * 
     * @todo it is a good idea to use this factory rather than
     *       {@link Var#var(String)} as the latter MAY be replaced by
     *       per-rule-instance variables rather than globally canonical
     *       variables
     *       <P>
     *       the only problem with this is that we need to access the variables
     *       in a bindingset by name after high-level query since the rule may
     *       not be available to the caller, e.g., the match rule uses
     *       dynamically generated rules that are not visible to the caller who
     *       only sees the binding sets.
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

//    /**
//     * <code>true</code> iff a DISTINCT constraint will be imposed when the
//     * rule is evaluated as a query.
//     */
//    final private boolean distinct;

    /**
     * Options that effect query evaluation.
     */
    final private IQueryOptions queryOptions;
    
    /**
     * Optional constraints on the bindings.
     */
    final private IConstraint[] constraints;

    /**
     * The optional override for rule evaluation.
     */
    final private IRuleTaskFactory taskFactory;
    
    /**
     * The bound constants (if any).
     */
    final private IBindingSet constants;

    /**
     * The set of distinct variables declared by the rule.
     */
    final private Set<IVariable> vars;
    
    /**
     * The set of distinct required variables declared by the rule.  These
     * are used in the projection (select or construct) and for aggregation,
     * and thus can never be dropped from the binding sets.
     */
    final private Set<IVariable> requiredVars;

    final public int getVariableCount() {
        
        return vars.size();
        
    }

    final public Iterator<IVariable> getVariables() {
        
        return vars.iterator();
        
    }
    
    final public int getRequiredVariableCount() {
        
        return requiredVars.size();
        
    }

    final public Iterator<IVariable> getRequiredVariables() {
        
        return requiredVars.iterator();
        
    }
    
    final public int getTailCount() {
        
        return tail.length;
        
    }
    
    final public IPredicate getHead() {
        
        return head;
        
    }
    
    final public Iterator<IPredicate> getTail() {
        
        return Arrays.asList(tail).iterator();
        
    }

    final public IPredicate getTail(int index) {
        
        return tail[index];
        
    }

    final public IQueryOptions getQueryOptions() {
        
        return queryOptions;
        
    }
    
//    final public boolean isDistinct() {
//        
//        return distinct;
//        
//    }
    
    final public int getConstraintCount() {
        
        return constraints == null ? 0 : constraints.length;
        
    }

    final public IConstraint getConstraint(int index) {
        
        if (constraints == null)
            throw new IndexOutOfBoundsException();
        
        return constraints[index];
        
    }
    
    final public Iterator<IConstraint> getConstraints() {
        
        return Arrays.asList(constraints).iterator();
        
    }
    
    public final IBindingSet getConstants() {
        
        return constants;
        
    }
    
    final public String getName() {
            
        return name;

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
        if (head != null) {
            
            sb.append(head.toString(bindingSet));

        }

        if(!(constants instanceof EmptyBindingSet)) {
            
            sb.append(", where ");
            
            sb.append(constants);
            
        }
        
        if (queryOptions != null) {
            
            sb.append(", queryOptions=" + queryOptions);
            
        }
        
        return sb.toString();

    }

    /**
     * Rule ctor.
     * 
     * @param name
     *            A label for the rule.
     * @param head
     *            The subset of bindings that are selected by the rule.
     * @param tail
     *            The tail (aka body) of the rule.
     * @param constraints
     *            An array of constaints on the legal states of the bindings
     *            materialized for the rule.
     * 
     * @throws IllegalArgumentException
     *             if the <i>name</i> is <code>null</code>.
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
//    * @throws IllegalArgumentException
//    *             if the <i>head</i> names more the one relation in its view
//    *             (tails may name more than one, which is interpreted as a
//    *             fused view of the named relations).
    public Rule(String name, IPredicate head, IPredicate[] tail,
            IConstraint[] constraints) {

        this(name, head, tail, QueryOptions.NONE, constraints,
                null/* constants */, null/* taskFactory */);
        
    }

    /**
     * Rule ctor.
     * 
     * @param name
     *            A label for the rule.
     * @param head
     *            The subset of bindings that are selected by the rule.
     * @param tail
     *            The tail (aka body) of the rule.
     * @param queryOptions
     *            Options that effect evaluation of the rule as a query.
     * @param constraints
     *            An array of constaints on the legal states of the bindings
     *            materialized for the rule.
     * 
     * @throws IllegalArgumentException
     *             if the <i>name</i> is <code>null</code>.
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
//    * @throws IllegalArgumentException
//    *             if the <i>head</i> names more the one relation in its view
//    *             (tails may name more than one, which is interpreted as a
//    *             fused view of the named relations).
    public Rule(String name, IPredicate head, IPredicate[] tail, 
            final IQueryOptions queryOptions, IConstraint[] constraints) {

        this(name, head, tail, queryOptions, constraints,
                null/* constants */, null/* taskFactory */, null/* requiredVars*/);

    }

    public Rule(String name, IPredicate head, IPredicate[] tail,
            IQueryOptions queryOptions, IConstraint[] constraints,
            IBindingSet constants, IRuleTaskFactory taskFactory) {

        this(name, head, tail, queryOptions, constraints,
                null/* constants */, taskFactory, null /* requiredVars */);

    }
    
    /**
     * Fully specified ctor.
     * 
     * @param name
     *            The name of the rule.
     * @param head
     *            The head of the rule. This is optional for rules that will be
     *            evaluated using {@link ActionEnum#Query} but required for
     *            rules that will be evaluated using a mutation
     *            {@link ActionEnum}.
     * @param tail
     *            The predicates in the tail of the rule.
     * @param queryOptions
     *            Additional constraints on the evaluate of a rule as a query
     *            (required, but see {@link QueryOptions#NONE}).
     * @param constraints
     *            The constraints on the rule (optional).
     * @param constants
     *            Bindings for variables that are bound as constants for the
     *            rule (optional).
     * @param taskFactory
     *            Optional override for rule evaluation (MAY be
     *            <code>null</code>).
     * @param requiredVars
     *            Optional set of required variables.  If <code>null</code>,
     *            all variables are assumed to be required.           
     */
    public Rule(String name, IPredicate head, IPredicate[] tail,
            IQueryOptions queryOptions, IConstraint[] constraints,
            IBindingSet constants, IRuleTaskFactory taskFactory, 
            IVariable[] requiredVars) {

        if (name == null)
            throw new IllegalArgumentException();
        
//        if (head == null)
//            throw new IllegalArgumentException();

        if (tail == null)
            throw new IllegalArgumentException();

        if (queryOptions == null)
            throw new IllegalArgumentException();
        
        if (constants == null) {
            
            constants = EmptyBindingSet.INSTANCE;
            
        }
        
        this.name = name;

        // the predicate declarations for the body.
        this.tail = tail;

        final Set<IVariable> vars = new HashSet<IVariable>();

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
        if(head != null) {

            if (head.getRelationCount() != 1) {
                
                throw new IllegalArgumentException(
                        "Expecting a single relation identifier for the head: head="
                                + head);
                
            }
            
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

        this.queryOptions = queryOptions;
        
        // constraint(s) on the variable bindings (MAY be null).
        this.constraints = constraints;

        if (constraints != null) {

            for (int i = 0; i < constraints.length; i++) {
            
                assert constraints[i] != null;
            
            }
            
        }

        // NOT NULL.
        this.constants = constants;
        
        // MAY be null
        this.taskFactory = taskFactory;
        
        // required variables may be null
        final Set<IVariable> s = new HashSet<IVariable>();
        if (requiredVars == null) {
            s.addAll(vars);
        } else {
            for (IVariable v : requiredVars) {
                s.add(v);
            }
        }
        this.requiredVars = Collections.unmodifiableSet(s);

    }

    public IRule<E> specialize(IBindingSet bindingSet, IConstraint[] constraints) {

        return specialize(getName() + "'", bindingSet, constraints);

    }
    
    public IRule<E> specialize(String name, IBindingSet bindingSet,
            IConstraint[] constraints) {

        if (name == null)
            throw new IllegalArgumentException();

        if (bindingSet == null)
            throw new IllegalArgumentException();

        /*
         * Setup the new head and the body for the new rule by applying the
         * bindings.
         */

        final IPredicate newHead = (head == null ? null : head
                .asBound(bindingSet));

        final IPredicate[] newTail = bind(tail, bindingSet);
        
//        final IPredicate newHead = head;
//
//        final IPredicate[] newTail = tail;

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

        final IRule<E> newRule = new Rule<E>(name, newHead, newTail,
                queryOptions, newConstraint, bindingSet, taskFactory, 
                requiredVars.toArray(new IVariable[requiredVars.size()]));

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

    public int getVariableCount(int index, IBindingSet bindingSet) {
        
//      if (index < 0 || index >= body.length)
//          throw new IndexOutOfBoundsException();

      if (bindingSet == null)
          throw new IllegalArgumentException();
      
      final IPredicate pred = tail[index];

      final int arity = pred.arity();
      
      int nbound = 0;
      
      for(int j=0; j<arity; j++) {

          final IVariableOrConstant t = pred.get(j);
          
          // check any variables.
          if (t.isVar()) {
              
              // if a variable is unbound then return false.
              if (!bindingSet.isBound((IVariable) t)) {
                  
                  nbound++;
                  
              }
              
          }
          
      }
      
      return nbound;
      
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

                    if(DEBUG) {
                        
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

    public IRuleTaskFactory getTaskFactory() {
        
        return taskFactory;
        
    }
    
    /*
     * IStep.
     */

    final public boolean isRule() {
        
        return true;
        
    }

}
