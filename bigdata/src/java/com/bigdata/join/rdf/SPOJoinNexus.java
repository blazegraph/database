/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Jun 25, 2008
 */

package com.bigdata.join.rdf;

import com.bigdata.join.AbstractSolutionBuffer;
import com.bigdata.join.ActionEnum;
import com.bigdata.join.ArrayBindingSet;
import com.bigdata.join.BlockingBuffer;
import com.bigdata.join.Constant;
import com.bigdata.join.DefaultEvaluationPlan;
import com.bigdata.join.IBindingSet;
import com.bigdata.join.IBlockingBuffer;
import com.bigdata.join.IBuffer;
import com.bigdata.join.IConstant;
import com.bigdata.join.IEvaluationPlan;
import com.bigdata.join.IJoinNexus;
import com.bigdata.join.IMutableRelation;
import com.bigdata.join.IPredicate;
import com.bigdata.join.IProgram;
import com.bigdata.join.IProgramTask;
import com.bigdata.join.IRelationLocator;
import com.bigdata.join.IRule;
import com.bigdata.join.ISolution;
import com.bigdata.join.IVariable;
import com.bigdata.join.IVariableOrConstant;
import com.bigdata.join.LocalRuleExecutionTask;
import com.bigdata.join.RuleState;
import com.bigdata.join.Solution;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOJoinNexus implements IJoinNexus {

    /**
     * 
     */
    private static final long serialVersionUID = 1151635508669768925L;

    private final boolean elementOnly;
    
    private final IRelationLocator<SPO> relationLocator;
    
    public final boolean isElementOnly() {
        
        return elementOnly;
        
    }

    /**
     * 
     * @param elementOnly
     */
    public SPOJoinNexus(boolean elementOnly,
            IRelationLocator<SPO> relationLocator) {

        if (relationLocator == null)
            throw new IllegalArgumentException();

        this.elementOnly = elementOnly;

        this.relationLocator = relationLocator;
        
    }
    
    @SuppressWarnings("unchecked")
    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet ) {

        if (e == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final SPO spo = (SPO) e;
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>)predicate;
        
        {

            final IVariableOrConstant<Long> t = pred.get(0);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.s));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(1);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.p));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(2);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.o));
                
            }

        }
        
    }

    @SuppressWarnings("unchecked")
    public SPO newElement(IPredicate predicate, IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>) predicate;

        final long s = asBound(pred, 0, bindingSet);

        final long p = asBound(pred, 1, bindingSet);

        final long o = asBound(pred, 2, bindingSet);

        return new SPO(s, p, o);
        
    }

    /**
     * Extract the bound value from the predicate. When the predicate is not
     * bound at that index, then extract its binding from the binding set.
     * 
     * @param pred
     *            The predicate.
     * @param index
     *            The index into that predicate.
     * @param bindingSet
     *            The binding set.
     *            
     * @return The bound value.
     */
    private long asBound(IPredicate<ISPO> pred, int index, IBindingSet bindingSet) {

        final IVariableOrConstant<Long> t = pred.get(index);

        final IConstant<Long> c;
        if(t.isVar()) {
            
            c = bindingSet.get((IVariable) t);
            
        } else {
            
            c = (IConstant<Long>)t;
            
        }

        return c.get().longValue();

    }
    
    public ISolution<SPO> newSolution(IRule rule, IBindingSet bindingSet) {

        final SPO spo = newElement(rule.getHead(), bindingSet);

        if (elementOnly) {

            return new Solution<SPO>(spo);

        }

        return new Solution<SPO>(spo, rule, bindingSet.clone());

    }

    public IBindingSet newBindingSet(IRule rule) {

        return new ArrayBindingSet(rule.getVariableCount());
        
    }

    public RuleState newRuleState(IRule rule) {
    
        return new RuleState( rule, this );
        
    }

    public IRelationLocator<SPO> getRelationLocator() {
        
        return relationLocator;
        
    }

    public IEvaluationPlan newEvaluationPlan(IRule rule) {
        
        return new DefaultEvaluationPlan(this, rule);
        
    }

    public IBlockingBuffer<ISolution> newQueryBuffer() {

        return new BlockingBuffer<ISolution>();
        
    }
    
    /**
     * The default buffer capacity.
     */
    private final int DEFAULT_BUFFER_CAPACITY = 10000;
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {

        return new AbstractSolutionBuffer.InsertSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation);

    }
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {

        return new AbstractSolutionBuffer.DeleteSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation);

    }

    public IProgramTask newProgramTask(ActionEnum action, IProgram program) {
        
        return new LocalRuleExecutionTask(action, program, this);
        
    }

}
