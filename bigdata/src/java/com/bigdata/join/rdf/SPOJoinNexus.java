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

import com.bigdata.join.ArrayBindingSet;
import com.bigdata.join.Constant;
import com.bigdata.join.DefaultEvaluationPlan;
import com.bigdata.join.IBindingSet;
import com.bigdata.join.IConstant;
import com.bigdata.join.IEvaluationPlan;
import com.bigdata.join.IJoinNexus;
import com.bigdata.join.IPredicate;
import com.bigdata.join.IRelationLocator;
import com.bigdata.join.IRule;
import com.bigdata.join.ISolution;
import com.bigdata.join.IVariable;
import com.bigdata.join.RuleState;
import com.bigdata.join.Solution;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOJoinNexus implements IJoinNexus {

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
    
    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet ) {

        if (e == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final ISPO spo = (ISPO) e;
        
        final SPOPredicate pred = (SPOPredicate)predicate;
        
        if (pred.s().isVar()) {

            bindingSet.set((IVariable) pred.s(), new Constant<Long>(spo.s()));

        }

        if (pred.p().isVar()) {

            bindingSet.set((IVariable) pred.p(), new Constant<Long>(spo.p()));

        }

        if (pred.o().isVar()) {

            bindingSet.set((IVariable) pred.o(), new Constant<Long>(spo.o()));
            
        }
        
    }

    public SPO newElement(IPredicate predicate, IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final SPOPredicate pred = (SPOPredicate)predicate;
        
        final long s, p, o;
        
        if (pred.s().isVar()) {

            s = ((IConstant<Long>)bindingSet.get((IVariable) pred.s())).get().longValue();

        } else {
            
            s = ((IConstant<Long>)pred.s()).get();
            
        }
        
        if (pred.p().isVar()) {

            p = ((IConstant<Long>)bindingSet.get((IVariable) pred.p())).get().longValue();

        } else {
            
            p = ((IConstant<Long>)pred.p()).get();
            
        }

        if (pred.o().isVar()) {

            o = ((IConstant<Long>)bindingSet.get((IVariable) pred.o())).get().longValue();

        } else {
            
            o = ((IConstant<Long>)pred.o()).get();
            
        }
        
        return new SPO(s,p,o);
        
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

    public IRelationLocator getRelationLocator() {
        
        return relationLocator;
        
    }

    public IEvaluationPlan newEvaluationPlan(IRule rule) {
        
        return new DefaultEvaluationPlan(this, rule);
        
    }
    
}
