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
 * Created on Aug 18, 2008
 */

package com.bigdata.relation.rule.eval;

import java.util.Arrays;

import com.bigdata.relation.rule.IRule;

/**
 * A factory for {@link IEvaluationPlan}s that uses a caller specified
 * evaluation order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FixedEvaluationPlanFactory implements IEvaluationPlanFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 4374802847795489346L;
    
    /** the evaluation order as specified to the ctor. */
    private final int[] order;
    
    public FixedEvaluationPlanFactory(final int[] order) {

        if (order == null)
            throw new IllegalArgumentException();

        /*
         * verify that the evaluation order uses each predicate in the tail of
         * the rule exactly once (e.g., it is a permutation of the sequence
         * [0,1,2...,tailCount-1].
         */
        for (int i = 0; i < order.length; i++) {

            int nfound = 0;

            for (int j = 0; j < order.length; j++) {

                if (order[j] == i)
                    nfound++;

            }

            if (nfound == 0)
                throw new IllegalArgumentException("tailIndex=" + i
                        + " is not in the evaluation order: "
                        + Arrays.toString(order));
            if (nfound > 1)
                throw new IllegalArgumentException("tailIndex=" + i
                        + " occurs more than once in the evaluation order: "
                        + Arrays.toString(order));
            
        }
        
        this.order = order;
        
    }
    
    public IEvaluationPlan newPlan(final IJoinNexus joinNexus, final IRule rule) {
   
        final int tailCount = rule.getTailCount();

        if (order.length != tailCount) {

            throw new IllegalArgumentException("tailCount=" + tailCount
                    + ", but have order[] of length=" + order.length);
            
        }
        
        return new IEvaluationPlan() {

            public int[] getOrder() {
                
                return order;
                
            }

            public boolean isEmpty() {
                
                return false;
                
            }

            public long rangeCount(int tailIndex) {

                return joinNexus.getRangeCountFactory().rangeCount(
                        rule.getTail(tailIndex));
                
            }
            
            public String toString() {
                
                return "order="+Arrays.toString(order);
                
            }
            
        };
        
    }

}
