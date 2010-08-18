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

package com.bigdata.relation.rule.eval;

import com.bigdata.bop.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;

/**
 * Useful for testing - will not reorder the join predicates.
 * 
 * @author mike
 */
public class NoReorderEvaluationPlan implements IEvaluationPlan {
    
    protected final IJoinNexus joinNexus;
    
    protected final IRule rule;
    
    protected final long[/*tailIndex*/] rangeCount;
    
    protected final int[] order;
    
    public NoReorderEvaluationPlan(final IJoinNexus joinNexus, 
            final IRule rule) {
        
        if (joinNexus == null)
            throw new IllegalArgumentException();

        if (rule == null)
            throw new IllegalArgumentException();
        
        this.joinNexus = joinNexus;
        
        this.rule = rule;

        final int tailCount = rule.getTailCount();
        
        this.rangeCount = new long[tailCount];
        
        this.order = new int[tailCount];
        
        for (int i = 0; i < tailCount; i++) {
            order[i] = i; // -1 is used to detect logic errors.
            rangeCount[i] = -1L;  // -1L indicates no range count yet.
        }
        
    }
    
    public int[] getOrder() {
        
        return order;
        
    }

    /**
     * <code>true</code> iff the rule was proven to have no solutions.
     * 
     * @todo this is not being computed.
     */
    private boolean empty = false;
    
    public boolean isEmpty() {
        
        return empty;
        
    }

    public long rangeCount(final int tailIndex) {

        if (rangeCount[tailIndex] == -1L) {

            final IPredicate predicate = rule.getTail(tailIndex);
            
            final ISolutionExpander expander = predicate.getSolutionExpander();

            if (expander != null && expander.runFirst()) {

                /*
                 * Note: runFirst() essentially indicates that the cardinality
                 * of the predicate in the data is to be ignored. Therefore we
                 * do not request the actual range count and just return -1L as
                 * a marker indicating that the range count is not available.
                 */
                
                return -1L;
                
            }
            
            final long rangeCount = joinNexus.getRangeCountFactory()
                    .rangeCount(rule.getTail(tailIndex));

            this.rangeCount[tailIndex] = rangeCount;

        }

        return rangeCount[tailIndex];

    }
    
}
