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

import com.bigdata.relation.rule.IRule;

/**
 * A factory for {@link IEvaluationPlan}s that do not reorder the predicates in
 * the tail.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NOPEvaluationPlanFactory implements IEvaluationPlanFactory {

    private static final long serialVersionUID = 0L;
    
    public static final transient NOPEvaluationPlanFactory INSTANCE = new NOPEvaluationPlanFactory();
    
    public IEvaluationPlan newPlan(final IJoinNexus joinNexus, final IRule rule) {
   
        final int tailCount = rule.getTailCount();
        
        final int[] order = new int[tailCount];
        
        for(int i=0; i<tailCount; i++) {
            
            order[i] = i;
            
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
            
        };
        
    }

}
