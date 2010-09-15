/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.List;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.E;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.CopyBindingSetOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;

/**
 * Utility class converts {@link IRule}s to {@link BOp}s.
 * <p>
 * Note: This is a stopgap measure designed to allow us to evaluate SPARQL
 * queries and verify the standalone {@link QueryEngine} while we develop a
 * direct translation from Sesame's SPARQL operator tree onto {@link BOp}s and
 * work on the scale-out query buffer transfer mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Implement Rule2BOpUtility.
 */
public class Rule2BOpUtility {

    /**
     * Convert an {@link IStep} into an operator tree. This should handle
     * {@link IRule}s and {@link IProgram}s as they are currently implemented
     * and used by the {@link BigdataSail}.
     * 
     * @param step
     *            The step.
     * 
     * @return
     */
    public static BindingSetPipelineOp convert(final IStep step) {
        
        if (step instanceof Rule)
            return convert((Rule) step);
        else if (step instanceof Program)
            return convert((Program) step);
        
        throw new UnsupportedOperationException();

    }

    /**
     * Convert a rule into an operator tree.
     * 
     * @param rule
     * 
     * @return
     */
    public static BindingSetPipelineOp convert(final Rule rule) {

        int bopId = 1;
        
        BindingSetPipelineOp left = new CopyBindingSetOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, bopId++),//
                        }));
        
        Iterator<Predicate> tails = rule.getTail();
        
        while (tails.hasNext()) {
        
            final int joinId = bopId++;
            
            final Predicate<?> pred = tails.next().setBOpId(bopId++);
            
            System.err.println(pred);
            
            final BindingSetPipelineOp joinOp = new PipelineJoin<E>(//
                    left, pred,//
                    NV.asMap(new NV[] {//
                            new NV(Predicate.Annotations.BOP_ID, joinId),//
                            }));
            
            left = joinOp;
            
        }
        
        System.err.println(toString(left));
        
        return left;
        
    }
    
    private static String toString(BOp bop) {
        
        StringBuilder sb = new StringBuilder();
        
        toString(bop, sb, 0);
        
        // chop off the last \n
        sb.setLength(sb.length()-1);
        
        return sb.toString();
        
    }
    
    private static void toString(final BOp bop, final StringBuilder sb, 
            final int indent) {
        
        for (int i = 0; i < indent; i++) {
            sb.append(' ');
        }
        sb.append(bop).append('\n');

        if (bop != null) {
            List<BOp> args = bop.args();
            for (BOp arg : args) {
                toString(arg, sb, indent+4);
            }
        }
        
    }
    
    /**
     * Convert a program into an operator tree.
     * 
     * @param program
     * 
     * @return
     */
    public static BindingSetPipelineOp convert(final Program program) {

        throw new UnsupportedOperationException();

    }

}
