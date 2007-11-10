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
/*
 * Created on Oct 29, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Iterator;

import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Base class for rules having a single predicate that is none bound in the tail
 * and a single variable in the head. These rules can be evaluated using
 * {@link IAccessPath#distinctTermScan()} rather than a full index scan. For
 * example:
 * 
 * <pre>
 *  rdf1:   (?u ?a ?y) -&gt; (?a rdf:type rdf:Property)
 *  rdfs4a: (?u ?a ?x) -&gt; (?u rdf:type rdfs:Resource)
 *  rdfs4b: (?u ?a ?v) -&gt; (?v rdf:type rdfs:Resource)
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleDistinctTermScan extends AbstractRuleRdf {
    
    /**
     * The sole unbound variable in the head of the rule.
     */
    private final Var h;
    
    /**
     * The access path that corresponds to the position of the unbound variable
     * reference from the head.
     */
    private final KeyOrder keyOrder;

    public AbstractRuleDistinctTermScan(Triple head, Pred[] body) {

        super( head, body);
        
        // head must be one bound.
        assert head.getVariableCount() == 1;

        // tail must have one predicate.
        assert body.length == 1;
        
        // the predicate in the tail must be "none" bound.
        assert body[0].getVariableCount() == N;

        // figure out which position in the head is the variable.
        if(head.s.isVar()) {
            
            h = (Var)head.s;
            
        } else if( head.p.isVar() ) {
            
            h = (Var)head.p;
            
        } else if( head.o.isVar() ) {
        
            h = (Var)head.o;
        
        } else {
            
            throw new AssertionError();
            
        }

        /*
         * figure out which access path we need for the distinct term scan which
         * will bind the variable in the head.
         */
        if(body[0].s == h) {
            
            keyOrder = KeyOrder.SPO;
            
        } else if( body[0].p == h ) {
            
            keyOrder = KeyOrder.POS;
            
        } else if( body[0].o == h ) {
        
            keyOrder = KeyOrder.OSP;
        
        } else {
            
            throw new AssertionError();
            
        }

    }

    final public void apply(State state) {

        final long computeStart = System.currentTimeMillis();

        /*
         * find the distinct predicates in the KB (efficient op).
         */
        
        IAccessPath accessPath = state.focusStore == null ? state.database
                .getAccessPath(keyOrder) : state.focusStore
                .getAccessPath(keyOrder);
        
        Iterator<Long> itr = accessPath.distinctTermScan();

        while (itr.hasNext()) {

            state.stats.nstmts[0]++;

            // a distinct term identifier for the selected access path.
            final long id = itr.next();
            
            /*
             * bind [v].
             * 
             * Note: This explicitly leaves the other variables in the head
             * unbound so that the justifications will be wildcards for those
             * variables.
             */

            state.set(h, id );

            state.emit();

        }
        
        state.stats.elapsed += System.currentTimeMillis() - computeStart;

    }

}
