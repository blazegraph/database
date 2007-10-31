/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 29, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Iterator;

import com.bigdata.rdf.store.AbstractTripleStore;
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

    public AbstractRuleDistinctTermScan(AbstractTripleStore db, Triple head, Pred[] body) {

        super(db, head, body);
        
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
