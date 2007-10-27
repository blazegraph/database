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
package com.bigdata.rdf.inf;

import java.util.Iterator;

import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Rdf1:
 * 
 * <pre>
 *   triple(?v rdf:type rdf:Property) :-
 *      triple( ?u ?v ?x ).
 * </pre>
 */
public class RuleRdf01 extends AbstractRuleRdf {
    
    protected final Var v;
    
    public RuleRdf01(InferenceEngine inf) {

        super(inf, //
                new Triple(var("v"), inf.rdfType, inf.rdfProperty), //
                new Pred[] { //
                    new Triple(var("u"), var("v"), var("x"))//
                });
        
        this.v = var("v");

    }

    public RuleStats apply(final RuleStats stats, final SPOBuffer buffer) {

        final long computeStart = System.currentTimeMillis();

        resetBindings();

        // find the distinct predicates in the KB (efficient op).
        Iterator<Long> itr = db.getAccessPath(KeyOrder.POS).distinctTermScan();

        while (itr.hasNext()) {

            stats.stmts1++;

            /*
             * bind [v].
             * 
             * Note: This rule explicitly leaves [u] and [x] unbound so that the
             * justifications will be wildcards for those variables.
             */

            set(v,itr.next());

            emit(buffer);

            stats.numComputed++;

        }

        assert checkBindings();
        
        stats.elapsed += System.currentTimeMillis() - computeStart;

        return stats;

    }

}
