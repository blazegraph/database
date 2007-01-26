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

import java.util.Arrays;


/**
 * rdfs11:
 * 
 * <pre>
 *       triple(?u,rdfs:subClassOf,?x) :-
 *          triple(?u,rdfs:subClassOf,?v),
 *          triple(?v,rdf:subClassOf,?x). 
 * </pre>
 */
public class RuleRdfs11 extends Rule {

    public RuleRdfs11(InferenceEngine store,Var u, Var v, Var x) {

        super(store, new Triple(u, store.rdfsSubClassOf, x), //
                new Pred[] { //
                new Triple(u, store.rdfsSubClassOf, v),//
                        new Triple(v, store.rdfsSubClassOf, x) //
                });

    }
    
    // FIXME implement rdfs11.
    public int apply() {
        
        // the predicate is fixed for both parts of the rule.
        final long p = store.rdfsSubClassOf.id;
        
        // the key for that predicate.
        final byte[] pkey = store.keyBuilder.id2key(p);
        
        // the successor of that key.
        final byte[] pkey1 = store.keyBuilder.id2key(p + 1);

        /*
         * Query for the 1st part of the rule.
         * 
         * Note that it does not matter which half of the rule we execute
         * first since they are both 2-unbound with the same predicate bound
         * and will therefore have exactly the same results.
         * 
         * Further note that we can perform a self-join on the returned
         * triples without going back to the database.
         */

        // in POS order.
        SPO[] ret1 = store.getStatements(store.ndx_pos, pkey, pkey1);
        
        // in SPO order.
        SPO[] ret2 = ret1.clone();
        Arrays.sort(ret2,SPOComparator.INSTANCE);

        // FIXME do join, insert or return statements.
        throw new UnsupportedOperationException();

    }

}