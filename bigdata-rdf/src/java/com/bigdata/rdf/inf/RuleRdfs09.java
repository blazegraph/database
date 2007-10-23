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

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.util.KeyOrder;

/**
 * rdfs9:
 * <pre>
 *       triple(?v,rdf:type,?x) :-
 *          triple(?u,rdfs:subClassOf,?x),
 *          triple(?v,rdf:type,?u). 
 * </pre>
 */
public class RuleRdfs09 extends AbstractRuleRdfs_2_3_7_9 {

    public RuleRdfs09( InferenceEngine inf, Var u, Var x, Var v ) {

        super(inf, new Triple(v, inf.rdfType, x),
                new Pred[] {
                new Triple(u, inf.rdfsSubClassOf, x),
                new Triple(v, inf.rdfType, u)
                });

    }
    
    /**
     * Overriden to be two bound (more selective, but also joining stmt1.s to
     * stmt2.o rather than to stmt2.p) and also returning data in POS order. The
     * query is formed from triple(?v,rdf:type,stmt1.s) and expressed in POS
     * order as { rdf:type, stmt1.s, ?v }.
     */
    protected SPO[] getStmts2( SPO stmt1 ) {
        
        byte[] fromKey = db.getKeyBuilder().statement2Key(inf.rdfType.id,
                stmt1.s, NULL);

        byte[] toKey = db.getKeyBuilder().statement2Key(inf.rdfType.id,
                stmt1.s + 1, NULL);

        return db.getStatements(KeyOrder.POS, fromKey, toKey);
    
    }
    
    protected SPO buildStmt3( SPO stmt1, SPO stmt2 ) {
        
        return new SPO( stmt2.s, inf.rdfType.id, stmt1.o, StatementEnum.Inferred );
        
    }

}
