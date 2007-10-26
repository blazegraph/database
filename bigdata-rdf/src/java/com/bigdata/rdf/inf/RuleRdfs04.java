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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Arrays;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;

/**
 * Computes both parts of rdfs4
 * 
 * <pre>
 * rdfs4a: (?u ?a ?x) -&gt; (?u rdf:type rdfs:Resource)
 * 
 * rdfs4b: (?u ?a ?v) -&gt; (?v rdf:type rdfs:Resource)
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write unit test.
 */
public class RuleRdfs04 extends AbstractRuleRdf {

    /**
     * @param inf
     * @param head
     * @param body
     */
    public RuleRdfs04(InferenceEngine inf, Var u, Var a, Var x) {
        
        // Note: This declaration of the rule is not complete since we are
        // computing two heads in the same rule as an efficiency.
        
        super(inf, new Triple(u, inf.rdfType, inf.rdfsResource),
                new Pred[] { new Triple(u, a, x) });
        
    }

    public RuleStats apply(RuleStats stats, SPOBuffer buffer) {

        final long computeStart = System.currentTimeMillis();

        // Full statement scan (3 unbound).

        ISPOIterator itr = db.getAccessPath(NULL, NULL, NULL).iterator();

        while (itr.hasNext()) {

            SPO[] stmts = itr.nextChunk();

            if(DEBUG) {
                
                log.debug("stmts1: chunk="+stmts.length+"\n"+Arrays.toString(stmts));
                
            }

            for (SPO stmt1 : stmts) {

                stats.stmts1++;

                // rdfs4a: (?u ?a ?x) -&gt; (?u rdf:type rdfs:Resource)
                {

                    SPO newSPO = new SPO(stmt1.s, inf.rdfType.id,
                            inf.rdfsResource.id, StatementEnum.Inferred);

                    Justification jst = null;

                    if (justify) {

                        jst = new Justification(this, newSPO,
                                new SPO[] { stmt1 });

                    }

                    buffer.add(newSPO, jst);

                }

                // rdfs4b: (?u ?a ?v) -&gt; (?v rdf:type rdfs:Resource)
                {

                    SPO newSPO = new SPO(stmt1.o, inf.rdfType.id,
                            inf.rdfsResource.id, StatementEnum.Inferred);

                    Justification jst = null;

                    if (justify) {

                        jst = new Justification(this, newSPO,
                                new SPO[] { stmt1 });

                    }

                    buffer.add(newSPO, jst);

                }

                stats.numComputed += 2;
                
            }

        }

        stats.elapsed += System.currentTimeMillis() - computeStart;

        return stats;

    }

}
