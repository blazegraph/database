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

import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Rules having a tail of the general form:
 * <pre>
 * ( ?u C1 C2 )
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractRuleRdfs_6_8_10_12_13 extends AbstractRuleNestedSubquery {

//    final Var u;
    
    public AbstractRuleRdfs_6_8_10_12_13
        ( AbstractTripleStore db, 
          Triple head, 
          Triple body
          ) {

        super(db, head, new Pred[] { body });

        /*
         * check the variable binding pattern on the tail.
         */
        
        assert super.body.length == 1;
        
        assert super.body[0].s.isVar();
        assert super.body[0].p.isConstant();
        assert super.body[0].o.isConstant();
        
    }
   
    /**
     * @todo This is actually correct for any single predicate rule. However,
     *       {@link AbstractRuleDistinctTermScan} is better suited for some
     *       special cases.
     */
    public RuleStats apply( final boolean justify, final SPOBuffer buffer) {
        
        if(true) return apply0(justify,buffer);
        
        final long computeStart = System.currentTimeMillis();
        
        resetBindings();
        
        /*
         * For example, rdfs10:
         * 
         * triple(?u,rdfs:subClassOf,?u) :- triple(?u,rdf:type,rdfs:Class).
         */

        /*
         * 2-bound query on POS index.
         */
        ISPOIterator itr = getAccessPath(0/*pred*/).iterator();

        while(itr.hasNext()) {
            
            SPO[] stmts0 = itr.nextChunk();
            
            if(DEBUG) {
                
                log.debug("stmts1: chunk="+stmts0.length+"\n"+Arrays.toString(stmts0));
                
            }

            stats.nstmts[0] += stmts0.length;
            
            for (SPO stmt1 : stmts0) {

                bind(0,stmt1);
                
                //set(u,stmt1.s);

                emit(justify,buffer);

            }

        }
        
        assert checkBindings();
        
        stats.elapsed += System.currentTimeMillis() - computeStart;
        
        return stats;
        
    }

}
