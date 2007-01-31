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

import java.util.Vector;

import com.bigdata.objndx.IEntryIterator;


public class AbstractRuleRdfs68101213 extends AbstractRuleRdf {

    public AbstractRuleRdfs68101213
        ( InferenceEngine store, 
          Triple head, 
          Triple body
          ) {

        super(store, head, new Pred[] { body });

    }
    
    protected SPO[] collectEntailments() {
        
        Vector<SPO> entailments = new Vector<SPO>();
        
        byte[] startKey = store.keyBuilder.statement2Key
            ( body[0].p.id, body[0].o.id, 0
              );
            
        byte[] endKey = store.keyBuilder.statement2Key
            ( body[0].p.id, body[0].o.id+1, 0
              );
        
        IEntryIterator it = store.ndx_pos.rangeIterator(startKey,endKey); 
        
        while ( it.hasNext() ) {
            
            it.next();
            SPO stmt = 
                new SPO(store.ndx_pos.keyOrder,store.keyBuilder,it.getKey());
            
            long _s = head.s.isVar() ? stmt.s : head.s.id;
            long _p = head.p.isVar() ? stmt.s : head.p.id;
            long _o = head.o.isVar() ? stmt.s : head.o.id;
            
            entailments.add( new SPO(_s, _p, _o) );
            
        }
        
        return entailments.toArray( new SPO[entailments.size()] );
        
    }

}