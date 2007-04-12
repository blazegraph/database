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
import java.util.Vector;

import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.TempTripleStore;


public abstract class AbstractRuleRdfs2379 extends AbstractRuleRdf {

    public AbstractRuleRdfs2379
        ( InferenceEngine store, 
          Triple head, 
          Pred[] body
          ) {

        super( store, head, body );

    }
    
    public Stats apply( TempTripleStore entailments ) {

        Stats stats = new Stats();
        
        long computeStart = System.currentTimeMillis();
        
        // create a place to hold the entailments
        Vector<SPO> stmts3 = new Vector<SPO>(BUFFER_SIZE);

        SPO[] stmts1 = getStmts1();
        for ( int i = 0; i < stmts1.length; i++ ) {
            SPO[] stmts2 = getStmts2( stmts1[i] );
            for ( int j = 0; j < stmts2.length; j++ ) {
                if (stmts3.size() == BUFFER_SIZE) {
                    dumpBuffer
                        ( stmts3.toArray( new SPO[stmts3.size()] ),
                          entailments
                          );
                    stmts3.clear();
                }
                stmts3.add( buildStmt3( stmts1[i], stmts2[j] ) );
                stats.numComputed++;
            }
        }
        if(debug) dumpBuffer( stmts3.toArray( new SPO[stmts3.size()] ), entailments );
        
        stats.computeTime = System.currentTimeMillis() - computeStart;

        return stats;

    }
    
    // default behavior is to use POS index to match body[0] and then sort
    // using POSComparator since the default for body[1] is the POS index
    // again (is the sort even necessary?)
    protected SPO[] getStmts1() {
        
        // use the POS index to look up the matches for body[0], the more
        // constrained triple
        byte[] fromKey = store.keyBuilder.statement2Key( body[0].p.id, 0, 0 );
        byte[] toKey = store.keyBuilder.statement2Key( body[0].p.id+1, 0, 0 );
        SPO[] stmts1 = store.getStatements(store.getPOSIndex(), KeyOrder.POS, fromKey, toKey);

        // make sure the statements are in POS order, since we are going to be
        // doing lookups against the POS index in a moment
        Arrays.sort(stmts1,POSComparator.INSTANCE);
        
        return stmts1;
        
    }
    
    // default behavior is to join the subject of stmt1 with the predicate
    // of body[1] using the POS index
    protected SPO[] getStmts2( SPO stmt1 ) {
        
        byte[] fromKey = store.keyBuilder.statement2Key(stmt1.s, 0, 0);
        byte[] toKey = store.keyBuilder.statement2Key(stmt1.s+1, 0, 0);
        return store.getStatements(store.getPOSIndex(), KeyOrder.POS, fromKey, toKey);
    
    }
    
    protected abstract SPO buildStmt3( SPO stmt1, SPO stmt2 );

}