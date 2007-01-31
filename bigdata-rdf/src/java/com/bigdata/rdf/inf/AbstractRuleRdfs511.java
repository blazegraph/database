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

import org.openrdf.model.URI;


public class AbstractRuleRdfs511 extends Rule {

    public AbstractRuleRdfs511(InferenceEngine store, Triple head, Pred[] body) {

        super(store, head, body);

    }
    
    public int apply() {

        long startTime = System.currentTimeMillis();
        
        // the predicate is fixed for all parts of the rule.
        final long p = head.p.id;
        
        // the key for that predicate.
        final byte[] pkey = store.keyBuilder.statement2Key(p, 0, 0);
        
        // the successor of that key.
        final byte[] pkey1 = store.keyBuilder.statement2Key(p+1, 0, 0);

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
        SPO[] stmts1 = store.getStatements(store.ndx_pos, pkey, pkey1);
        // in SPO order.
        Arrays.sort(stmts1,SPOComparator.INSTANCE);
        // a clone of the answer set
        SPO[] stmts2 = stmts1.clone();

        Vector<SPO> v = new Vector<SPO>();
        // the simplest n^2 algorithm
        for( int i = 0; i < stmts1.length; i++ ) {
            // printStatement(stmts1[i]);
            for ( int j = 0; j < stmts2.length; j++ ) {
                if ( stmts1[i].o == stmts2[j].s ) {
                    v.add( new SPO(stmts1[i].s, p, stmts2[j].o) );
                }
            }
        }
        
        long collectionTime = System.currentTimeMillis() - startTime;
        
        System.out.println( getClass().getName() + " collected " + v.size() + 
                            " intersections in " + collectionTime + " millis" );
        
        System.out.println( getClass().getName() + " number of statements before: " + store.ndx_spo.getEntryCount());
        
        int numAdded = 0;
        
        SPO[] stmts3 = v.toArray(new SPO[v.size()]);
        
        // deal with the SPO index
        Arrays.sort(stmts3,SPOComparator.INSTANCE);
        for ( int i = 0; i < stmts3.length; i++ ) {
            byte[] key = store.keyBuilder.statement2Key
                ( stmts3[i].s, stmts3[i].p, stmts3[i].o
                  );
            if ( !store.ndx_spo.contains(key) ) {
                store.ndx_spo.insert(key, null);
                numAdded++;
            }
        }

        // deal with the POS index
        Arrays.sort(stmts3,POSComparator.INSTANCE);
        for ( int i = 0; i < stmts3.length; i++ ) {
            byte[] key = store.keyBuilder.statement2Key
                ( stmts3[i].p, stmts3[i].o, stmts3[i].s
                  );
            if ( !store.ndx_pos.contains(key) ) {
                store.ndx_pos.insert(key, null);
            }
        }

        // deal with the OSP index
        Arrays.sort(stmts3,OSPComparator.INSTANCE);
        for ( int i = 0; i < stmts3.length; i++ ) {
            byte[] key = store.keyBuilder.statement2Key
                ( stmts3[i].o, stmts3[i].s, stmts3[i].p
                  );
            if ( !store.ndx_osp.contains(key) ) {
                store.ndx_osp.insert(key, null);
            }
        }

        System.out.println( getClass().getName() + " number of statements after: " + store.ndx_spo.getEntryCount());
        
        return numAdded;
        
    }

    private void printStatement( SPO stmt ) {
        
        URI s = (URI) store.ndx_idTerm.lookup(store.keyBuilder.id2key(stmt.s));
         
        URI p = (URI) store.ndx_idTerm.lookup(store.keyBuilder.id2key(stmt.p));
         
        URI o = (URI) store.ndx_idTerm.lookup(store.keyBuilder.id2key(stmt.o));
         
        System.err.println("sco("+abbrev(s)+", "+abbrev(o)+")");
        
    }
    
    private String abbrev( URI uri ) {
        
        return uri.getURI().substring(uri.getURI().lastIndexOf('#'));
        
    }
    
}