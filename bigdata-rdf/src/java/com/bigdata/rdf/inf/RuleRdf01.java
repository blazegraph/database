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

import com.bigdata.btree.IEntryIterator;
import com.bigdata.rdf.KeyOrder;
import com.bigdata.rdf.TempTripleStore;


public class RuleRdf01 extends AbstractRuleRdf {

    public RuleRdf01(InferenceEngine store, Var u, Var v, Var x) {

        super(store, new Triple(v, store.rdfType, store.rdfProperty), //
                new Pred[] { //
                new Triple(u, v, x)
                });

    }

    public Stats apply( final Stats stats, final SPO[] buffer, TempTripleStore btree ) {
        
        final long computeStart = System.currentTimeMillis();
        
        long lastP = -1;
        
        int n = 0;
        
        IEntryIterator it = store.getPOSIndex().rangeIterator(null,null); 
        
        while ( it.hasNext() ) {
            
            it.next();
            
            stats.stmts1++;
            
            SPO stmt = new SPO(KeyOrder.POS, store.keyBuilder, it.getKey());
            
            if ( stmt.p != lastP ) {
                
                lastP = stmt.p;

                buffer[n++] = new SPO(stmt.p, store.rdfType.id,
                        store.rdfProperty.id);
                
                if (n == buffer.length) {

                    insertStatements(buffer, n, btree);
                    
                    n = 0;
                    
                }
                
                stats.numComputed++;
                
            }
            
        }
        
        insertStatements( buffer, n, btree );
        
        stats.computeTime += System.currentTimeMillis() - computeStart;
        
        return stats;
        
    }
/*
    public long[] collectPredicates2() {

        Vector<Long> predicates = new Vector<Long>();
        
        IEntryIterator it = store.ndx_termId.rangeIterator
            ( store.keyBuilder.uriStartKey(),
              store.keyBuilder.uriEndKey()
              );
        
        while ( it.hasNext() ) {
            
            it.next();
            
            long id = (Long)it.getValue();
            
            // Value v = (Value) store.ndx_idTerm.lookup(store.keyBuilder.id2key(id));
            
            int numStmts = store.ndx_pos.rangeCount
                (store.keyBuilder.statement2Key(id, 0, 0), 
                 store.keyBuilder.statement2Key(id+1, 0, 0)
                 );
            
            if ( numStmts > 0 ) {
                
                // System.err.println(((URI)v).getURI() + " : " + numStmts );
            
                predicates.add( id );
                
            }
            
        }
        
        int i = 0;
        
        long[] longs = new long[predicates.size()];
        
        for ( Iterator<Long> it2 = predicates.iterator(); it2.hasNext(); ) {
            
            longs[i++] = it2.next();
            
        }
        
        return longs;
        
    }
    
    private void countLiterals() {
        
        System.out.println( "number of literals: " + 
        store.ndx_termId.rangeCount
            ( store.keyBuilder.litStartKey(),
              store.keyBuilder.litEndKey()
              )
            );

        IEntryIterator it = store.ndx_termId.rangeIterator
            ( store.keyBuilder.litStartKey(),
              store.keyBuilder.litEndKey()
              );
        
        while ( it.hasNext() ) {
            
            it.next();
            
            long id = (Long)it.getValue();
            
            Value v = (Value) store.ndx_idTerm.lookup(store.keyBuilder.id2key(id));
            
            System.err.println(((Literal)v).getLabel());
            
        }
        
    }
*/
}