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

public abstract class AbstractRuleRdf extends Rule {
    
    public AbstractRuleRdf(InferenceEngine store, Triple head, Pred[] body) {

        super(store, head, body);

    }
    
//    /**
//     * Copies the statements into the primary store.
//     * 
//     * @todo refactor for common code and #of statements parameters with
//     *       {@link #dumpBuffer(SPO[], int, TempTripleStore)}, which copies the
//     *       statements into the temporary store.
//     * 
//     * @param entailments
//     *            The statements.
//     * 
//     * @return The #of statements actually added to the store.
//     */
//    protected int insertEntailments( SPO[] entailments ) {
//        
//        int numAdded = 0;
//        
//        // deal with the SPO index
//        IIndex spo = store.getSPOIndex();
//        Arrays.sort(entailments,SPOComparator.INSTANCE);
//        for ( int i = 0; i < entailments.length; i++ ) {
//            byte[] key = store.keyBuilder.statement2Key
//                ( entailments[i].s, entailments[i].p, entailments[i].o
//                  );
//            if ( !spo.contains(key) ) {
//                spo.insert(key, null);
//                numAdded++;
//            }
//        }
//
//        // deal with the POS index
//        IIndex pos = store.getPOSIndex();
//        Arrays.sort(entailments,POSComparator.INSTANCE);
//        for ( int i = 0; i < entailments.length; i++ ) {
//            byte[] key = store.keyBuilder.statement2Key
//                ( entailments[i].p, entailments[i].o, entailments[i].s
//                  );
//            if ( !pos.contains(key) ) {
//                pos.insert(key, null);
//            }
//        }
//
//        // deal with the OSP index
//        IIndex osp = store.getOSPIndex();
//        Arrays.sort(entailments,OSPComparator.INSTANCE);
//        for ( int i = 0; i < entailments.length; i++ ) {
//            byte[] key = store.keyBuilder.statement2Key
//                ( entailments[i].o, entailments[i].s, entailments[i].p
//                  );
//            if ( !osp.contains(key) ) {
//                osp.insert(key, null);
//            }
//        }
//
//        return numAdded;
//        
//    }
    
//    protected int insertEntailments2( TempTripleStore entailments ) {
//        
//        return insertEntailments( getStatements( entailments ) );
//        
//    }

//    protected TempTripleStore convert( SPO[] stmts ) {
//        
//        TempTripleStore tts = new TempTripleStore();
//        
//        for ( int i = 0; i < stmts.length; i++ ) {
//            
//            tts.addStatement( stmts[i].s, stmts[i].p, stmts[i].o );
//            
//        }
//        
//        return tts;
//        
//    }
    
//    /**
//     * Extracts all statements in the store into an {@link SPO}[].
//     * 
//     * @param store
//     *            The store.
//     * 
//     * @return The array of statements.
//     */
//    protected SPO[] getStatements( TempTripleStore store ) {
//        
//        SPO[] stmts = new SPO[store.getStatementCount()];
//        
//        int i = 0;
//        
//        IIndex ndx_spo = store.getSPOIndex();
//        
//        IEntryIterator it = ndx_spo.rangeIterator(null, null);
//        
//        while ( it.hasNext() ) {
//            
//            it.next();
//            
//            stmts[i++] = new SPO(KeyOrder.SPO, store.keyBuilder, it.getKey());
//            
//        }
//        
//        return stmts;
//        
//    }
    
}