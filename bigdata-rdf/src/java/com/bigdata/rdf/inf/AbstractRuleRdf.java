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

import org.openrdf.model.URI;


public abstract class AbstractRuleRdf extends Rule {

    public AbstractRuleRdf(InferenceEngine store, Triple head, Pred[] body) {

        super(store, head, body);

    }
    
    public int apply() {

        // long startTime = System.currentTimeMillis();
        
        SPO[] entailments = collectEntailments();
/*        
        long collectionTime = System.currentTimeMillis() - startTime;
        
        System.out.println( getClass().getName() + " collected " + 
                            entailments.length + " entailments in " + 
                            collectionTime + " millis" );

        int numStmtsBefore = store.ndx_spo.getEntryCount(); 
        
        System.out.println( getClass().getName() + 
                            " number of statements before: " + 
                            numStmtsBefore);
        
        startTime = System.currentTimeMillis();
*/        
        int numAdded = insertEntailments( entailments );
/*
        long insertionTime = System.currentTimeMillis() - startTime;
        
        int numStmtsAfter = store.ndx_spo.getEntryCount();
        
        System.out.println( getClass().getName() + 
                            " number of statements after: " + 
                            numStmtsAfter);
        
        System.out.println( getClass().getName() + 
                            " inserted " + ( numStmtsAfter - numStmtsBefore ) +
                            " statements in " + insertionTime + " millis");
*/        
        return numAdded;
        
    }

    protected abstract SPO[] collectEntailments();
        
    protected int insertEntailments( SPO[] entailments ) {
        
        int numAdded = 0;
        
        // deal with the SPO index
        Arrays.sort(entailments,SPOComparator.INSTANCE);
        for ( int i = 0; i < entailments.length; i++ ) {
            byte[] key = store.keyBuilder.statement2Key
                ( entailments[i].s, entailments[i].p, entailments[i].o
                  );
            if ( !store.ndx_spo.contains(key) ) {
                store.ndx_spo.insert(key, null);
                numAdded++;
            }
        }

        // deal with the POS index
        Arrays.sort(entailments,POSComparator.INSTANCE);
        for ( int i = 0; i < entailments.length; i++ ) {
            byte[] key = store.keyBuilder.statement2Key
                ( entailments[i].p, entailments[i].o, entailments[i].s
                  );
            if ( !store.ndx_pos.contains(key) ) {
                store.ndx_pos.insert(key, null);
            }
        }

        // deal with the OSP index
        Arrays.sort(entailments,OSPComparator.INSTANCE);
        for ( int i = 0; i < entailments.length; i++ ) {
            byte[] key = store.keyBuilder.statement2Key
                ( entailments[i].o, entailments[i].s, entailments[i].p
                  );
            if ( !store.ndx_osp.contains(key) ) {
                store.ndx_osp.insert(key, null);
            }
        }

        return numAdded;
        
    }
    
    protected void printStatement( SPO stmt ) {
        
        URI s = (URI) store.ndx_idTerm.lookup(store.keyBuilder.id2key(stmt.s));
         
        URI p = (URI) store.ndx_idTerm.lookup(store.keyBuilder.id2key(stmt.p));
         
        URI o = (URI) store.ndx_idTerm.lookup(store.keyBuilder.id2key(stmt.o));
         
        System.err.println(abbrev(s)+","+abbrev(p)+","+abbrev(o));
        
    }
    
    protected String abbrev( URI uri ) {
        
        return uri.getURI().substring(uri.getURI().lastIndexOf('#'));
        
    }
    
}