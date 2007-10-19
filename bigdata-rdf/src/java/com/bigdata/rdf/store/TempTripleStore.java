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
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf.store;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.inf.OSPComparator;
import com.bigdata.rdf.inf.POSComparator;
import com.bigdata.rdf.inf.SPO;
import com.bigdata.rdf.inf.SPOComparator;
import com.bigdata.rdf.serializers.RdfValueSerializer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.serializers.TermIdSerializer;

/**
 * A temporary triple store based on the <em>bigdata</em> architecture. Data
 * is buffered in memory but will overflow to disk for large stores.
 * <p>
 * Note: This class is often used to support inference. When so used, the
 * statement indices are populated with the term identifiers from the main
 * database and the ids and terms indices in the {@link TempTripleStore} are NOT
 * used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TempTripleStore extends AbstractLocalTripleStore implements ITripleStore {

    private final IIndex ndx_termId;
    private final IIndex ndx_idTerm;

    /*
     * Note: You MUST NOT retain hard references to these indices across
     * operations since they may be discarded and re-loaded.
     */
    private final IIndex ndx_spo;
    private final IIndex ndx_pos;
    private final IIndex ndx_osp;

    final private TemporaryStore store;

    final public IIndex getSPOIndex() {

        return ndx_spo;
        
    }
    
    final public IIndex getPOSIndex() {

        return ndx_pos;

    }
    
    final public IIndex getOSPIndex() {
        
        return ndx_osp;

    }

    final public IIndex getTermIdIndex() {

        return ndx_termId;
        
    }
    
    final public IIndex getIdTermIndex() {

        return ndx_idTerm;
        
    }

    /**
     * NOP.
     */
    final public void commit() {
        
    }
    
    final public boolean isStable() {
        
        return store.isStable();
        
    }
    
    final public void close() {
        
        store.close();
        
    }
    
    final public void closeAndDelete() {
        
        store.closeAndDelete();
        
    }
    
    /**
     * Create a transient {@link ITripleStore} backed by a
     * {@link TemporaryStore}.
     * <p>
     * Note: the {@link TempTripleStore} declares its indices as {@link BTree}s
     * (do not support isolation) rather than {@link UnisolatedBTree} (supports
     * transactional isolation and purge of historical data). This offers a
     * significant performance boost when you do not need transactions or the
     * ability to purge historical data versions from the store as they age.
     */
    public TempTripleStore(Properties properties) {

        super(properties);

        store = new TemporaryStore();
        
        ndx_termId = store.registerIndex(name_termId, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                TermIdSerializer.INSTANCE));

        ndx_idTerm = store.registerIndex(name_idTerm, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                RdfValueSerializer.INSTANCE));

        ndx_spo = store.registerIndex(name_spo, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                StatementSerializer.INSTANCE));
        
        ndx_pos = store.registerIndex(name_pos, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                StatementSerializer.INSTANCE));
        
        ndx_osp = store.registerIndex(name_osp, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                StatementSerializer.INSTANCE));
        
    }

    /**
     * Used to efficiently add entailments from the array into the
     * {@link TempTripleStore}.
     * 
     * @param stmts
     *            The source statements.
     * 
     * @param n
     *            The #of statements in the buffer.
     */
    public void addStatements(SPO[] stmts, int n ) {
        
        // deal with the SPO index
        IIndex spo = getSPOIndex();
        Arrays.sort(stmts,0,n,SPOComparator.INSTANCE);
        for ( int i = 0; i < n; i++ ) {
            byte[] key = keyBuilder.statement2Key
                ( stmts[i].s, stmts[i].p, stmts[i].o
                  );
            if ( !spo.contains(key) ) {
                spo.insert(key, null);
            }
        }

        // deal with the POS index
        IIndex pos = getPOSIndex();
        Arrays.sort(stmts,0,n,POSComparator.INSTANCE);
        for ( int i = 0; i < n; i++ ) {
            byte[] key = keyBuilder.statement2Key
                ( stmts[i].p, stmts[i].o, stmts[i].s
                  );
            if ( !pos.contains(key) ) {
                pos.insert(key, null);
            }
        }

        // deal with the OSP index
        IIndex osp = getOSPIndex();
        Arrays.sort(stmts,0,n,OSPComparator.INSTANCE);
        for ( int i = 0; i < n; i++ ) {
            byte[] key = keyBuilder.statement2Key
                ( stmts[i].o, stmts[i].s, stmts[i].p
                  );
            if ( !osp.contains(key) ) {
                osp.insert(key, null);
            }
        }
        
    }
    
}
