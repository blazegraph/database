/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.inf.JustificationSerializer;
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

    private BTree ndx_termId;
    private BTree ndx_idTerm;

    private BTree ndx_spo;
    private BTree ndx_pos;
    private BTree ndx_osp;

    private BTree ndx_just;
    
    final private TemporaryStore store;
    
    public TemporaryStore getBackingStore() {
        
        return store;
        
    }

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

    final public IIndex getJustificationIndex() {
        
        return ndx_just;
        
    }
    
    /**
     * NOP.
     */
    final public void commit() {
        
    }
    
    final public boolean isStable() {
        
        return store.isStable();
        
    }
    
    final public void clear() {
        
        /*
         * FIXME probable problem with removeAll failing to clear the hard
         * reference cache in BTree.  if fixed, then also make the indices
         * final.
         */
        
        if(false) {
            
            ndx_termId.removeAll();
            ndx_idTerm.removeAll();
    
            ndx_spo.removeAll();
            ndx_pos.removeAll();
            ndx_osp.removeAll();
            
            ndx_just.removeAll();
            
        } else {        
        
            store.dropIndex(name_idTerm); ndx_termId = null;
            store.dropIndex(name_termId); ndx_idTerm = null;
            
            store.dropIndex(name_spo); ndx_spo = null;
            store.dropIndex(name_pos); ndx_pos = null;
            store.dropIndex(name_osp); ndx_osp = null;
            
            store.dropIndex(name_just); ndx_just = null;
            
            createIndices();
            
        }
        
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

        /*
         * @todo property for how large the temporary store can get in memory
         * before it overflows (Question is how to minimize the time to create
         * the backing file, which adds significant latency - plus the temp
         * store is not as concurrency savvy).
         */
        store = new TemporaryStore();

        createIndices();
        
    }

    private void createIndices() {
        
        ndx_termId = (BTree)store.registerIndex(name_termId, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                TermIdSerializer.INSTANCE));

        ndx_idTerm = (BTree)store.registerIndex(name_idTerm, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                RdfValueSerializer.INSTANCE));

        ndx_spo = (BTree)store.registerIndex(name_spo, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                StatementSerializer.INSTANCE));
        
        ndx_pos = (BTree)store.registerIndex(name_pos, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                StatementSerializer.INSTANCE));
        
        ndx_osp = (BTree)store.registerIndex(name_osp, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                StatementSerializer.INSTANCE));
        
        ndx_just = (BTree) store.registerIndex(name_just, new BTree(store,
                BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                                    JustificationSerializer.INSTANCE));

    }
    
    public void usage(){
        
        System.err.println("file="+store.getBufferStrategy().getFile());
        System.err.println("byteCount="+store.getBufferStrategy().getNextOffset());
        
        super.usage();
        
    }
    
}
