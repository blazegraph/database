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

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * A temporary triple store based on the <em>bigdata</em> architecture. Data
 * is buffered in memory but will overflow to disk for large stores.
 * <p>
 * Note: This class is often used to support inference. When so used, the
 * statement indices are populated with the term identifiers from the main
 * database and the ids and terms indices in the {@link TempTripleStore} are NOT
 * used.
 * <p>
 * Note: This class does NOT support {@link #commit()} or {@link #abort()}. If
 * you want an in-memory {@link ITripleStore} that supports commit and abort
 * then use a {@link LocalTripleStore} and specify
 * {@link com.bigdata.journal.Options#BUFFER_MODE} as
 * {@link BufferMode#Transient}.
 * 
 * FIXME examine use of {@link TemporaryStore} vs a {@link Journal} with
 * {@link BufferMode#Temporary}. The latter provides full concurrency control
 * and group commit while the former presumably has less startup costs because
 * it does not have the thread pools for concurrency control.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TempTripleStore extends AbstractLocalTripleStore implements ITripleStore {

    private BTree ndx_term2Id;
    private BTree ndx_id2Term;

    private BTree ndx_spo;
    private BTree ndx_pos;
    private BTree ndx_osp;

    private BTree ndx_just;
    
    private TemporaryStore store;
    
    private final AbstractTripleStore db;
    
    public TemporaryStore getStore() {
        
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

    final public IIndex getTerm2IdIndex() {

        return ndx_term2Id;
        
    }
    
    final public IIndex getId2TermIndex() {

        return ndx_id2Term;
        
    }

    final public IIndex getJustificationIndex() {
        
        return ndx_just;
        
    }
    
    /**
     * NOP - the temporary store does not support commits or aborts.
     */
    final public void commit() {
        
    }
    
    /**
     * NOP - the temporary store does not support commits or aborts.
     */
    final public void abort() {
        
    }
    
    final public boolean isStable() {
        
        return store.isStable();
        
    }
    
    final public boolean isReadOnly() {
        
        return false;
        
    }
    
    final public void clear() {
        
//        if(true) {

            if(lexicon) {

                ndx_term2Id.removeAll();
                ndx_id2Term.removeAll();
                
                if(textIndex) {
                    
                    getSearchEngine().clear();
                    
                }
                
            }
    
            ndx_spo.removeAll();
            
            if(!oneAccessPath) {
             
                ndx_pos.removeAll();
                
                ndx_osp.removeAll();
                
            }
            
            if(justify) {

                ndx_just.removeAll();
                
            }
            
//        } else {        
//
//            if(lexicon) {
//            
//                store.dropIndex(name_idTerm); ndx_termId = null;
//                store.dropIndex(name_termId); ndx_idTerm = null;
//                
//                if(textIndex) {
//                    
////                    store.dropIndex(name_freeText); ndx_freeText = null;
//                    
//                }
//                
//            }
//            
//            store.dropIndex(name_spo); ndx_spo = null;
//            
//            if(!oneAccessPath) {
//            
//                store.dropIndex(name_pos); ndx_pos = null;
//             
//                store.dropIndex(name_osp); ndx_osp = null;
//                
//            }
//            
//            if(justify) {
//
//                store.dropIndex(name_just); ndx_just = null;
//                
//            }
//            
//            createIndices();
//            
//        }
        
    }
    
    final public void close() {
        
        store.close();
        
        super.close();
        
    }
    
    final public void closeAndDelete() {
        
        store.closeAndDelete();
        
        super.closeAndDelete();
        
    }
    
    /**
     * Uses the thread-local {@link RdfKeyBuilder} on the
     * {@link AbstractTripleStore} supplied to the ctor if defined and otherwise
     * creates its own thread-local {@link RdfKeyBuilder}s.
     */
    final public RdfKeyBuilder getKeyBuilder() {
        
        if(db != null) return db.getKeyBuilder();
        
        return super.getKeyBuilder();
        
    }
    
    /**
     * 
     * @todo define options for {@link TemporaryStore} and then extend them
     *       here.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends AbstractTripleStore.Options { //, TemporaryStore.Options {
        
    }
    
    /**
     * Create a transient {@link ITripleStore} backed by a
     * {@link TemporaryStore}.
     * <p>
     * Note: the {@link TempTripleStore} declares its indices that do NOT
     * support isolation. This offers a significant performance boost when you
     * do not need transactions or the ability to purge historical data versions
     * from the store as they age.
     * <p>
     * Note: Users of the {@link TempTripleStore} may find it worthwhile to turn
     * off a variety of options in order to minimize the time and space burden
     * of the temporary store depending on the use which will be made of it.
     * 
     * @param properties
     *            See {@link Options}.
     */
    public TempTripleStore(Properties properties) {
       
        this(properties, null);
        
    }
    
    /**
     * Variant that will reuse the same thread-local {@link RdfKeyBuilder}s
     * exposed by the optional <i>db</i>.
     * 
     * @param properties
     * @param db
     *            A database to which requests for a thread-local
     *            {@link RdfKeyBuilder} will be delegated (optional).
     */
    public TempTripleStore(Properties properties,AbstractTripleStore db) {
        
        super(properties);

        this.db = db;
        
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

        if (lexicon) {

            ndx_term2Id = (BTree) store.registerIndex(name_term2Id, BTree.create(
                    store, getTerm2IdIndexMetadata(name_term2Id)));

            ndx_id2Term = (BTree) store.registerIndex(name_id2Term, BTree.create(
                    store, getId2TermIndexMetadata(name_id2Term)));

        }

        if (oneAccessPath) {

            ndx_spo = (BTree) store.registerIndex(name_spo, BTree.create(store,
                    getStatementIndexMetadata(name_spo)));

        } else {

            ndx_spo = (BTree) store.registerIndex(name_spo, BTree.create(store,
                    getStatementIndexMetadata(name_spo)));

            ndx_pos = (BTree) store.registerIndex(name_pos, BTree.create(store,
                    getStatementIndexMetadata(name_pos)));

            ndx_osp = (BTree) store.registerIndex(name_osp, BTree.create(store,
                    getStatementIndexMetadata(name_osp)));

        }

        if (justify) {

            ndx_just = (BTree) store.registerIndex(name_just, BTree.create(
                    store, getJustIndexMetadata(name_just)));

        }

    }
    
    public String usage(){
        
        return super.usage()+
        ("\nfile="+store.getBufferStrategy().getFile())+
        ("\nbyteCount="+store.getBufferStrategy().getNextOffset())
        ;
        
    }
    
    /**
     * This store is NOT safe for concurrent operations.
     */
    public boolean isConcurrent() {

        return false;
        
    }
    
}
