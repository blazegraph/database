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

import java.lang.ref.WeakReference;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.model.Value;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IDataSerializer.NoDataSerializer;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.store.IndexWriteProc.FastRDFKeyCompression;
import com.bigdata.rdf.store.IndexWriteProc.FastRDFValueCompression;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.service.LocalDataServiceClient;

/**
 * A triple store based on the <em>bigdata</em> architecture.
 * 
 * @deprecated By {@link ScaleOutTripleStore} when deployed using a
 *             {@link LocalDataServiceClient}.  This option supports high concurrency and
 *             optimized joins (well, I am working on the latter, but when that is done
 *             this will be the one to use for a local deployment).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalTripleStore extends AbstractLocalTripleStore implements ITripleStore {

    protected final Journal store;
    
    protected final boolean isolatableIndices;
    
    /*
     * At this time is is valid to hold onto a reference during a given load or
     * query operation but not across commits (more accurately, not across
     * overflow() events). Eventually we will have to put a client api into
     * place that hides the routing of btree api operations to the journals and
     * segments for each index partition.
     */
    private IIndex ndx_termId;
    private IIndex ndx_idTerm;
    private IIndex ndx_freeText;
    private IIndex ndx_spo;
    private IIndex ndx_pos;
    private IIndex ndx_osp;
    private IIndex ndx_just;
    
    /*
     * Note: At this time it is valid to hold onto a reference during a given
     * load or query operation but not across commits (more accurately, not
     * across overflow() events).
     */
    
    final public IIndex getTermIdIndex() {

        if(!lexicon) return null;
        
        if(ndx_termId!=null) return ndx_termId;
        
        IIndex ndx = store.getIndex(name_termId);
        
        if(ndx==null) {
            
                IndexMetadata metadata = new IndexMetadata(name_termId,UUID.randomUUID());
                
                metadata.setDeleteMarkers(isolatableIndices);

                metadata.setBranchingFactor(store.getDefaultBranchingFactor());
                
                ndx_termId = ndx = store.registerIndex(name_termId,
                        BTree.create(store, metadata)
                );
            
        }
        
        return ndx;
        
    }

    final public IIndex getIdTermIndex() {

        if(!lexicon) return null;

        if (ndx_idTerm != null)
            return ndx_idTerm;

        IIndex ndx = store.getIndex(name_idTerm);

        if (ndx == null) {

            IndexMetadata metadata = new IndexMetadata(name_idTerm,UUID.randomUUID());

            metadata.setDeleteMarkers(isolatableIndices);

            metadata.setBranchingFactor(store.getDefaultBranchingFactor());

            ndx_idTerm = ndx = store.registerIndex(name_idTerm, BTree.create(
                    store, metadata));

        }

        return ndx;

    }

    final public IIndex getFullTextIndex() {

        if(!textIndex) return null;

        if(ndx_freeText!=null) return ndx_freeText;
        
        IIndex ndx = store.getIndex(name_freeText);
        
        if(ndx==null && textIndex) {

            IndexMetadata metadata = new IndexMetadata(name_freeText,UUID.randomUUID());

            metadata.setDeleteMarkers(isolatableIndices);

            metadata.setBranchingFactor(store.getDefaultBranchingFactor());

            metadata.setValueSerializer(NoDataSerializer.INSTANCE);
            
            ndx_freeText = ndx = store.registerIndex(name_freeText, BTree
                    .create(store, metadata));
            
        }
        
        return ndx;
        
    }

    /**
     * Returns and creates iff necessary a scalable restart safe index for RDF
     * {@link _Statement statements}.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The index.
     * 
     * @see #name_spo
     * @see #name_pos
     * @see #name_osp
     */
    private IIndex getStatementIndex(String name) {

        IIndex ndx = store.getIndex(name);

        if (ndx == null) {
                
                IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());
                
                metadata.setDeleteMarkers(isolatableIndices);

                metadata.setBranchingFactor(store.getDefaultBranchingFactor());
                
                metadata.setLeafKeySerializer(FastRDFKeyCompression.N3);
                
                metadata.setValueSerializer(new FastRDFValueCompression());
                
                ndx = store.registerIndex(name, BTree.create(store, metadata));

        }

        return ndx;

    }

    final public IIndex getSPOIndex() {

        if(ndx_spo!=null) return ndx_spo;

        return ndx_spo = getStatementIndex(name_spo);
        
    }
    
    final public IIndex getPOSIndex() {

        if(ndx_pos!=null) return ndx_pos;

        return ndx_pos = getStatementIndex(name_pos);

    }
    
    final public IIndex getOSPIndex() {
        
        if(ndx_osp!=null) return ndx_osp;

        return ndx_osp = getStatementIndex(name_osp);

    }

    final public IIndex getJustificationIndex() {

        if (ndx_just != null)
            return ndx_just;

        IIndex ndx = store.getIndex(name_just);

        if (ndx == null) {

            IndexMetadata metadata = new IndexMetadata(name_just,UUID.randomUUID());

            metadata.setDeleteMarkers(isolatableIndices);

            metadata.setBranchingFactor(store.getDefaultBranchingFactor());

            metadata.setValueSerializer(NoDataSerializer.INSTANCE);
            
            ndx_just = ndx = store.registerIndex(name_just, BTree.create(store,
                    metadata));

        }

        return ndx;

    }

    /**
     * The backing embedded database.
     */
    final public IJournal getJournal() {
        
        return store;
        
    }
    
    /**
     * Delegates the operation to the backing store.
     */
    final public void commit() {
     
        final long begin = System.currentTimeMillis();

        store.commit();

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("commit: commit latency="+elapsed+"ms");

        if(INFO) log.info("\n"+usage());
        
    }

    final public void abort() {
        
        // discard the write sets.
        
        store.abort();
        
        /*
         * Discard the hard references to the indices since they may have
         * uncommitted writes. The indices will be re-loaded from the store the
         * next time they are used.
         */
        
        ndx_termId = null;
        ndx_idTerm = null;
        ndx_freeText = null;
        ndx_spo = null;
        ndx_pos = null;
        ndx_osp = null;
        ndx_just = null;
        
    }
    
    final public boolean isStable() {
        
        return store.isStable();
        
    }
    
    final public boolean isReadOnly() {
        
        return store.isReadOnly();
        
    }

    final public void clear() {
        
        store.dropIndex(name_idTerm); ndx_termId = null;
        store.dropIndex(name_termId); ndx_idTerm = null;
        if(store.getIndex(name_freeText)!=null) {
            
            store.dropIndex(name_freeText); ndx_freeText = null;
            
        }
        
        store.dropIndex(name_spo); ndx_spo = null;
        store.dropIndex(name_pos); ndx_pos = null;
        store.dropIndex(name_osp); ndx_osp = null;
        
        if(store.getIndex(name_just)!=null) {

            store.dropIndex(name_just); ndx_just = null;
            
        }
        
        registerIndices();
        
    }
    
    final public void close() {
        
        super.close();
        
        store.shutdown();
        
    }
    
    final public void closeAndDelete() {
        
        super.closeAndDelete();
        
        store.closeAndDelete();
        
    }
    
    public static interface Options extends AbstractTripleStore.Options {
        
        /**
         * When <code>true</code>, the terms, ids, and statement indices will
         * support isolation and maintain version metadata. Version metadata is
         * required for both transactions and scale-out indices. Otherwise the
         * indices will NOT support isolation by transactions.
         * 
         * @deprecated For now, this just turns on the use of delete markers in
         *             the indices. That is only interesting if you want to
         *             measure the performance impact of delete markers on the
         *             index. This does NOT provide transactional isolation.
         *             That is not available for the {@link LocalTripleStore}
         *             since it does not use the concurrency control API.
         */
        public static final String ISOLATABLE_INDICES = "isolatableIndices";

        public static final String DEFAULT_ISOLATABLE_INDICES = "false";
        
    }
    
    /**
     * Create or re-open a triple store using a local embedded database.
     */
    public LocalTripleStore(Properties properties) {

        super(properties);
        
        store = new /*Master*/Journal(properties);

        isolatableIndices = Boolean
                .parseBoolean(properties.getProperty(
                        Options.ISOLATABLE_INDICES,
                        Options.DEFAULT_ISOLATABLE_INDICES));

        log.info(Options.ISOLATABLE_INDICES+"="+isolatableIndices);
        
        registerIndices();
        
    }
    
    /**
     * Note: This may be used to force eager registration of the indices such
     * that they are always on hand for {@link #asReadCommittedView()}.
     */
    protected void registerIndices() {

        getTermIdIndex();
        
        getIdTermIndex();
        
        getSPOIndex();
        
        getPOSIndex();
        
        getOSPIndex();
        
        getJustificationIndex();
        
        /*
         * Note: A commit is required in order for a read-committed view to have
         * access to the register indices.
         */
        
        commit();
        
    }

    public String usage(){
        
        return super.usage() + store.getCounters();
        
    }

    private WeakReference<ReadCommittedTripleStore> readCommittedRef;
    
    /**
     * A factory returning the singleton read-committed view of the database.
     */
    public ReadCommittedTripleStore asReadCommittedView() {

        synchronized(this) {
        
            ReadCommittedTripleStore view = readCommittedRef == null ? null
                    : readCommittedRef.get();
            
            if(view == null) {
                
                view = new ReadCommittedTripleStore(this);
                
                readCommittedRef = new WeakReference<ReadCommittedTripleStore>(view);
                
            }
            
            return view; 
        
        }
        
    }
    
    /**
     * This store is NOT safe for concurrent operations.
     */
    public boolean isConcurrent() {

        return false;
        
    }

    /**
     * A read-committed view of a read-write triple store. Data committed on the
     * read-write triple store will become visible in this view. The view does
     * NOT support any mutation operations.
     * 
     * @todo The {@link ScaleOutTripleStore} uses unisolated writes so it always
     *       has read-committed semantics so it does not make sense to implement
     *       a read-committed view for that class. However, the manner in which
     *       it chooses the read-behavior point MAY change in order to support
     *       long-running data load operations.
     *
     * @see LocalTripleStore#asReadCommittedView()
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ReadCommittedTripleStore extends AbstractLocalTripleStore {

        private final LocalTripleStore db;
        
        /**
         * Create a read-committed view of a read-write triple store.
         * 
         * @param db
         *            The read-write triple store.
         */
        protected ReadCommittedTripleStore(LocalTripleStore db) {

            super(db.getProperties());
        
            this.db = db;
            
        }

        /**
         * True iff the backing database is stable.
         */
        public boolean isStable() {

            return db.isStable();
            
        }

        /**
         * The view is read-only, but it is updated each time there is a commit
         * against the database (read-committed semantics).
         */
        public boolean isReadOnly() {
            
            return true;
            
        }
        
        /**
         * @throws UnsupportedOperationException always.
         */
        public void abort() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void clear() {

            throw new UnsupportedOperationException();
            
        }

        /** NOP */
        public void close() {
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void closeAndDelete() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void commit() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public long addTerm(Value value) {

            throw new UnsupportedOperationException();
            
        }

        /**
         * This will resolve terms that are pre-existing but will throw an
         * exception if there is an attempt to define a new term.
         * 
         * @throws UnsupportedOperationException
         *             if there is an attempt to write on an index.
         */
        public void addTerms(RdfKeyBuilder keyBuilder, _Value[] terms, int numTerms) {

            super.addTerms(keyBuilder, terms, numTerms);
            
        }

//        private IIndex ndx_termId;
//        private IIndex ndx_idTerm;
//        private IIndex ndx_freeText;
//        private IIndex ndx_spo;
//        private IIndex ndx_pos;
//        private IIndex ndx_osp;
//        private IIndex ndx_just;

        public IIndex getTermIdIndex() {
            
//            if (ndx_termId == null) {

            return db.store.getIndex(name_termId, ITx.READ_COMMITTED);

//            }
            
//            return ndx_termId;
            
        }

        public IIndex getIdTermIndex() {
        
            return db.store.getIndex(name_idTerm, ITx.READ_COMMITTED);

//            if(ndx_idTerm==null) {
//                
//                ndx_idTerm = new ReadCommittedIndex(db.store,name_idTerm);
//                
//            }
//            
//            return ndx_idTerm;

        }

        public IIndex getFullTextIndex() {

            return db.store.getIndex(name_freeText, ITx.READ_COMMITTED);

//            if(ndx_freeText==null && textIndex) {
//                
//                ndx_freeText= new ReadCommittedIndex(db.store,name_freeText);
//                
//            }
//            
//            return ndx_freeText;
            
        }

        public IIndex getSPOIndex() {

            return db.store.getIndex(name_spo, ITx.READ_COMMITTED);

//            if(ndx_spo ==null) {
//                
//                ndx_spo = new ReadCommittedIndex(db.store,name_spo);
//                
//            }
//            
//            return ndx_spo;

        }
        
        public IIndex getPOSIndex() {

            return db.store.getIndex(name_pos, ITx.READ_COMMITTED);
//
//            if(ndx_pos ==null) {
//                
//                ndx_pos = new ReadCommittedIndex(db.store,name_pos);
//                
//            }
//            
//            return ndx_pos;

        }

        public IIndex getOSPIndex() {

            return db.store.getIndex(name_osp, ITx.READ_COMMITTED);

//            if(ndx_osp ==null) {
//                
//                ndx_osp = new ReadCommittedIndex(db.store,name_osp);
//                
//            }
//            
//            return ndx_osp;

        }

        public IIndex getJustificationIndex() {

            return db.store.getIndex(name_just, ITx.READ_COMMITTED);

//            if(ndx_just ==null) {
//                
//                ndx_just = new ReadCommittedIndex(db.store,name_just);
//                
//            }
//            
//            return ndx_just;

        }

        /**
         * This store is safe for concurrent operations (but it only supports
         * read operations).
         */
        public boolean isConcurrent() {

            return true;
            
        }
        
    }

}
