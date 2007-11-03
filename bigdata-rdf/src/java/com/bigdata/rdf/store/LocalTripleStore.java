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

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ConcurrentJournal;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.inf.JustificationSerializer;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.serializers.RdfValueSerializer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.serializers.TermIdSerializer;
import com.bigdata.scaleup.MasterJournal;

/**
 * A triple store based on the <em>bigdata</em> architecture.
 * 
 * @todo remove overflow() support - this will become part of the journal API.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalTripleStore extends AbstractLocalTripleStore implements ITripleStore {

    protected final /*Master*/Journal store;
    
    private final boolean isolatableIndices;
    
    /*
     * At this time is is valid to hold onto a reference during a given load or
     * query operation but not across commits (more accurately, not across
     * overflow() events). Eventually we will have to put a client api into
     * place that hides the routing of btree api operations to the journals and
     * segments for each index partition.
     */
    private IIndex ndx_termId;
    private IIndex ndx_idTerm;
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

        if(ndx_termId!=null) return ndx_termId;
        
        IIndex ndx = store.getIndex(name_termId);
        
        if(ndx==null) {
            
            if (isolatableIndices) {

                ndx_termId = ndx = store.registerIndex(name_termId);

            } else {

                ndx_termId = ndx = store.registerIndex(name_termId, new BTree(
                        store, BTree.DEFAULT_BRANCHING_FACTOR, UUID
                                .randomUUID(), TermIdSerializer.INSTANCE));
            
            }
            
        }
        
        return ndx;
        
    }

    final public IIndex getIdTermIndex() {

        if (ndx_idTerm != null)
            return ndx_idTerm;

        IIndex ndx = store.getIndex(name_idTerm);

        if (ndx == null) {

            if (isolatableIndices) {

                ndx_idTerm = ndx = store.registerIndex(name_idTerm);

            } else {

                ndx_idTerm = ndx = store.registerIndex(name_idTerm, new BTree(
                        store, BTree.DEFAULT_BRANCHING_FACTOR, UUID
                                .randomUUID(), RdfValueSerializer.INSTANCE));

            }

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

            if (isolatableIndices) {

                ndx = store.registerIndex(name);

            } else {

                ndx = store.registerIndex(name, new BTree(store,
                        BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                        StatementSerializer.INSTANCE));

            }

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

            if (isolatableIndices) {

                ndx_just = ndx = store.registerIndex(name_just);

            } else {

                ndx_just = ndx = store
                        .registerIndex(name_just, new BTree(store,
                                BTree.DEFAULT_BRANCHING_FACTOR, UUID
                                        .randomUUID(),
                                JustificationSerializer.INSTANCE));

            }

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

        /*final long commitTime = */ store.commit();

        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("commit: commit latency="+elapsed+"ms");

        if(INFO) usage();
        
//        return commitTime;
        
    }

    final public boolean isStable() {
        
        return store.isStable();
        
    }
    
    final public void clear() {
        
        store.dropIndex(name_idTerm); ndx_termId = null;
        store.dropIndex(name_termId); ndx_idTerm = null;
        
        store.dropIndex(name_spo); ndx_spo = null;
        store.dropIndex(name_pos); ndx_pos = null;
        store.dropIndex(name_osp); ndx_osp = null;
        
        store.dropIndex(name_just); ndx_just = null;
        
    }
    
    final public void close() {
        
        store.shutdown();
        
    }
    
    final public void closeAndDelete() {
        
        store.closeAndDelete();
        
    }
    
    public static class Options extends ConcurrentJournal.Options {
        
        /**
         * When true, the terms, ids, and statement indices will be registered
         * as {@link UnisolatedBTree} and will support transactions. Otherwise
         * the indices will be registered as {@link BTree} and will NOT support
         * isolation by transactions.
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
        
    }
    
    /**
     * @deprecated overflow handling is being moved into the journal.
     */
    protected void didOverflow(Object state) {
        
        // clear hard references to named indices.
        ndx_termId = null;
        ndx_idTerm = null;
        ndx_spo = null;
        ndx_pos = null;
        ndx_osp = null;
        
    }

    public void usage(){
        
        System.err.println("file="+store.getBufferStrategy().getFile());
        System.err.println("byteCount="+store.getBufferStrategy().getNextOffset());
        
        super.usage();
        
    }
    
}
