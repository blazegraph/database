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
import com.bigdata.journal.Tx;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.serializers.RdfValueSerializer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.serializers.TermIdSerializer;
import com.bigdata.scaleup.MasterJournal;

/**
 * A triple store based on the <em>bigdata</em> architecture.
 * 
 * @todo define two versions of this - one that uses overflow and one that does
 *       not?
 * 
 * @todo refactor the rdf application to use the client-data service divide and
 *       compare performance with the embedded database (ideally both will
 *       continue to run so that they may be directly compared), support journal
 *       overflow (sync and then async) and view in the data service (assembled
 *       from journal(s) and index segment(s)) so that we can do very large data
 *       loads (refactor out of the scale-up package), tune the forward chainer
 *       to remove more redundency, add flag for entailments vs assertions vs
 *       told triples so that they can be identified in the KB, move to a quad
 *       store model, support scalable joins, test on large lubm data set.
 * 
 * @todo Refactor to support transactions and concurrent load/query and test
 *       same.
 *       <p>
 *       Conflicts arise in the bigdata-RDF store when concurrent transactions
 *       attempt to define the same term. The problem arises because on index is
 *       used to map the term to an unique identifier and another to map the
 *       identifiers back to terms. Further, the statement indices use term
 *       identifiers directly in their keys. Therefore, resolving concurrent
 *       definition of the same term requires that we either do NOT isolate the
 *       writes on the term indices (which is probably an acceptable strategy)
 *       or that we let the application order the pass over the isolated indices
 *       and give the conflict resolver access to the {@link Tx} so that it can
 *       update the dependent indices if a conflict is discovered on the terms
 *       index.
 *       <p>
 *       The simplest approach appears to be NOT isolating the terms and ids
 *       indices. As long as the logic resides at the index, e.g., a lambda
 *       expression/method, to assign the identifier and create the entry in the
 *       ids index we can get buy with less isolation. If concurrent processes
 *       attempt to define the same term, then one or the other will wind up
 *       executing first (writes on indices are single threaded) and the result
 *       will be coherent as long as the write is committed before the ids are
 *       returned to the application. It simply does not matter which process
 *       defines the term since all that we care about is atomic, consistent,
 *       and durable. This is a case where group commit would work well (updates
 *       are blocked together on the server automatically to improve
 *       throughput).
 *       <p>
 *       Concurrent assertions of the same statement cause write-write
 *       conflicts, but they are trivially resolved -- we simply ignore the
 *       write-write conflict since both transactions agree on the statement
 *       data. Unlike the term indices, isolation is important for statements
 *       since we want to guarentee that a set of statements either is or is not
 *       asserted atomically. (With the terms index, we could care less as long
 *       as the indices are coherent.)
 *       <p>
 *       The only concern with the statement indices occurs when one transaction
 *       asserts a statement and a concurrent transaction deletes a statement. I
 *       need to go back and think this one through some more and figure out
 *       whether or not we need to abort a transaction in this case.
 * 
 * @todo bnodes do not need to be store in the terms or ids indices if we
 *       presume that an unknown identifier is a bnode. however, we still need
 *       to ensure that bnode identifiers are distinct or the same when and
 *       where appropriate, so we need to assign identifiers to bnodes in a
 *       restart-safe manner even if we "forget" the term-id mapping. (The
 *       possibility of an incomplete ids index during data load for the
 *       scale-out solution means that we must either read from a historical
 *       known consistent timestamp or record bnodes in the terms index.)
 * 
 * @todo the only added cost for a quad store is the additional statement
 *       indices. There are only three more statement indices in a quad store.
 *       Since statement indices are so cheap, it is probably worth implementing
 *       them now, even if only as a configuration option. (There may be reasons
 *       to maintain both versions.)
 * 
 * @todo verify read after commit (restart safe) for large data sets (multiple
 *       index partitions for scale-out and overflow for scale-out/scale-up).
 * 
 * @todo test re-load rate for a data set and verify that no new statements are
 *       added when re-loading a data set.
 * 
 * @todo add bulk data export (buffering statements and bulk resolving term
 *       identifiers).
 * 
 * @todo possibly save frequently seen terms in each batch for the next batch in
 *       order to reduce unicode conversions.
 * 
 * @todo support metadata about the statement, e.g., whether or not it is an
 *       inference. consider that we may need to move the triple/quad ids into
 *       the value in the statement indices since some key compression schemes
 *       are not reversable (we depend on reversable keys to extract the term
 *       ids for a statement).
 * 
 * @todo Try a variant in which we have metadata linking statements and terms
 *       together. In this case we would have to go back to the terms and update
 *       them to have metadata about the statement. it is a bit circular since
 *       we can not create the statement until we have the terms and we can not
 *       add the metadata to the terms until we have the statement.
 * 
 * @todo examine role for semi joins for a Sesame 2.x integration (quad store
 *       with real query operators). semi-joins (join indices) can be declared
 *       for various predicate combinations and then maintained. The
 *       declarations can be part of the scale-out index metadata. The logic
 *       that handles batch data load can also maintain the join indices. While
 *       triggers could be used for this purpose, there would need to be a means
 *       to aggregate and order the triggered events and then redistribute them
 *       against the partitions of the join indices. If the logic is in the
 *       client, then we need to make sure that newly declared join indices are
 *       fully populated (e.g., clients are notified to start building the join
 *       index and then we start the index build from existing data to remove
 *       any chance that the join index would be incomplete - the index would be
 *       ready as soon as the index build completes and client operations would
 *       be in a maintenance role).
 * 
 * @todo provide option for closing aspects of the entire store vs just a single
 *       context in a quad store vs just a "document" before it is loaded into a
 *       triple store (but with the term identifiers of the triple store). For
 *       example, in an open web and internet scale kb it is unlikely that you
 *       would want to have all harvested ontologies closed against all the
 *       data. however, that might make more sense in a more controlled setting.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalTripleStore extends AbstractLocalTripleStore implements ITripleStore {

    protected final /*Master*/Journal store;
    
    private final boolean isolatableIndices;
    
    /*
     * Note: You MUST NOT retain hard references to these indices across
     * operations since they may be discarded and re-loaded.
     */
    private IIndex ndx_termId;
    private IIndex ndx_idTerm;
    private IIndex ndx_spo;
    private IIndex ndx_pos;
    private IIndex ndx_osp;
    
    /*
     * Note: At this time is is valid to hold onto a reference during a given
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

    /**
     * The backing embedded database.
     */
    final public IJournal getJournal() {
        
        return store;
        
    }
    
    /**
     * Delegates the opertion to the backing store.
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
     * FIXME This class needs to listen for an {@link MasterJournal#overflow()} event
     * and clear its index references when one occurs. Failure to do this means
     * that a journal overflow event will cause the index references to become
     * bad (the will refer to the old, closed journal).
     * 
     * @param state
     */
    protected void didOverflow(Object state) {
        
        // clear hard references to named indices.
        ndx_termId = null;
        ndx_idTerm = null;
        ndx_spo = null;
        ndx_pos = null;
        ndx_osp = null;
        
    }
    
}
