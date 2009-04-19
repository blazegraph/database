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
 * Created on Jan 27, 2007
 */

package com.bigdata.rdf.rio;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.KVOTermIdComparator;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.lexicon.Term2IdWriteTask;
import com.bigdata.rdf.lexicon.WriteTaskStats;
import com.bigdata.rdf.lexicon.Id2TermWriteProc.Id2TermWriteProcConstructor;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOIndexWriteProc;
import com.bigdata.rdf.spo.SPOIndexWriter;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOTupleSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedUnboundedChunkBuffer;
import com.bigdata.service.IScaleOutClientIndex;
import com.bigdata.service.ndx.pipeline.DefaultDuplicateRemover;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * The asynchronous statement buffer w/o SIDs is much simpler. If we require
 * that the document is fully buffered in memory, then we can simplify this to
 * just:
 * 
 * <pre>
 * 
 * Given:
 * 
 * value[] - RDF Values observed in the S,P,O, or C positions.
 * 
 * statement[] - RDF Statements reported by the parser.
 * 
 * Do:
 * 
 * value[] =&gt; TERM2ID (Sync RPC, assigning TIDs)
 * 
 * value[] =&gt; ID2TERM (Async)
 * 
 * value[] =&gt; Text (Async, iff enabled)
 * 
 *  statement[] =&gt; (SPO,POS,OSP) (Async)
 * </pre>
 * 
 * Note: This DOES NOT support truth maintenance. Truth maintenance requires
 * that the term identifiers are resolved against the database's lexicon while
 * the statements are written onto a local (and temporary) triple store. There
 * is no (or at least less) reason to use asynchronous writes against a local
 * store.
 * <p>
 * Note: This DOES NOT support SIDS.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Write a variant which does support SIDs. This should be asynchronous
 * all the way down but it will have to wait on synchronous RPC for the TERM2ID
 * index for SIDs _regardless_ of whether there are dependencies among the
 * statements since statements MUST be assigned consistent SIDS before they may
 * be written onto the statement indices. Review the code that is already
 * handling this for better async efficiencies.
 * 
 * <pre>
 * AsynchronousStatementBufferWithSids:
 * 
 * When SIDs are enabled, we must identify the minimum set of statements
 * whose SIDs are referenced by blank nodes in the S, P, O positions of
 * other statements.  Since we can not make that determination until we
 * reach the end of the document, all statements which use blank nodes
 * are placed into the deferredStatements container.
 * 
 * Further, and unlike the synchronous StatementBuffer, we must defer
 * writes of grounded statements until we know whether or not their SID
 * appears in a blank node reference by another statement.  We MUST use
 * synchronous RPC to obtain the SIDs for those statements.  This means
 * that the entire document MUST be parsed into memory.  Since we must
 * buffer the entire document in memory when SIDs are enabled (when using
 *  asynchronous writes), distinct implementations of the asynchronous
 *  statement buffer are used depending on whether or not SIDs are
 * enabled.
 * 
 * Once the end of the document has been reached, we iteratively divide
 * the parsed statements into three collections.  This process ends once
 * all three collections are empty.
 * 
 *    1. groundedStatements : These are statements which are not
 *       referenced by other statements using their SID and which do not
 *       contain references to the SIDs of other statements. The
 *       groundedStatements are written asynchronously since there is no
 *       dependency on their SIDs.
 * 
 *    2. referencedStatements : These are statements whose SID has not
 *       been assigned yet and which do not reference other statements
 *       but which are themselves referenced by other statements using a
 *       blank node. These statements are written using synchronous RPC
 *       so that we may obtain their SIDs and thereby convert one or more
 *       deferredStatements to either groundedStatements or
 *       referencedStatements.
 * 
 *    3. deferredStatements : These are statements using a blank node to
 *       reference another statement whose SID has not been assigned yet.
 *       These statements MAY also be referenced by other deferred
 *       statements.  However, those references MAY NOT form a cycle.
 *       Deferred statements are moved to either the groundedStatements
 *       or the referencedStatements collection once their blank node
 *       references have been assigned SIDs.
 * 
 * Given:
 * 
 * value[] - RDF Values observed in the S, P, O, and C positions.
 * 
 * unresolvedRefs[] - RDF blank nodes observed in the C position are
 *            entered into this collection.  They are removed
 *            from the collection as they are resolved.
 * 
 * statement[] - RDF Statements reported by the parser.
 * 
 * Do:
 * 
 * // remove blank nodes serving as SIDs from the value[].
 * value[] := value[] - unresolvedRef[];
 * 
 * value[] =&gt; TERM2ID (Sync RPC, assigning TIDs)
 * 
 * value[] =&gt; ID2TERM (Async)
 * 
 * value[] =&gt; Text (Async, iff enabled)
 * 
 * // initially, all statements are deferred.
 * deferredStatements := statements;
 * 
 * while(!groundedStatements.isEmpty() &amp;&amp; !referencedStatements.isEmpty()
 *    &amp;&amp; !deferredStatements.isEmpty()) {
 * 
 *   groundedStatement[] =&gt; TERM2ID (async)
 * 
 *   groundedStatement[] := []; // empty.
 * 
 *   referencedStatement[] =&gt; TERM2ID (Sync RPC, assigning SIDs)
 * 
 *   foreach spo : referencedStatements {
 * 
 *     unresolvedRefs.remove( spo.c );
 * 
 *   }
 * 
 *   referencedStatement[] := []; // empty.
 * 
 *   foreach spo : deferredStatement[i] {
 * 
 *      if(spo.isGrounded) {
 * 
 *         // true iff S.tid, P.tid, and O.tid are bound, implying that
 *         // this statement does not have any unresolved references to
 *         // other statements.
 * 
 *     if(unresolvedReferences.contains(spo.c)) {
 * 
 *         // will be written synchronously.
 *         referencedStatements.add( spo );
 * 
 *     } else {
 * 
 *         // will be written asynchronously.
 *         groundedStatement.add( spo );
 * 
 *     }
 * 
 *      }
 * 
 *   }
 * 
 * }
 * </pre>
 * 
 * @todo evaluate this approach for writing on a local triple store. if there is
 *       a performance benefit then refactor accordingly (requires asynchronous
 *       write API for BTree and friends).
 * 
 * @todo This is more chunk oriented than the {@link StatementBuffer}. In
 *       particular, it only sorts chunks or things rather than everything that
 *       was processed (the only exception right now is for the TERM2ID index,
 *       but that should probably be fixed). This implies that merge sorts in
 *       the chunk combiner are going to play more of a role since we are not
 *       doing a full sort. Things are broken out this way in order to prevent
 *       very large chunks of Values from being written out on the indices. This
 *       should lead to more even performance.
 */
public class AsynchronousStatementBufferWithoutSids<S extends BigdataStatement>
        implements IStatementBuffer<S> {

    final protected transient static Logger log = Logger
            .getLogger(AsynchronousStatementBufferWithoutSids.class);

    final protected transient boolean INFO = log.isInfoEnabled();

    final protected transient boolean DEBUG = log.isDebugEnabled();
    
    private final AsynchronousWriteConfiguration<S> asynchronousWriteConfiguration;
    
    private final AbstractTripleStore database;
    
    private final BigdataValueFactory valueFactory;
    
    /**
     * A canonicalizing map for RDF {@link Value}s. The use of this map
     * provides a ~40% performance gain.
     */
    private LinkedHashMap<Value, BigdataValue> values;

    /**
     * A canonicalizing map for blank nodes. This map MUST be cleared before you
     * begin to add statements to the buffer from a new "source" otherwise it
     * will co-reference blank nodes from distinct sources. The life cycle of
     * the map is the life cycle of the document being loaded, so if you are
     * loading a large document with a lot of blank nodes the map will also
     * become large.
     */
    private Map<String, BigdataBNodeImpl> bnodes;
    
    /**
     * The total #of parsed statements so far.
     * 
     * {@link IBuffer}
     */
    private int statementCount;

    /**
     * Buffer used to accumulate chunks of statements.
     */
    private UnsynchronizedUnboundedChunkBuffer<S> statements;
    
    public final AbstractTripleStore getDatabase() {
        
        return database;
        
    }

    /**
     * Returns <code>null</code>.
     * <p>
     * Note: This implementation does not support the concept of a focusStore so
     * it can not be used for truth maintenance.
     */
    public AbstractTripleStore getStatementStore() {
        
        return null;
        
    }

    public boolean isEmpty() {
        
        return statementCount == 0;
        
    }
    
    public int size() {
        
        return statementCount;
        
    }

    /**
     * 
     */
    protected AsynchronousStatementBufferWithoutSids(
            final AsynchronousWriteConfiguration<S> asynchronousWriteConfiguration) {

        if (asynchronousWriteConfiguration == null)
            throw new IllegalArgumentException();

        this.asynchronousWriteConfiguration = asynchronousWriteConfiguration;
        
        this.database = asynchronousWriteConfiguration.tripleStore;
        
        this.valueFactory = database.getValueFactory();
        
    }

    /**
     * Signals the end of a source and causes all buffered statements to be
     * written. The source limits the scope within which blank nodes are
     * co-referenced by their IDs.
     * <p>
     * Note: This implementation uses synchronous RPC to write on the TERM2ID
     * index and asynchronous writes on the other indices (ID2TERM, Text (if
     * enabled), SPO, POS, OSP).
     * <p> 
     * Note: this implementation always returns ZERO (0).
     */
    public long flush() {
 
        log.info("");

        try {

            // write the data.
            write();
            
        } catch (Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        // discard all buffer state (including bnodes and deferred statements).
        reset();
        
        return 0L;

    }
    
    /**
     * Clears all buffered data, including the canonicalizing mapping for blank
     * nodes and deferred provenance statements.
     */
    public void reset() {
        
        log.info("");
        
        /*
         * Note: clear the reference NOT the contents of the map! This makes it
         * possible for the caller to reuse the same map across multiple
         * StatementBuffer instances.
         */
        bnodes = null;
        
        values = null;
        
        statements = null;
        
        statementCount = 0;

    }
    
    public void setBNodeMap(final Map<String, BigdataBNodeImpl> bnodes) {
    
        if (bnodes == null)
            throw new IllegalArgumentException();
        
        if (this.bnodes != null)
            throw new IllegalStateException();
        
        this.bnodes = bnodes;
        
    }
    
    /**
     * Add an "explicit" statement to the buffer (flushes on overflow, no
     * context).
     * 
     * @param s
     * @param p
     * @param o
     */
    public void add(Resource s, URI p, Value o) {
        
        add(s, p, o, null, StatementEnum.Explicit);
        
    }
    
    /**
     * Add an "explicit" statement to the buffer (flushes on overflow).
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     */
    public void add(Resource s, URI p, Value o, Resource c) {
        
        add(s, p, o, c, StatementEnum.Explicit);
        
    }
    
    /**
     * Add a statement to the buffer (core impl).
     */
    public void add(final Resource s, final URI p, final Value o,
            final Resource c, final StatementEnum type) {
        
        // add to the buffer.
        handleStatement(s, p, o, c, type);

    }
    
    public void add(final S e) {

        add(e.getSubject(), e.getPredicate(), e.getObject(), e.getContext(),
                (e instanceof BigdataStatement ? ((BigdataStatement) e)
                        .getStatementType() : null));

    }
    
    /**
     * Canonicalizing mapping for blank nodes.
     * <p>
     * Note: This map MUST stay in effect while reading from a given source and
     * MUST be cleared (or set to null) before reading from another source.
     */
    private BigdataBNodeImpl getCanonicalBNode(final BigdataBNodeImpl bnode) {
        
        // the BNode's ID.
        final String id = bnode.getID();

        if (bnodes == null) {

            /*
             * Allocate a canonicalizing map for blank nodes. Since this will be
             * a private map it does not need to be thread-safe.
             */
            bnodes = new HashMap<String, BigdataBNodeImpl>(
                    asynchronousWriteConfiguration.bnodesInitialCapacity);

            // fall through.
            
        }

        /*
         * Specialized for a concurrent hash map.
         */
        if (bnodes instanceof ConcurrentHashMap) {

            final BigdataBNodeImpl tmp = ((ConcurrentHashMap<String, BigdataBNodeImpl>) bnodes)
                    .putIfAbsent(id, bnode);
            
            if(tmp != null) {

                // already exists in the map.
                return tmp;
                
            }

            if (DEBUG)
                log.debug("added: " + bnode);

            // was inserted into the map.
            return bnode;

        }

        /*
         * Synchronized on the map to make the conditional insert atomic.
         */
        synchronized (bnodes) {

            final BigdataBNodeImpl tmp = bnodes.get(id);

            if (tmp != null) {

                // already exists in the map.
                return tmp;

            }

            // insert this blank node into the map.
            bnodes.put(id, bnode);

            if (DEBUG)
                log.debug("added: " + bnode);

            // was inserted into the map.
            return bnode;

        } // synchronized

    }
    
    /**
     * Canonicalizing mapping for a term.
     * <p>
     * Note: Blank nodes are made canonical with the scope of the source from
     * which the data are being read. See {@link #bnodes}. All other kinds of
     * terms are made canonical within the scope of the buffer's current
     * contents in order to keep down the demand on the heap with reading either
     * very large documents or a series of small documents.
     * 
     * @param term
     *            A term.
     * 
     * @return Either the term or the pre-existing term in the buffer with the
     *         same data.
     */
    private BigdataValue getCanonicalValue(final BigdataValue term0) {

        if (term0 == null) {

            // Note: This handles an empty context position.
            return term0;
            
        }
        
        final BigdataValue term;

        if (term0 instanceof BNode) {

            // impose canonicalizing mapping for blank nodes.
            term = getCanonicalBNode((BigdataBNodeImpl) term0);

            /*
             * Fall through.
             * 
             * Note: This also records the blank node in the values map so that
             * we can process the values map without having to consider the
             * blank nodes as well.
             */

        } else {
            
            // not a blank node.
            term = term0;
            
        }

        if (values == null) {
         
            /*
             * Create a private (non-thread safe) canonicalizing mapping for RDF
             * Values.
             * 
             * Note: A linked hash map is used to make the iterator faster.
             */

            values = new LinkedHashMap<Value, BigdataValue>(
                    asynchronousWriteConfiguration.valuesInitialCapacity);

        }

        /*
         * Impose a canonicalizing mapping on the term.
         */
        
        final BigdataValue tmp = values.get(term);
        
        if (tmp != null) {

            // already exists.
            return tmp;

        }

        // add to the map.
        if (values.put(term, term) != null) {

            throw new AssertionError();

        }

        if (DEBUG)
            log.debug("n=" + values.size() + ", added: " + term);
        
        // return the new term.
        return term;

    }
    
    /**
     * Adds the values and the statement into the buffer.
     * 
     * @param s
     *            The subject.
     * @param p
     *            The predicate.
     * @param o
     *            The object.
     * @param c
     *            The context (may be null).
     * @param type
     *            The statement type.
     * 
     * @throws IndexOutOfBoundsException
     *             if the buffer capacity is exceeded.
     * 
     * @see #nearCapacity()
     */
    private void handleStatement(final Resource s, final URI p, final Value o,
            final Resource c, final StatementEnum type) {

        _handleStatement(
                (Resource) getCanonicalValue((BigdataResource)valueFactory.asValue(s)),//
                (URI) getCanonicalValue((BigdataURI)valueFactory.asValue(p)),//
                (Value) getCanonicalValue((BigdataValue)valueFactory.asValue(o)),//
                (Resource) getCanonicalValue((BigdataResource)valueFactory.asValue(c)), //
                type);

    }

    /**
     * Form the BigdataStatement object using the valueFactory now that we
     * bindings which were (a) allocated by the valueFactory and (b) are
     * canonical for the scope of this document.
     */
    private void _handleStatement(final Resource s, final URI p, final Value o,
            final Resource c, final StatementEnum type) {

        final BigdataStatement stmt = valueFactory.createStatement(
                (BigdataResource) s, (BigdataURI) p, (BigdataValue) o,
                (BigdataResource) c, type);

        if (statements == null) {
            
            statements = new UnsynchronizedUnboundedChunkBuffer<S>(
                    asynchronousWriteConfiguration.statementChunkCapacity);
            
        }
        
        statements.add((S) stmt);
        
        // total #of statements accepted.
        statementCount++;

        if (DEBUG)
            log.debug("n=" + statementCount + ", added: " + stmt);

    }

    /**
     * This does all the work. The write on the TERM2ID index uses synchronous
     * RPC so that we have all the TIDs (term identifiers) on hand before we
     * begin to write on the remaining indices. The rest of the indices use the
     * asynchronous write API. This method does not return until the
     * asynchronous writes have been <em>buffered</em>.
     * 
     * @throws Exception 
     */
    private void write() throws Exception {

        if (INFO) {
            log.info("bnodeCount=" + (bnodes == null ? 0 : bnodes.size())
                    + ", values=" + values.size() + ", statementCount="
                    + statementCount);
        }
        
        /*
         * Synchronous RPC.
         * 
         * Note: This is responsible for assigning the TIDs (term identifiers)
         * to the {@link BigdataValue}s. We CAN NOT write on the other indices
         * until we have those TIDs.
         */
        {

            // @todo should feed in a chunked iterator.
            
            // dense array of the distinct terms.
            final BigdataValue[] values = this.values.values().toArray(
                    new BigdataValue[0]);

            final KVO<BigdataValue>[] a = new Term2IdWriteTask(
                    asynchronousWriteConfiguration.lexiconRelation,
                    false/* readOnly */, values.length, values,
                    new WriteTaskStats()).call();
            
        }

        /*
         * Setup tasks which can run asynchronously. These tasks have no
         * dependencies. They can each proceed at their own rate. However, we
         * can not return from within this method until they are all done.
         * 
         * Note: Each task runs in parallel.
         * 
         * Note: Each task uses the asynchronous write API. When the Future for
         * that task is complete all it means is that the data are now buffered
         * on the asynchronous write buffer for the appropriate index. It DOES
         * NOT mean that those writes are complete.
         * 
         * Note: These tasks all process iterators. This approach was choosen to
         * isolate the tasks (which queue data for asynchronous writes) from the
         * data structures in this IStatementBuffer implementation. An example
         * of something which WOULD NOT work is if these tasks were inner
         * classes accessing the instance fields on this class since reset()
         * would clear those fields which might cause spontaneous failures
         * within ongoing processing.
         */
        final List<Callable> tasks = new LinkedList<Callable>();

        tasks.add(new AsyncId2TermIndexWriteTask(valueFactory, newId2TIterator(
                values.values().iterator(),
                asynchronousWriteConfiguration.id2termChunkCapacity),
                asynchronousWriteConfiguration.buffer_id2t));

        if (asynchronousWriteConfiguration.buffer_text != null) {

            // FIXME full text index.
            throw new UnsupportedOperationException();
//            tasks.add(new AsyncTextWriteTask());

        }

        tasks.add(new AsyncSPOIndexWriteTask(SPOKeyOrder.SPO,
                asynchronousWriteConfiguration.spoRelation,
//                (IChunkedOrderedIterator<ISPO>) 
                statements.iterator(),
                asynchronousWriteConfiguration.buffer_spo));

        if (asynchronousWriteConfiguration.buffer_pos != null) {

            tasks.add(new AsyncSPOIndexWriteTask(SPOKeyOrder.POS,
                    asynchronousWriteConfiguration.spoRelation,
//                    (IChunkedOrderedIterator<ISPO>) 
                    statements.iterator(),
                    asynchronousWriteConfiguration.buffer_pos));

        }

        if (asynchronousWriteConfiguration.buffer_osp != null) {

            tasks.add(new AsyncSPOIndexWriteTask(SPOKeyOrder.OSP,
                    asynchronousWriteConfiguration.spoRelation,
//                    (IChunkedOrderedIterator<ISPO>) 
                    statements.iterator(),
                    asynchronousWriteConfiguration.buffer_osp));

        }

        /*
         * Submit all tasks. They will run in parallel. If they complete
         * successfully then all we know is that the data has been buffered for
         * asynchronous writes on the various indices.
         * 
         * Note: java 1.6.0_07/12 build problems under linux when typed as
         * <Future> or any other combination that I have tried.
         */
        final List futures = asynchronousWriteConfiguration.tripleStore.getExecutorService()
                .invokeAll(tasks);

        // make sure that no errors were reported by those tasks.
        for (Object f : futures) {

            ((Future)f).get();

        }
        
    }

    /**
     * Wrap a {@link BigdataValue}[] with a chunked iterator which filters out
     * blank nodes (blank nodes are not written onto the reverse index).
     */
    @SuppressWarnings("unchecked")
    static <V extends BigdataValue> IChunkedIterator<V> newId2TIterator(
            final Iterator<V> itr, final int chunkSize) {

        return new ChunkedWrappedIterator(new Striterator(itr)
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    /*
                     * Filter hides blank nodes since we do not write them onto
                     * the reverse index.
                     */
                    @Override
                    protected boolean isValid(Object obj) {

                        return !(obj instanceof BNode);
                        
                    }

                }), chunkSize, BigdataValue.class);

    }
    
    /**
     * Asynchronous writes on the ID2TERM index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class AsyncId2TermIndexWriteTask implements Callable<Void> {

        final protected transient static Logger log = Logger
                .getLogger(AsyncId2TermIndexWriteTask.class);

        final protected transient boolean INFO = log.isInfoEnabled();

        final protected transient boolean DEBUG = log.isDebugEnabled();

        private final BigdataValueFactory valueFactory;

        private final IChunkedIterator<BigdataValue> src;

        private final BlockingBuffer<KVO<BigdataValue>[]> buffer;

        /**
         * 
         * @param src
         *            The visits chunks of distinct {@link Value}s with their
         *            TIDs assigned. Blank nodes will automatically be filtered
         *            out.
         */
        public AsyncId2TermIndexWriteTask(
                final BigdataValueFactory valueFactory,
                final IChunkedIterator<BigdataValue> src,
                final BlockingBuffer<KVO<BigdataValue>[]> buffer) {

            this.valueFactory = valueFactory;

            this.src = src;
            
            this.buffer = buffer;

        }
        
        public Void call() throws Exception {

            // used to serialize the Values for the BTree.
            final BigdataValueSerializer<BigdataValue> ser = valueFactory.getValueSerializer();
            
            // thread-local key builder removes single-threaded constraint.
            final IKeyBuilder tmp = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);

            // buffer is reused for each serialized term.
            final DataOutputBuffer out = new DataOutputBuffer();

            while (src.hasNext()) {

                final BigdataValue[] chunkIn = src.nextChunk();

                final KVO<BigdataValue>[] chunkOut = new KVO[chunkIn.length];

                int i = 0;

                for (BigdataValue v : chunkIn) {

                    assert v != null;
                    
                    if(v instanceof BNode) {

                        // Do not write blank nodes on the reverse index.
                        continue;
                        
                    }
                    
                    if (v.getTermId() == IRawTripleStore.NULL) {

                        throw new RuntimeException("No TID: " + v);
                        
                    }

                    final byte[] key = tmp.reset().append(v.getTermId())
                            .getKey();

                    // Serialize the term.
                    final byte[] val = ser.serialize(v, out.reset());

                    chunkOut[i++] = new KVO<BigdataValue>(key, val, v);
                    
                }

                // make dense.
                final KVO<BigdataValue>[] dense = KVO.dense(chunkOut, i);

                /*
                 * Put into term identifier order in preparation for writing on
                 * the reverse index.
                 */
                Arrays.sort(dense, 0, dense.length,
                        KVOTermIdComparator.INSTANCE);

                // add chunk to async write buffer
                buffer.add(dense);
                
            }

            // Done.
            return null;
            
        }
        
    }
    
    /**
     * Writes the statement chunks onto the specified statement index using the
     * asynchronous write API.
     * <p>
     * Note: This is similar to the {@link SPOIndexWriter}, but the latter uses
     * synchronous RPC.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class AsyncSPOIndexWriteTask implements Callable<Void> {

        final protected transient static Logger log = Logger
                .getLogger(AsyncSPOIndexWriteTask.class);

        final protected transient boolean INFO = log.isInfoEnabled();

        final protected transient boolean DEBUG = log.isDebugEnabled();

        private final IKeyOrder<ISPO> keyOrder;

        /* Note: problem with java 1.6.0_07 and _12 on linux when typed. */
        private final IChunkedOrderedIterator/*<ISPO>*/ src;

        private final BlockingBuffer<KVO<ISPO>[]> writeBuffer;

        private final SPOTupleSerializer tupleSer;
        
        public AsyncSPOIndexWriteTask(
                final IKeyOrder<ISPO> keyOrder,
                final SPORelation spoRelation,
                /* Note: problem with java 1.6.0_07 and _12 on linux when typed. */
                final IChunkedOrderedIterator/*<ISPO>*/ src,
                final BlockingBuffer<KVO<ISPO>[]> writeBuffer) {

            if (keyOrder == null)
                throw new IllegalArgumentException();

            if (writeBuffer == null)
                throw new IllegalArgumentException();
            
            this.keyOrder = keyOrder;

            this.src = src;
            
            this.writeBuffer = writeBuffer;
            
            // the tuple serializer for this access path.
            this.tupleSer = (SPOTupleSerializer) spoRelation.getIndex(keyOrder)
                    .getIndexMetadata().getTupleSerializer();
            
        }
        
        public Void call() throws Exception {

            long chunksOut = 0;
            long elementsOut = 0;
            
            final ByteArrayBuffer vbuf = new ByteArrayBuffer(1+8/*max length*/);

            while(src.hasNext()) {

                // next chunk, in the specified order.
                final ISPO[] chunk = (ISPO[])src.nextChunk(keyOrder);

                // note: a[] will be dense since nothing is filtered.
                final KVO<ISPO>[] a = new KVO[chunk.length];

                for (int i = 0; i < chunk.length; i++) {
                    
                    final ISPO spo = chunk[i];
                    
                    if (spo == null)
                        throw new IllegalArgumentException();

                    if (!spo.isFullyBound())
                        throw new IllegalArgumentException("Not fully bound: "
                                + spo.toString());

                    // generate key for the index.
                    final byte[] key = tupleSer.statement2Key(keyOrder, spo);

                    // generate value for the index.
                    final byte[] val = spo.serializeValue(vbuf);

                    a[i] = new KVO<ISPO>(key, val, spo);

                }

                // put chunk into sorted order based on assigned keys.
                Arrays.sort(a);

                // write chunk on the buffer.
                writeBuffer.add(a);

                chunksOut++;
                elementsOut += a.length;

                if (DEBUG)
                    log.debug("Wrote chunk: index=" + keyOrder + ", chunksOut="
                            + chunksOut + ", elementsOut=" + elementsOut
                            + ", chunkSize=" + a.length);

                if (log.isTraceEnabled())
                    log.trace("Wrote: index=" + keyOrder + ", chunk="
                            + Arrays.toString(a));

            }

            if (DEBUG)
                log.debug("Done: index=" + keyOrder + ", chunksOut="
                        + chunksOut + ", elementsOut=" + elementsOut);
            
            // done.
            return null;

        }
        
    }

    /**
     * Factory interface for asynchronous writers on an {@link ITripleStore}.
     * 
     * @todo add a factory method returning an interface or abstract class
     *       provides whatever configuration is relevant given the
     *       {@link ScaleOutTripleStore} and also provides a factory for
     *       {@link IStatementBuffer}s based on that configuration.
     */
    public static interface IAsynchronousWriteConfiguration<S extends Statement>
            extends IStatementBufferFactory<S> {

        /**
         * Return a new {@link IStatementBuffer} which may be used to bulk load
         * RDF data. The writes will proceed asynchronously using buffers shared
         * by all {@link IStatementBuffer}s returned by this factory for a
         * given instance of this class. Each {@link IStatementBuffer} MAY be
         * recycled using {@link IBuffer#reset()} or simply discarded after its
         * use.
         */
        public IStatementBuffer<S> newStatementBuffer();

        /**
         * Cancel all {@link Future}s.
         * 
         * @param mayInterruptIfRunning
         */
        public void cancelAll(final boolean mayInterruptIfRunning);

        /**
         * Close the {@link BlockingBuffer}s and await their {@link Future}s.
         * 
         * @throws ExecutionException
         *             if any {@link Future} fails.
         * 
         * @throws InterruptedException
         *             if interrupted while awaiting any of the {@link Future}s.
         */
        public void awaitAll() throws InterruptedException, ExecutionException;
        
    }

    /**
     * Configuration object specifies the {@link BlockingBuffer}s which will be
     * used to write on each of the indices and the reference for the TERM2ID
     * index since we will use synchronous RPC on that index. The same
     * {@link AsynchronousWriteConfiguration} may be used for multiple
     * concurrent {@link AsynchronousStatementBufferWithoutSids} instances.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo this is already setup such that it would be easy to parameterize
     *       for TM.
     */
    public static class AsynchronousWriteConfiguration<S extends BigdataStatement> implements
            IAsynchronousWriteConfiguration<S> {
       
        private final ScaleOutTripleStore tripleStore;
        
        private final LexiconRelation lexiconRelation;

        private final SPORelation spoRelation;
        
        /**
         * The initial capacity of the canonicalizing mapping for RDF
         * {@link Value}.
         */
        final int valuesInitialCapacity = 100000 * 3;

        /**
         * The initial capacity of the canonicalizing mapping for RDF
         * {@link BNode}s.
         */
        final int bnodesInitialCapacity = 10000;
        
        /**
         * The chunk size for statements.
         * <p>
         * Note: This directly governs the size of the {@link KVO}[] chunks
         * written onto the {@link BlockingBuffer}s for the statement indices.
         */
        final int statementChunkCapacity;

        /**
         * The chunk size for ID2TERM async writes.
         */
        final int id2termChunkCapacity;

        final int indexWriteQueueCapacity;

        final int indexPartitionWriteQueueCapacity;

        private final BlockingBuffer<KVO<BigdataValue>[]> buffer_id2t;
        private final BlockingBuffer<KVO<BigdataValue>[]> buffer_text;
        private final BlockingBuffer<KVO<ISPO>[]> buffer_spo;
        private final BlockingBuffer<KVO<ISPO>[]> buffer_pos;
        private final BlockingBuffer<KVO<ISPO>[]> buffer_osp;
        
        private final LongAggregator statementResultHandler = new LongAggregator();

        public IStatementBuffer<S> newStatementBuffer() {

            return new AsynchronousStatementBufferWithoutSids<S>(this);

        }

        /**
         * Return an estimate of the #of statements written on the indices.
         * <p>
         * This value is aggregated across any {@link IStatementBuffer} obtained
         * from {@link #newStatementBuffer()} for this instance.
         * <p>
         * This value actually reports the #of statements written on the SPO
         * index for the database. Statements are written asynchronously in
         * chunks and the writes MAY proceed at different rates for each of the
         * statement indices. The counter value will be stable once the
         * {@link #awaitAll()} returns normally.
         */
        public long getStatementCount() {

            return statementResultHandler.getResult().longValue();
            
        }

        public AsynchronousWriteConfiguration(final ScaleOutTripleStore tripleStore,
                final int writeChunkSize) {

            if (tripleStore == null)
                throw new IllegalArgumentException();
            
            this.tripleStore = tripleStore;
            
            this.lexiconRelation = tripleStore.getLexiconRelation();

            this.spoRelation = tripleStore.getSPORelation();

            this.id2termChunkCapacity=writeChunkSize;
            
            this.statementChunkCapacity = writeChunkSize;
            
            this.indexWriteQueueCapacity = writeChunkSize;
            
            this.indexPartitionWriteQueueCapacity = writeChunkSize;
            
            if (tripleStore.isStatementIdentifiers()) {

                throw new UnsupportedOperationException("SIDs not supported");
                
            }
            
            this.buffer_id2t = ((IScaleOutClientIndex) lexiconRelation
                    .getId2TermIndex())
                    .newWriteBuffer(
                            indexWriteQueueCapacity,
                            indexPartitionWriteQueueCapacity,
                            null/* resultHandler */,
                            new DefaultDuplicateRemover<BigdataValue>(true/* testRefs */),
                            Id2TermWriteProcConstructor.INSTANCE);

            if (tripleStore.getLexiconRelation().isTextIndex()) {

                /*
                 * FIXME text index. This can be partly handled by factoring out
                 * a filter to be applied to a striterator. However, true async
                 * writes need to reach into the full text index package.
                 * Probably there should be a KVO ctor which knows how to form
                 * the key and value from the object for a given index, e.g.,
                 * using the tupleSerializer. I could probably clean things up
                 * enourmously in that manner and just write filters rather than
                 * custom glue for sync and async index writes.
                 */
                throw new UnsupportedOperationException();
//                this.buffer_text = ((IScaleOutClientIndex) lexiconRelation.getId2TermIndex())
//                        .newWriteBuffer(
//                                indexWriteQueueCapacity,
//                                indexPartitionWriteQueueCapacity,
//                                resultHandler,
//                                new DefaultDuplicateRemover<BigdataValue>(true/* testRefs */),
//                                ctor);
                
            } else {
                
                this.buffer_text = null;
                
            }

            this.buffer_spo = ((IScaleOutClientIndex) spoRelation.getSPOIndex())
                    .newWriteBuffer(
                            indexWriteQueueCapacity,
                            indexPartitionWriteQueueCapacity,
                            statementResultHandler,
                            new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                            SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

            if (!tripleStore.getSPORelation().oneAccessPath) {

                this.buffer_pos = ((IScaleOutClientIndex) spoRelation.getPOSIndex())
                        .newWriteBuffer(
                                indexWriteQueueCapacity,
                                indexPartitionWriteQueueCapacity,
                                null/* resultHandler */,
                                new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                                SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

                this.buffer_osp = ((IScaleOutClientIndex) spoRelation.getOSPIndex())
                        .newWriteBuffer(
                                indexWriteQueueCapacity,
                                indexPartitionWriteQueueCapacity,
                                null/* resultHandler */,
                                new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                                SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

            } else {

                this.buffer_pos = null;

                this.buffer_osp = null;
                
            }

        }
        
        public void cancelAll(final boolean mayInterruptIfRunning) {

            if(log.isInfoEnabled())
                log.info("Cancelling futures.");

            buffer_id2t.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_text != null)
                buffer_text.getFuture().cancel(mayInterruptIfRunning);

            buffer_spo.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_pos != null)
                buffer_pos.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_osp != null)
                buffer_osp.getFuture().cancel(mayInterruptIfRunning);

        }

        /**
         * Close buffers and then await their {@link Future}s. Once closed, the
         * buffers will not accept further input.
         */
        public void awaitAll() throws InterruptedException, ExecutionException {

            if(log.isInfoEnabled())
                log.info("Closing buffers.");
            
            buffer_id2t.close();
            
            if (buffer_text != null)
                buffer_text.close();

            buffer_spo.close();
            
            if (buffer_pos != null)
                buffer_pos.close();
            
            if (buffer_osp != null)
                buffer_osp.close();

            /*
             * Await futures.
             */

            if(log.isInfoEnabled())
                log.info("Awaiting futures.");

            buffer_id2t.getFuture().get();

            if (buffer_text != null)
                buffer_text.getFuture().get();

            buffer_spo.getFuture().get();

            if (buffer_pos != null)
                buffer_pos.getFuture().get();

            if (buffer_osp != null)
                buffer_osp.getFuture().get();

        }

    }

}
