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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.IAsyncResultHandler;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer;
import com.bigdata.rdf.lexicon.Term2IdWriteProc;
import com.bigdata.rdf.lexicon.Id2TermWriteProc.Id2TermWriteProcConstructor;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer.LexiconKeyBuilder;
import com.bigdata.rdf.lexicon.Term2IdWriteProc.Term2IdWriteProcConstructor;
import com.bigdata.rdf.lexicon.Term2IdWriteTask.AssignTermId;
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
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IRunnableBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedUnboundedChunkBuffer;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.IScaleOutClientIndex;
import com.bigdata.service.ndx.pipeline.DefaultDuplicateRemover;
import com.bigdata.service.ndx.pipeline.KVOC;
import com.bigdata.service.ndx.pipeline.KVOLatch;
import com.bigdata.service.ndx.pipeline.KVOList;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
 * store. However, TM could use this to copy the data from the temporary triple
 * store to the database. This should be plugged in transparently in the
 * copyStatements() API for the tripleStore.
 * <p>
 * Note: This DOES NOT support SIDS.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME Write a variant which does support SIDs. This should be
 *          asynchronous all the way down but it will have to wait on
 *          synchronous RPC for the TERM2ID index for SIDs _regardless_ of
 *          whether there are dependencies among the statements since statements
 *          MUST be assigned consistent SIDS before they may be written onto the
 *          statement indices. Review the code that is already handling this for
 *          better async efficiencies.
 * 
 *          <pre>
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
 * asynchronous writes), distinct implementations of the asynchronous
 * statement buffer are used depending on whether or not SIDs are
 * enabled. [Actually, we fully buffer anyway so we can use the same
 * implementation class.]
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
 */
public class AsynchronousStatementBufferWithoutSids2<S extends BigdataStatement,F>
        implements IStatementBuffer<S> {

    final protected transient static Logger log = Logger
            .getLogger(AsynchronousStatementBufferWithoutSids2.class);

    private final AsynchronousWriteBufferFactoryWithoutSids2<S,F> statementBufferFactory;
    
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
     * Return the identifier for the document.
     */
    public F getDocumentIdentifier() {

        // FIXME pass value into the ctor via newStatementBuffer().
//        throw new UnsupportedOperationException();
        return null;
        
    }
    
    /**
     * 
     */
    protected AsynchronousStatementBufferWithoutSids2(
            final AsynchronousWriteBufferFactoryWithoutSids2<S,F> asynchronousWriteConfiguration) {

        if (asynchronousWriteConfiguration == null)
            throw new IllegalArgumentException();

        this.statementBufferFactory = asynchronousWriteConfiguration;
        
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

            statementBufferFactory.lock.lock();
            try {
                statementBufferFactory.unfinishedDocumentCount
                        .incrementAndGet();
            } finally {
                statementBufferFactory.lock.unlock();
            }

            // write the data.
            write();
            
        } catch (Exception ex) {

            statementBufferFactory.lock.lock();
            try {
                if(statementBufferFactory.unfinishedDocumentCount
                        .decrementAndGet()==0) {
                    statementBufferFactory.documentsAllDone.signalAll();
                }
            } finally {
                statementBufferFactory.lock.unlock();
            }
        
            throw new RuntimeException(ex);
            
        }
        
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
                    statementBufferFactory.bnodesInitialCapacity);

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

            if (log.isDebugEnabled())
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

            if (log.isDebugEnabled())
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
                    statementBufferFactory.valuesInitialCapacity);

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

        if (log.isDebugEnabled())
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
                    statementBufferFactory.producerChunkSize);
            
        }
        
        statements.add((S) stmt);
        
        // total #of statements accepted.
        statementCount++;

        if (log.isDebugEnabled())
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

        if (log.isInfoEnabled()) {
            log.info("bnodeCount=" + (bnodes == null ? 0 : bnodes.size())
                    + ", values=" + values.size() + ", statementCount="
                    + statementCount);
        }

        if (statementBufferFactory.isAnyDone()) {

            throw new RuntimeException("Factory closed?");

        }

        /*
         * Note: This is responsible for assigning the TIDs (term identifiers)
         * to the {@link BigdataValue}s. We CAN NOT write on the other indices
         * until we have those TIDs.
         * 
         * Run task which will queue BigdataValue[] chunks onto the TERM2ID
         * async write buffer.
         * 
         * Note: If there is not enough load being placed the async index write
         * then it can wait up to its idle/chunk timeout. Normally we want to
         * use an infinite chunk timeout so that all chunks written on the index
         * partitions are as full as possible. Therefore, the TERM2ID async
         * writer should use a shorter idle timeout or it can live lock.
         * Ideally, there should be some explicit notice when we are done
         * queuing writes on TERM2ID across all source documents. Even then we
         * can live lock if the input queue is not large enough.
         */

        /*
         * Latch notifies us when all writes for _this_ document on TERM2ID are
         * complete such that we have the assigned term identifiers for all
         * BigdataValues appearing in the document. This event is used to
         * transfer the document to another queue.
         */
        final KVOLatch tidsLatch = new KVOLatch() {

            public String toString() {
                
                return super.toString()+" : tidsLatch";
                
            }

            @Override
            protected void signal() throws InterruptedException {

                super.signal();

                // add to _unbounded_ workQueue.
                statementBufferFactory.bufferIndexWritesService
                        .submit(new BufferIndexWritesTask());

                // #of documents whose TIDs have been assigned.
                statementBufferFactory.tidsReadyCount.incrementAndGet();

                statementBufferFactory.lock.lock();
                try {
                    // no longer waiting on TIDs for this document.
                    if (statementBufferFactory.tidsWaitingCount
                            .decrementAndGet() == 0) {
                        statementBufferFactory.tidsAllDone.signalAll();
                    }
                    /*
                     * Increment the #of documents ready to write on the
                     * remaining indices.
                     */
                    statementBufferFactory.documentsPendingWritesCount
                            .incrementAndGet();
                } finally {
                    statementBufferFactory.lock.unlock();
                }
                
                if (log.isInfoEnabled()) {
                    log.info("Latch done: " + "tidsReady="
                            + statementBufferFactory.tidsReadyCount
                            + ", tidsWaiting="
                            + statementBufferFactory.tidsWaitingCount.get());
                }

            }
            
        };

        // pre-increment to avoid notice on transient zeros.
        tidsLatch.inc();

        try {

            final Callable<Void> task = new AsyncTerm2IdIndexWriteTask(
                    tidsLatch, statementBufferFactory.lexiconRelation,
                    newT2IdIterator(values.values().iterator(),
                            statementBufferFactory.producerChunkSize),
                    statementBufferFactory.buffer_t2id);

            statementBufferFactory.lock.lock();
            try {
                // Now waiting on TIDs for this document.
                statementBufferFactory.tidsWaitingCount.incrementAndGet();
            } finally {
                statementBufferFactory.lock.unlock();
            }

            // queue chunks onto the write buffer.
            task.call();

            // done parsing this document.
            statementBufferFactory.documentsParsedCount.incrementAndGet();

        } finally {

            /*
             * Decrement now that all chunks have been queued for asynchronous
             * writes.
             */

            tidsLatch.dec();

        }

        // At this point the writes on TERM2ID have been buffered.
        
    }

    /**
     * Task which buffers index writes for the remaining indices (everything
     * other than TERM2ID).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class BufferIndexWritesTask implements Callable<Void> {
        
        public Void call() throws Exception {

            try {

                writeOnRemainingIndices();

            } catch (Throwable t) {

                statementBufferFactory.documentErrorCount.incrementAndGet();

                statementBufferFactory.lock.lock();
                try {
                    if (statementBufferFactory.unfinishedDocumentCount
                            .decrementAndGet() == 0) {
                        statementBufferFactory.documentsAllDone.signalAll();
                    }
                } finally {
                    statementBufferFactory.lock.unlock();
                }

                /*
                 * Note: log error since no one watches the future for this
                 * task.
                 */
                
                log.error(this,t);
                
                throw new Exception(t);
                
            }
            
            return null;
            
        }
        
    }

    /**
     * Buffers write requests for the remaining indices (everything except
     * TERM2ID).
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void writeOnRemainingIndices() throws InterruptedException,
            ExecutionException {

        if(log.isDebugEnabled()) {
            
            log.debug("Writing on remaining indices.");
            
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
         * NOT mean that those writes are complete. However, the
         * [documentStableLatch] DOES indicate when the data is restart safe.
         * 
         * Note: These tasks all process iterators. This approach was chosen to
         * isolate the tasks (which queue data for asynchronous writes) from the
         * data structures in this IStatementBuffer implementation. An example
         * of something which WOULD NOT work is if these tasks were inner
         * classes accessing the instance fields on this class since reset()
         * would clear those fields which might cause spontaneous failures
         * within ongoing processing.
         */
        final List<Callable> tasks = new LinkedList<Callable>();

        /*
         * The #of triples parsed from this document. This is added to the total
         * #of restart safe told triples loaded by this client when the latch is
         * triggered. Of course, the actual #of triples on the database is only
         * available by querying the database since the same triple can occur in
         * more than one document, and documents are loaded by distributed
         * clients so there is no way to correct for such duplicate told triples
         * short of querying the database.
         */
        final int toldTriplesThisDocument = statementCount;
        
        /*
         * Latch is signaled when all data buffered for this document is RESTART
         * SAFE on the database.
         * 
         * Note: In order for the latch to have those semantics we have to
         * include it on each KVO object buffered for all remaining indices. The
         * semantics are valid in the presence of duplicate removes IFF they
         * obey the contract for KVOList and link together the duplicates such
         * that the latch is decremented for each distinct KVOC instance,
         * including those which were eliminated as duplicates.
         */
        final KVOLatch documentRestartSafeLatch = new KVOLatch() {

            public String toString() {
                
                return super.toString()+" : documentRestartSafeLatch";
                
            }
            
            @Override
            protected void signal() throws InterruptedException {

                super.signal();

                statementBufferFactory.lock.lock();
                try {

                    // no longer waiting.
                    statementBufferFactory.documentsWaitingOnWritesCount
                            .decrementAndGet();

                    // one more that is done.
                    statementBufferFactory.documentRestartSafeCount
                            .incrementAndGet();

                    // increment by the #of told triples in this document.
                    statementBufferFactory.toldTriplesRestartSafeCount
                            .addAndGet(toldTriplesThisDocument);
                    
                    // notify that the document is done.
                    statementBufferFactory
                            .documentDone(getDocumentIdentifier());

                    // decrement counter.
                    if (statementBufferFactory.unfinishedDocumentCount
                            .decrementAndGet() == 0) {
                    
                        // and signalAll iff zero.
                        statementBufferFactory.documentsAllDone.signalAll();
                        
                    }
                    
                } finally {
                    
                    statementBufferFactory.lock.unlock();
                    
                }

                if (log.isInfoEnabled())
                    log.info("documentsDone="
                            + statementBufferFactory.documentRestartSafeCount
                                    .get()
                            + ", documentsWaiting="
                            + statementBufferFactory.documentsWaitingOnWritesCount
                                    .get());

            }

        };

        tasks.add(new AsyncId2TermIndexWriteTask(documentRestartSafeLatch,
                valueFactory, newId2TIterator(values.values().iterator(),
                        statementBufferFactory.producerChunkSize),
                statementBufferFactory.buffer_id2t));

        if (statementBufferFactory.buffer_text != null) {

            // FIXME full text index.
            throw new UnsupportedOperationException();
            // tasks.add(new AsyncTextWriteTask());

        }

        tasks.add(new AsyncSPOIndexWriteTask(documentRestartSafeLatch,
                SPOKeyOrder.SPO, statementBufferFactory.spoRelation,
                // (IChunkedOrderedIterator<ISPO>)
                statements.iterator(), statementBufferFactory.buffer_spo));

        if (statementBufferFactory.buffer_pos != null) {

            tasks.add(new AsyncSPOIndexWriteTask(documentRestartSafeLatch,
                    SPOKeyOrder.POS, statementBufferFactory.spoRelation,
                    // (IChunkedOrderedIterator<ISPO>)
                    statements.iterator(), statementBufferFactory.buffer_pos));

        }

        if (statementBufferFactory.buffer_osp != null) {

            tasks.add(new AsyncSPOIndexWriteTask(documentRestartSafeLatch,
                    SPOKeyOrder.OSP, statementBufferFactory.spoRelation,
                    // (IChunkedOrderedIterator<ISPO>)
                    statements.iterator(), statementBufferFactory.buffer_osp));

        }

        /*
         * Submit all tasks. They will run in parallel. If they complete
         * successfully then all we know is that the data has been buffered for
         * asynchronous writes on the various indices.
         * 
         * Note: java 1.6.0_07/12 build problems under linux when typed as
         * <Future> or any other combination that I have tried.
         */
        final List futures;

        /*
         * This latch is incremented _before_ buffering writes, and within each
         * routine that buffers writes, to avoid false triggering. This is done
         * to ensure that the latch will be positive until we exit the try /
         * finally block. We do this around the submit of the tasks and do not
         * decrement the latch until the futures are available so we known that
         * all data is buffered.
         */
        documentRestartSafeLatch.inc();
        try {

            futures = statementBufferFactory.tripleStore.getExecutorService()
                    .invokeAll((List) tasks);

        } finally {

            // decrement so that the latch can be triggered.
            documentRestartSafeLatch.dec();
            
        }

        // make sure that no errors were reported by those tasks.
        for (Object f : futures) {

            ((Future) f).get();

        }

        /*
         * At this point all writes have been buffered.
         */
        statementBufferFactory.lock.lock();
        try {
            statementBufferFactory.documentsWaitingOnWritesCount
                    .incrementAndGet();

            // no longer pending - we are now doing the writes for this doc.
            if (statementBufferFactory.documentsPendingWritesCount
                    .decrementAndGet() == 0) {
                statementBufferFactory.documentsPendingWritesDone.signalAll();
            }
        } finally {
            statementBufferFactory.lock.unlock();
        }

        if (log.isInfoEnabled()) {

            log.info("documentsWaiting="
                    + statementBufferFactory.documentsWaitingOnWritesCount.get());
            
        }
        
        // discard all buffer state (including bnodes and deferred statements).
        reset();
        
    }
    
    /**
     * Wrap a {@link BigdataValue}[] with a chunked iterator.
     */
    @SuppressWarnings("unchecked")
    static <V extends BigdataValue> IChunkedIterator<V> newT2IdIterator(
            final Iterator<V> itr, final int chunkSize) {

        return new ChunkedWrappedIterator(new Striterator(itr), chunkSize,
                BigdataValue.class);

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
     * Asynchronous writes on the TERM2ID index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo something similar for the SIDs
     */
    static class AsyncTerm2IdIndexWriteTask implements Callable<Void> {

        final protected transient static Logger log = Logger
                .getLogger(AsyncTerm2IdIndexWriteTask.class);

        private final KVOLatch latch;
        
        private final Term2IdTupleSerializer tupleSer;

        private final IChunkedIterator<BigdataValue> src;

        private final IRunnableBuffer<KVO<BigdataValue>[]> buffer;

        /**
         * 
         * @param latch
         * @param r
         * @param src
         *            The visits chunks of distinct {@link Value}s.
         * @param buffer
         */
        public AsyncTerm2IdIndexWriteTask(
                final KVOLatch latch,
                final LexiconRelation r,
                final IChunkedIterator<BigdataValue> src,
                final IRunnableBuffer<KVO<BigdataValue>[]> buffer) {

            if (latch == null)
                throw new IllegalArgumentException();
            
            if (r == null)
                throw new IllegalArgumentException();
            
            if (src == null)
                throw new IllegalArgumentException();
            
            if (buffer == null)
                throw new IllegalArgumentException();
            
            this.latch = latch;
            
            this.tupleSer = (Term2IdTupleSerializer) r.getIndex(
                    LexiconKeyOrder.TERM2ID).getIndexMetadata()
                    .getTupleSerializer();

            this.src = src;
            
            this.buffer = buffer;

        }

        /**
         * Reshapes the {@link #src} into {@link KVOC}[]s a chunk at a time and
         * submits each chunk to the write buffer for the TERM2ID index.
         */
        public Void call() throws Exception {

            /*
             * This is a thread-local instance, which is why we defer obtaining
             * this object until call() is executing.
             */
            final LexiconKeyBuilder keyBuilder = tupleSer.getLexiconKeyBuilder();
            
            latch.inc();

            try {

                while (src.hasNext()) {

                    final BigdataValue[] chunkIn = src.nextChunk();

                    final KVOC<BigdataValue>[] chunkOut = new KVOC[chunkIn.length];

                    int i = 0;

                    for (BigdataValue v : chunkIn) {

                        // Assign a sort key to each Value.
                        chunkOut[i++] = new KVOC<BigdataValue>(keyBuilder
                                .value2Key(v), null/* val */, v, latch)
//                                {
//                            @Override
//                            public void done() {
//                                /*
//                                 * verify that the term identifier is assigned
//                                 * before we decrement the latch.
//                                 */
//                                if (obj.getTermId() == IRawTripleStore.NULL)
//                                    throw new AssertionError("No termid? "
//                                            + this);
//                                super.done();
//                            }
//                        }
                        ;

                    }

                    // Place in KVO sorted order (by the byte[] keys).
                    Arrays.sort(chunkOut);

                    if (log.isInfoEnabled())
                        log.info("Adding chunk to TERM2ID master: chunkSize="
                                + chunkOut.length);
                    
                    // add chunk to async write buffer
                    buffer.add(chunkOut);

                }

            } finally {

                latch.dec();

            }

            // Done.
            return null;
            
        }
        
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

        private final KVOLatch latch;
        
        private final BigdataValueFactory valueFactory;

        private final IChunkedIterator<BigdataValue> src;

        private final IRunnableBuffer<KVO<BigdataValue>[]> buffer;

        /**
         * 
         * @param src
         *            The visits chunks of distinct {@link Value}s with their
         *            TIDs assigned. Blank nodes will automatically be filtered
         *            out.
         */
        public AsyncId2TermIndexWriteTask(
                final KVOLatch latch,
                final BigdataValueFactory valueFactory,
                final IChunkedIterator<BigdataValue> src,
                final IRunnableBuffer<KVO<BigdataValue>[]> buffer) {

            if (latch == null)
                throw new IllegalArgumentException();

            if (valueFactory == null)
                throw new IllegalArgumentException();
            
            if (src == null)
                throw new IllegalArgumentException();
            
            if (buffer == null)
                throw new IllegalArgumentException();
            
            this.latch = latch;
            
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

            latch.inc();

            try {

                while (src.hasNext()) {

                    final BigdataValue[] chunkIn = src.nextChunk();

                    final KVOC<BigdataValue>[] chunkOut = new KVOC[chunkIn.length];

                    int i = 0;

                    for (BigdataValue v : chunkIn) {

                        assert v != null;

                        if (v instanceof BNode) {

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

                        /*
                         * Note: The BigdataValue instance is NOT supplied to
                         * the KVO since we do not want it to be retained and
                         * since there is no side-effect on the BigdataValue for
                         * writes on ID2TERM (unlike the writes on TERM2ID).
                         */
                        chunkOut[i++] = new KVOC<BigdataValue>(key, val,
                                null/* v */, latch);

                    }

                    // make dense.
                    final KVO<BigdataValue>[] dense = KVO.dense(chunkOut, i);

                    /*
                     * Put into key order in preparation for writing on the
                     * reverse index.
                     */
                    Arrays.sort(dense);

                    // add chunk to asynchronous write buffer
                    buffer.add(dense);

                }

            } finally {

                latch.dec();

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
        
        private final KVOLatch latch;

        private final IKeyOrder<ISPO> keyOrder;

        /* Note: problem with java 1.6.0_07 and _12 on linux when typed. */
        private final IChunkedOrderedIterator/*<ISPO>*/ src;

        private final IRunnableBuffer<KVO<ISPO>[]> writeBuffer;

        private final SPOTupleSerializer tupleSer;
        
        public AsyncSPOIndexWriteTask(
                final KVOLatch latch,
                final IKeyOrder<ISPO> keyOrder,
                final SPORelation spoRelation,
                /* Note: problem with java 1.6.0_07 and _12 on linux when typed. */
                final IChunkedOrderedIterator/*<ISPO>*/ src,
                final IRunnableBuffer<KVO<ISPO>[]> writeBuffer) {

            if (latch == null)
                throw new IllegalArgumentException();

            if (keyOrder == null)
                throw new IllegalArgumentException();

            if (writeBuffer == null)
                throw new IllegalArgumentException();

            this.latch = latch;
            
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
            
            final ByteArrayBuffer vbuf = new ByteArrayBuffer(1 + 8/* max length */);

            latch.inc();

            try {
                
                while (src.hasNext()) {

                    // next chunk, in the specified order.
                    final ISPO[] chunk = (ISPO[]) src.nextChunk(keyOrder);

                    // note: a[] will be dense since nothing is filtered.
                    final KVOC<ISPO>[] a = new KVOC[chunk.length];

                    for (int i = 0; i < chunk.length; i++) {

                        final ISPO spo = chunk[i];

                        if (spo == null)
                            throw new IllegalArgumentException();

                        if (!spo.isFullyBound())
                            throw new IllegalArgumentException(
                                    "Not fully bound: " + spo.toString());

                        // generate key for the index.
                        final byte[] key = tupleSer
                                .statement2Key(keyOrder, spo);

                        // generate value for the index.
                        final byte[] val = spo.serializeValue(vbuf);

                        /*
                         * Note: The SPO is deliberately not provided to the KVO
                         * instance since it is not required (there is nothing
                         * being passed back from the write via a side-effect on
                         * the BigdataStatementImpl) and since it otherwise will
                         * force the retention of the RDF Value objects in its
                         * s/p/o/c positions.
                         */
                        a[i] = new KVOC<ISPO>(key, val, null/* spo */, latch);

                    }

                    // put chunk into sorted order based on assigned keys.
                    Arrays.sort(a);

                    // write chunk on the buffer.
                    writeBuffer.add(a);

                    chunksOut++;
                    elementsOut += a.length;

                    if (log.isDebugEnabled())
                        log.debug("Wrote chunk: index=" + keyOrder
                                + ", chunksOut=" + chunksOut + ", elementsOut="
                                + elementsOut + ", chunkSize=" + a.length);

                    if (log.isTraceEnabled())
                        log.trace("Wrote: index=" + keyOrder + ", chunk="
                                + Arrays.toString(a));

                }

            } finally {

                latch.dec();

            }

            if (log.isDebugEnabled())
                log.debug("Done: index=" + keyOrder + ", chunksOut="
                        + chunksOut + ", elementsOut=" + elementsOut);
            
            // done.
            return null;

        }
        
    }

    /**
     * Configuration object specifies the {@link BlockingBuffer}s which will be
     * used to write on each of the indices and the reference for the TERM2ID
     * index since we will use synchronous RPC on that index. The same
     * {@link AsynchronousWriteBufferFactoryWithoutSids2} may be used for
     * multiple concurrent {@link AsynchronousStatementBufferWithoutSids2}
     * instances.
     * 
     * @todo invert the inner and the outer class (the buffer factory should be
     *       the outer class).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     * @param <S>
     *            The generic type of the statement objects.
     * @param <F>
     *            The generic type of the file, URI, URL, etc which identifies
     *            the document to be loaded.
     */
    public static class AsynchronousWriteBufferFactoryWithoutSids2<S extends BigdataStatement,F>
        implements IAsynchronousWriteStatementBufferFactory<S> {

        /**
         * The database into which the statements will be written.
         */
        private final ScaleOutTripleStore tripleStore;
        
        /**
         * The lexicon.
         */
        private final LexiconRelation lexiconRelation;

        /**
         * The triples.
         */
        private final SPORelation spoRelation;

        /**
         * The initial capacity of the canonicalizing mapping for RDF
         * {@link Value}.
         */
        final int valuesInitialCapacity;

        /**
         * The initial capacity of the canonicalizing mapping for RDF
         * {@link BNode}s.
         */
        final int bnodesInitialCapacity;
        
        /**
         * The chunk size used by the producer to break the terms and statements
         * into chunks before writing them onto the {@link BlockingBuffer} for
         * the master.
         */
        final int producerChunkSize;
        
        private final IRunnableBuffer<KVO<BigdataValue>[]> buffer_t2id;
        private final IRunnableBuffer<KVO<BigdataValue>[]> buffer_id2t;
        private final IRunnableBuffer<KVO<BigdataValue>[]> buffer_text;
        private final IRunnableBuffer<KVO<ISPO>[]> buffer_spo;
        private final IRunnableBuffer<KVO<ISPO>[]> buffer_pos;
        private final IRunnableBuffer<KVO<ISPO>[]> buffer_osp;
        
        /**
         * Counts statements written on the database (applied only to the SPO index
         * so we do not double count).
         */
        private final LongAggregator statementResultHandler = new LongAggregator();
        
        /**
         * The timestamp taken when the factory is created.
         */
        private final long startTime;
        
        /**
         * The timestamp taken when the factory is {@link #close()}d or when
         * execution is {@link #cancelAll(boolean) cancelled}.
         */
        private long endTime;

        /**
         * The elapsed milliseconds between the time the first statement buffer
         * was requested and either the current time -or- the time the factory
         * finished work. In the latter case, the end time is assigned either by
         * {@link #close()} or by {@link #cancelAll(boolean)}.
         */
        public long getElapsedMillis() {

            if (startTime == 0L)
                return 0L;

            if (endTime == 0L) {

                return System.currentTimeMillis() - startTime;
                
            }
            
            return endTime - startTime;
            
        }
        
        /**
         * Lock used for making termination conditions in {@link #close()}
         * atomic.
         */
        private final ReentrantLock lock = new ReentrantLock();

        /**
         * The #of documents that have been parsed (cumulative total).
         */
        private final AtomicLong documentsParsedCount = new AtomicLong(0L);

        /**
         * The #of documents that are waiting on their TIDs (current value). You
         * MUST own the {@link #lock} when incrementing or decrementing this
         * value. When decrementing this counter, if it reaches zero then you
         * MUST do {@link Condition#signalAll()} on {@link #tidsAllDone}.
         */
        private final AtomicLong tidsWaitingCount = new AtomicLong(0L);

        /**
         * Signaled when {@link #tidsWaitingCount} hits zero.
         */
        private final Condition tidsAllDone = lock.newCondition();
        
        /**
         * The #of documents whose TIDs have been assigned (cumulative total).
         */
        private final AtomicLong tidsReadyCount = new AtomicLong(0L);

        /**
         * The #of documents whose whose TIDs have been assigned but whose
         * writes have not yet been buffered (current value). You MUST own the
         * {@link #lock} when incrementing or decrementing this value. When
         * decrementing this counter, if it reaches zero then you MUST do
         * {@link Condition#signalAll()} on {@link #documentsPendingWritesDone}.
         */
        private final AtomicLong documentsPendingWritesCount = new AtomicLong(0L);

        /**
         * Signaled when {@link #documentsPendingWritesCount} hits zero.
         */
        private final Condition documentsPendingWritesDone = lock.newCondition();

        /**
         * The #of documents whose writes have been buffered but are not yet
         * restart safe on the database (current value).
         */
        private final AtomicLong documentsWaitingOnWritesCount = new AtomicLong();

        /**
         * The #of told triples parsed from documents using this factory and
         * made restart safe on the database. This is incremented each time a
         * document has been made restart safe by the #of distinct told triples
         * parsed from that document.
         * <p>
         * Note: The same triple can occur in more than one document, and
         * documents having duplicate triples may be loaded by distributed
         * clients. The actual #of triples on the database is only available by
         * querying the database.
         */
        private final AtomicLong toldTriplesRestartSafeCount = new AtomicLong();
        
        /**
         * The #of documents which have been fully processed and are
         * restart-safe on the database (cumulative total).
         */
        private final AtomicLong documentRestartSafeCount = new AtomicLong();

        /**
         * The #of documents which have been accepted for processing and have
         * not yet reached either an error state or been made restart safe. You
         * MUST own the {@link #lock} when incrementing or decrementing this
         * value. When decrementing this counter, if it reaches zero then you
         * MUST do {@link Condition#signalAll()} on {@link #documentsAllDone}.
         */
        private final AtomicLong unfinishedDocumentCount = new AtomicLong();

        /**
         * Signaled when {@link #unfinishedDocumentCount} hits zero.
         */
        private final Condition documentsAllDone = lock.newCondition();

        /**
         * The #of documents for which the {@link BufferIndexWritesTask} failed.
         */
        private final AtomicLong documentErrorCount = new AtomicLong();
        
        /**
         * Bounded thread pool using an unbounded work queue to run
         * {@link BufferIndexWritesTask}s. Tasks are added to the work queue by
         * the "TIDs Ready" {@link KVOLatch}. Once the index writes have been
         * buffered, the statement buffer is placed onto the
         * {@link #docsWaitingQueue}. This {@link ExecutorService} MUST be
         * unbounded since tasks will be assigned by {@link KVOLatch#signal()}
         * and that method MUST NOT block.
         */
        private ExecutorService bufferIndexWritesService;

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
         * 
         * @see SPOIndexWriteProc
         */
        public long getStatementCount() {

            return statementResultHandler.getResult().longValue();
            
        }

        /**
         * 
         * @todo add parameter for the specific file, url, etc to be loaded.
         */
        public IStatementBuffer<S> newStatementBuffer() {
            
            return new AsynchronousStatementBufferWithoutSids2<S,F>(this);

        }

        /**
         * 
         * @param tripleStore
         * @param producerChunkSize
         *            The chunk size used when writing chunks onto the master
         *            for the asynchronous index write API. If this value is on
         *            the order of the #of terms or statements in the parsed
         *            documents, then all terms / statements will be written
         *            onto the master in one chunk. The master will split the
         *            chunk based on the separator keys for the index partitions
         *            and write splits onto the sink for each index partition.
         *            The master and sink configuration is specified via the
         *            {@link IndexMetadata} when the triple store indices are
         *            created.
         * @param valuesInitialCapacity
         *            The initial capacity of the map of the distinct RDF
         *            {@link Value}s parsed from a single document.
         * @param bnodesInitialCapacity
         *            The initial capacity of the map of the distinct RDF
         *            {@link BNode}s parsed from a single document.
         */
//        * @param parserPoolSize
//        *            The #of worker threads in the thread pool for parsing RDF
//        *            documents.
//        * @param writerPoolSize
//        *            The #of worker threads in the thread pool for buffering
//        *            asynchronous index writes.
        public AsynchronousWriteBufferFactoryWithoutSids2(//
                final ScaleOutTripleStore tripleStore,//
                final int producerChunkSize, //
                final int valuesInitialCapacity,//
                final int bnodesInitialCapacity //
//                final int parserPoolSize,
//                final int writerPoolSize
                ) {

            if (tripleStore == null)
                throw new IllegalArgumentException();
            
            this.tripleStore = tripleStore;
            
            this.lexiconRelation = tripleStore.getLexiconRelation();

            this.spoRelation = tripleStore.getSPORelation();

            this.producerChunkSize = producerChunkSize;

            this.valuesInitialCapacity = valuesInitialCapacity;
            
            this.bnodesInitialCapacity = bnodesInitialCapacity;
            
            if (tripleStore.isStatementIdentifiers()) {

                throw new UnsupportedOperationException("SIDs not supported");
                
            }

            this.buffer_t2id = ((IScaleOutClientIndex) lexiconRelation
                    .getTerm2IdIndex())
                    .newWriteBuffer(
                            new Term2IdWriteProcAsyncResultHandler(false/* readOnly */),
                            new DefaultDuplicateRemover<BigdataValue>(true/* testRefs */),
                            new Term2IdWriteProcConstructor(
                                    false/* readOnly */, lexiconRelation
                                            .isStoreBlankNodes(),
                                    lexiconRelation.getTermIdBitsToReverse()));
            
            this.buffer_id2t = ((IScaleOutClientIndex) lexiconRelation
                    .getId2TermIndex())
                    .newWriteBuffer(
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
                 * enormously in that manner and just write filters rather than
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
                            statementResultHandler,
                            new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                            SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

            if (!tripleStore.getSPORelation().oneAccessPath) {

                this.buffer_pos = ((IScaleOutClientIndex) spoRelation.getPOSIndex())
                        .newWriteBuffer(
                                null/* resultHandler */,
                                new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                                SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

                this.buffer_osp = ((IScaleOutClientIndex) spoRelation.getOSPIndex())
                        .newWriteBuffer(
                                null/* resultHandler */,
                                new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                                SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

            } else {

                this.buffer_pos = null;

                this.buffer_osp = null;
                
            }

            /*
             * Note: This service MUST NOT block or reject tasks as long as the
             * statement buffer factory is open. It is configured with an
             * unbounded workQueue and a bounded thread pool. The #of threads in
             * the pool should build up to the maximumPoolSize and idle threads
             * will be retired, but only after several minutes.
             */

            bufferIndexWritesService = new ThreadPoolExecutor(//
                    1, // corePoolSize
                    5, // maximumPoolSize
                    4, // keepAliveTime
                    TimeUnit.MINUTES, // keepAlive units.
                    new LinkedBlockingQueue<Runnable>(/* unbounded */),// workQueue
                    new DaemonThreadFactory(getClass().getName()) // threadFactory
            );

            startTime = System.currentTimeMillis();
            
        }

        public boolean isAnyDone() {

            if (buffer_t2id != null)
                if (buffer_t2id.getFuture().isDone())
                    return true;

            if (buffer_id2t.getFuture().isDone())
                return true;

            if (buffer_text != null)
                if (buffer_text.getFuture().isDone())
                    return true;

            if (buffer_spo.getFuture().isDone())
                return true;

            if (buffer_pos != null)
                if (buffer_pos.getFuture().isDone())
                    return true;

            if (buffer_osp != null)
                if (buffer_osp.getFuture().isDone())
                    return true;

            return false;
            
        }
        
        public void cancelAll(final boolean mayInterruptIfRunning) {

            if(log.isInfoEnabled())
                log.info("Cancelling futures.");
            
            if (endTime == 0L)
                endTime = System.currentTimeMillis();

            if (buffer_t2id != null)
                buffer_t2id.getFuture().cancel(mayInterruptIfRunning);

            buffer_id2t.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_text != null)
                buffer_text.getFuture().cancel(mayInterruptIfRunning);

            buffer_spo.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_pos != null)
                buffer_pos.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_osp != null)
                buffer_osp.getFuture().cancel(mayInterruptIfRunning);

            bufferIndexWritesService.shutdownNow();
            
        }

        /**
         * Awaits a signal that all documents which have queued writes are
         * finished and then closes the remaining buffers.
         */
        public void close() {
            
            try {
                lock.lockInterruptibly();
                try {
                    
                    /*
                     * No more documents may request TIDs, so first close the
                     * TERM2ID master. It will flush its writes.
                     */
                    if (buffer_t2id != null) {
                        if(log.isInfoEnabled()) {
                            log.info("Closing TERM2ID buffer.");
                        }
                        buffer_t2id.close();
                    }
                    if (tidsWaitingCount.get() != 0) {
                        // wait for those writes to be restart safe.
                        if(log.isInfoEnabled()) {
                            log.info("Awaiting TIDs : "+tidsWaitingCount);
                        }
                        tidsAllDone.await();
                    }

                    /*
                     * Now wait for all documents with TIDs to write on the
                     * remaining indices.
                     */
                    if (documentsPendingWritesCount.get() != 0) {
                        // wait for those writes to be restart safe.
                        if (log.isInfoEnabled()) {
                            log.info("Awaiting pending documents: "
                                    + documentsPendingWritesCount);
                        }
                        documentsPendingWritesDone.await();
                    }
                    
                    /*
                     * No more documents may write on the index buffers, so
                     * close the remaining index masters. They will flush their
                     * writes.
                     */
                    if(log.isInfoEnabled())
                        log.info("Closing remaining buffers.");

                    buffer_id2t.close();
                    
                    if (buffer_text != null)
                        buffer_text.close();

                    buffer_spo.close();
                    
                    if (buffer_pos != null)
                        buffer_pos.close();
                    
                    if (buffer_osp != null)
                        buffer_osp.close();

                    if (unfinishedDocumentCount.get() != 0) {
                        // and wait for their writes to be restart safe.
                        if(log.isInfoEnabled()) {
                            log.info("Awaiting documents : "+unfinishedDocumentCount);
                        }
                        documentsAllDone.await();
                    }

                } finally {
                    lock.unlock();
                    if (endTime == 0L)
                        endTime = System.currentTimeMillis();
                }
                
            } catch (InterruptedException ex) {
                
                // @todo should declare this exception in the API.
                throw new RuntimeException(ex);
                
            }

        }

        public void awaitAll() throws InterruptedException, ExecutionException {

            // Close the asynchronous write buffers.
            close();
            
            // Await futures for the asynchronous write buffers.
            if(log.isInfoEnabled())
                log.info("Awaiting futures.");

            if (buffer_t2id != null)
                buffer_t2id.getFuture().get();

            buffer_id2t.getFuture().get();

            if (buffer_text != null)
                buffer_text.getFuture().get();

            buffer_spo.getFuture().get();

            if (buffer_pos != null)
                buffer_pos.getFuture().get();

            if (buffer_osp != null)
                buffer_osp.getFuture().get();

            /*
             * This thread pool should be all done since the buffers on which
             * the worker tasks are writing are all done.
             */
            bufferIndexWritesService.shutdown();

        }

        /**
         * This method may be <em>extended</em> in order to trigger additional
         * processing when a document has become restart safe on the database,
         * however, the implementation MUST NOT block. One technique is to add
         * the <i>documentIdentifier</i> to an <em>unbounded</em> queue which is
         * then drained by another thread.
         * 
         * @param documentIdentifier
         *            The document identifier.
         */
        protected void documentDone(F documentIdentifier) {

        }

        public CounterSet getCounters() {
            
            final CounterSet counterSet = new CounterSet();

            /**
             * The #of documents that have been parsed.
             */
            counterSet.addCounter("documentsParsedCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(documentsParsedCount.get());
                }
            });

            /**
             * The #of documents that have been parsed and which are awaiting
             * their TIDs.
             */
            counterSet.addCounter("TIDsWaitingCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(tidsWaitingCount.get());
                }
            });

            /**
             * The #of documents whose TERM2ID writes are restart-safe on the
             * database. Each parser thread will block until the TERM2ID writes
             * are done and then proceed to write on the remaining indices.
             */
            counterSet.addCounter("TIDsReadyCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(tidsReadyCount.get());
                }
            });

            /**
             * 
             */
            counterSet.addCounter("documentsPendingWritesCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(documentsPendingWritesCount.get());
                }
            });

            /**
             */
            counterSet.addCounter("documentsWaitingOnWritesCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(documentsWaitingOnWritesCount.get());
                }
            });

            counterSet.addCounter("unfinishedDocumentCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(unfinishedDocumentCount.get());
                }
            });

            /**
             * The #of triples written on the SPO index (this does not count
             * triples that were already present on the index). 
             */
            counterSet.addCounter("toldTriplesWriteCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(getStatementCount());
                }
            });
            
            /**
             * The #of told triples parsed from documents using this factory and
             * made restart safe on the database. This is incremented each time a
             * document has been made restart safe by the #of distinct told triples
             * parsed from that document.
             * <p>
             * Note: The same triple can occur in more than one document, and
             * documents having duplicate triples may be loaded by distributed
             * clients. The actual #of triples on the database is only available by
             * querying the database.
             */
            counterSet.addCounter("toldTriplesRestartSafeCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(toldTriplesRestartSafeCount.get());
                }
            });

            /**
             * The told triples per second rate which have been made restart
             * safe by this factory object. When you are loading using multiple
             * clients, then the total told triples per second rate is the
             * aggregation across all of those instances.
             */
            counterSet.addCounter("toldTriplesRestartSafePerSec", new Instrument<Long>() {

                @Override
                protected void sample() {

                    final long elapsed = getElapsedMillis();

                    final double tps = (long) (((double) toldTriplesRestartSafeCount.get())
                            / ((double) elapsed) * 1000d);

                    setValue((long) tps);

                }
            });

            /**
             * The #of documents which have been processed by this client and
             * are restart safe on the database by this client.
             */
            counterSet.addCounter("documentRestartSafeCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(documentRestartSafeCount.get());
                }
            });

            /**
             * The #of documents for which the buffer index writes task failed.
             */
            counterSet.addCounter("documentErrorCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(documentErrorCount.get());
                }
            });

            return counterSet;
            
        }
        
    }

    /**
     * Class applies the term identifiers assigned by the
     * {@link Term2IdWriteProc} to the {@link BigdataValue} references in the
     * {@link KVO} correlated with each {@link Split} of data processed by that
     * procedure.
     * <p>
     * Note: Of necessity, this requires access to the {@link BigdataValue}s
     * whose term identifiers are being resolved. This implementation presumes
     * that the array specified to the ctor and the array returned for each
     * chunk that is processed have correlated indices and that the offset into
     * the array is given by {@link Split#fromIndex}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static private class Term2IdWriteProcAsyncResultHandler
            implements
            IAsyncResultHandler<Term2IdWriteProc.Result, Void, BigdataValue, KVO<BigdataValue>> {

        private final boolean readOnly;
        
        /**
         * 
         * @param readOnly
         *            if readOnly was specified for the {@link Term2IdWriteProc}.
         */
        public Term2IdWriteProcAsyncResultHandler(final boolean readOnly) {

            this.readOnly = readOnly;
            
        }

        /**
         * NOP
         * 
         * @see #aggregateAsync(KVO[],
         *      com.bigdata.rdf.lexicon.Term2IdWriteProc.Result, Split)
         */
        public void aggregate(final Term2IdWriteProc.Result result,
                final Split split) {

        }

        /**
         * Copy the assigned / discovered term identifiers onto the
         * corresponding elements of the terms[].
         */
        public void aggregateAsync(final KVO<BigdataValue>[] chunk,
                final Term2IdWriteProc.Result result, final Split split) {

            for (int i = 0; i < chunk.length; i++) {

                final long termId = result.ids[i];

                if (termId == IRawTripleStore.NULL) {

                    if (!readOnly)
                        throw new AssertionError();

                } else {

                    // assign the term identifier.
                    chunk[i].obj.setTermId(termId);

                    if(chunk[i] instanceof KVOList) {
                        
                        final KVOList<BigdataValue> tmp = (KVOList<BigdataValue>) chunk[i];

                        if (!tmp.isDuplicateListEmpty()) {

                            // assign the term identifier to the duplicates.
                            tmp.map(new AssignTermId(termId));

                        }
                        
                    }

                    if (log.isDebugEnabled()) {
                        log
                                .debug("termId=" + termId + ", term="
                                        + chunk[i].obj);
                    }

                }

            }

        }

        public Void getResult() {

            return null;

        }

    }

}
