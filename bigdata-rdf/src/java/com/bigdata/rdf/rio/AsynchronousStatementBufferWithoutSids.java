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

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.lexicon.Term2IdWriteTask;
import com.bigdata.rdf.lexicon.WriteTaskStats;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOIndexWriteProc;
import com.bigdata.rdf.spo.SPOIndexWriter;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOTupleSerializer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.service.IScaleOutClientIndex;
import com.bigdata.service.ndx.pipeline.DefaultDuplicateRemover;
import com.bigdata.striterator.IChunkedIterator;

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
 * FIXME Write a variant which does support SIDs.
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
 *       a performance benefit then refactor accordingly.
 */
public class AsynchronousStatementBufferWithoutSids implements
        IStatementBuffer<Statement> {

    final protected static Logger log = Logger
            .getLogger(AsynchronousStatementBufferWithoutSids.class);

    final protected boolean INFO = log.isInfoEnabled();

    final protected boolean DEBUG = log.isDebugEnabled();
    
    private final Config config;
    
    private final AbstractTripleStore database;
    
    private final BigdataValueFactory valueFactory;
    
    /**
     * The total #of parsed statements so far.
     */
    private int statementCount;

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
     * A list of chunks of statements. Rather than bother to filter out
     * duplicate statements, a chunk is allocted and appended to this list.
     * Statements are added to the current chunk until it is full, then a new
     * chunk is allocated.
     * <p>
     * Note: The statement chunks are always dense. All chunks except the last
     * one will be entirely full. The last chunk is typically only partly full.
     * Processing of the chunks can halt either by checking for a
     * <code>null</code> {@link Statement} reference in the chunk or by
     * comparing a counter against {@link #statementCount}.
     */
    private List<BigdataStatement[]> statementChunks;

    /**
     * The current chunk of statements -or- <code>null</code> if a chunk needs
     * to be allocated. The chunk is always added to {@link #statementChunks}
     * when it is allocated. The chunk size is determined by the
     * {@link #initialCapacity}.
     */
    private BigdataStatement[] statementChunk = null;

    /**
     * The #of statements in the current chunk. When the chunk is full (as
     * determined by this counter), a new chunk is allocated and added to the
     * {@link #statementChunks} and this counter is reset to ZERO(0).
     */
    private int statementChunkCount = 0;

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
    protected AsynchronousStatementBufferWithoutSids(final Config config) {
            
        if (config == null)
            throw new IllegalArgumentException();

        this.config = config;
        
        this.database = config.tripleStore;
        
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
        
        statementChunks = null;
        
        statementChunk = null;
        
        statementChunkCount = 0;
        
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
    
    public void add(final Statement e) {

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
                    config.bnodesInitialCapacity);

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
    private BigdataValue getCanonicalValue(BigdataValue term) {

        if (term instanceof BNode) {

            term = getCanonicalBNode((BigdataBNodeImpl) term);

            /*
             * Fall through.
             * 
             * Note: This also records the blank node in the values map so that
             * we can process the values map without having to consider the
             * blank nodes as well.
             */

        }

        if (values == null) {
         
            /*
             * Create a private (non-thread safe) canonicalizing mapping for RDF
             * Values.
             * 
             * Note: A linked hash map is used to make the iterator faster.
             */

            values = new LinkedHashMap<Value, BigdataValue>(
                    config.valuesInitialCapacity);

        }

        /*
         * Impose a canonicalizing mapping on the terms.
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
    private void handleStatement(Resource s, URI p, Value o, Resource c,
            StatementEnum type) {

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
    private void _handleStatement(Resource s, URI p, Value o, Resource c,
            StatementEnum type) {

        final BigdataStatement stmt = valueFactory.createStatement(
                (BigdataResource) s, (BigdataURI) p, (BigdataValue) o,
                (BigdataResource) c, type);

        if (statementChunk == null
                || statementChunkCount + 1 == statementChunk.length) {

            // Start a new chunk of statements.

            statementChunkCount = 0;

            statementChunk = new BigdataStatement[config.statementChunkCapacity];

            statementChunks.add(statementChunk);

        }

        // add to the current chunk.
        statementChunk[statementChunkCount++] = stmt;

        // total #of statements accepted.
        statementCount++;

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

        // dense array of the distinct terms.
        final BigdataValue[] values = this.values.values().toArray(
                new BigdataValue[0]);

        /*
         * Synchronous RPC.
         * 
         * Note: This is responsible for assigning the TIDs (term identifiers)
         * to the {@link BigdataValue}s. We CAN NOT write on the other indices
         * until we have those TIDs.
         */
        
        final KVO<BigdataValue>[] a = new Term2IdWriteTask(
                config.lexiconRelation, false/* readOnly */, values.length,
                values, new WriteTaskStats()).call();

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
         */
        final List<Callable> tasks = new LinkedList<Callable>();

        // @todo should break writes on buffer into chunks (or the buffer could do
        //       that).
        // FIXME ID2T
//        tasks.add(new AsyncID2TermWriteTask());

        if (config.buffer_text != null) {

            // FIXME full text index.
//            tasks.add(new AsyncTextWriteTask());

        }

        tasks.add(new AsyncSPOIndexWriteTask(SPOKeyOrder.SPO,
                config.buffer_spo));

        if (config.buffer_pos != null) {

            tasks.add(new AsyncSPOIndexWriteTask(SPOKeyOrder.POS,
                    config.buffer_pos));

        }

        if (config.buffer_osp != null) {

            tasks.add(new AsyncSPOIndexWriteTask(SPOKeyOrder.OSP,
                    config.buffer_osp));

        }

        // submit all tasks : they will run in parallel.
        final List<Future> futures = config.tripleStore
                .getExecutorService().invokeAll(tasks);

        // make sure that no errors were reported by those tasks.
        for (Future f : futures) {

            f.get();

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
     * 
     * @todo Modify the outer class to write the statements onto a buffer which
     *       automatically breaks them down into chunks and which is capabale of
     *       returning an {@link IChunkedIterator}. This will connect the
     *       chunked paradigm for producers with the chunked paradigm for
     *       consumers. Then this (inner) class can be made static and reused
     *       easily.
     *       <p>
     *       Note: We really need to do the same thing with the values[] since
     *       we want to feed it into a chunked buffer. However, since it has
     *       been reshaped into: (a) a value[]; and (b) a KVO[] that is dense
     *       and distinct, we can just use those and save some effort.
     */
    class AsyncSPOIndexWriteTask implements Callable<Void> {

        private final SPOKeyOrder keyOrder;

        // Note: Not used, but programmed in.
        private final IElementFilter<ISPO> filter = null;

        private final BlockingBuffer<KVO<ISPO>[]> writeBuffer;

        private final SPOTupleSerializer tupleSer;
        
        public AsyncSPOIndexWriteTask(final SPOKeyOrder keyOrder,
                final BlockingBuffer<KVO<ISPO>[]> writeBuffer) {

            if (keyOrder == null)
                throw new IllegalArgumentException();

            if (writeBuffer == null)
                throw new IllegalArgumentException();
            
            this.keyOrder = keyOrder;
            
            this.writeBuffer = writeBuffer;
            
            // the tuple serializer for this access path.
            this.tupleSer = (SPOTupleSerializer) config.tripleStore
                    .getSPORelation().getIndex(keyOrder).getIndexMetadata()
                    .getTupleSerializer();
            
        }
        
        public Void call() throws Exception {
            
            final ByteArrayBuffer vbuf = new ByteArrayBuffer(1+8/*max length*/);
            
            int i = 0;
            
            ISPO last = null;
            
            for (BigdataStatement[] chunk : statementChunks) {

                final KVO<ISPO>[] a = new KVO[statementCount];

                int j = 0;
                
                for (ISPO spo : chunk) {

                    // done?
                    if (i == statementCount)
                        break;
                    
                    if (spo == null)
                        throw new IllegalArgumentException("null @ index=" + i);

                    if (!spo.isFullyBound())
                        throw new IllegalArgumentException("Not fully bound: "
                                + spo.toString());

                    // skip statements that match the filter.
                    if (filter != null && filter.accept(spo))
                        continue;

                    // skip duplicate records.
                    if (last != null && last.equals(spo))
                        continue;

                    // generate key for the index.
                    final byte[] key = tupleSer.statement2Key(keyOrder, spo);

                    // generate value for the index.
                    final byte[] val = spo.serializeValue(vbuf);

                    a[j++] = new KVO<ISPO>(key, val, spo);

                    last = spo;

                }

                // empty chunk?
                if (j == 0)
                    continue;
                
                /*
                 * Make the chunk dense.
                 */
                final KVO<ISPO>[] b;

                if (j != a.length) {

                    b = new KVO[j];
                    
                    System
                            .arraycopy(a, 0/* srcpos */, b, 0/*dstpos*/, j/*len*/);
                    
                } else {
                    
                    b = a;
                    
                }
                
                // put the chunk into sorted order.
                Arrays.sort(b);
                
                // write the chunk on the buffer.
                writeBuffer.add(b);

            }
            
            return null;

        }
        
    }
    
    /**
     * Configuration object specifies the {@link BlockingBuffer}s which will be
     * used to write on each of the indices and the reference for the TERM2ID
     * index since we will use synchronous RPC on that index. The same
     * {@link Config} may be used for multiple concurrent
     * {@link AsynchronousStatementBufferWithoutSids} instances.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo add a factory method returning an interface or abstract class
     *       provides whatever configuration is relevant given the
     *       {@link ScaleOutTripleStore} and also provides a factory for
     *       {@link IStatementBuffer}s based on that configuration.
     * 
     * @todo this is already setup such that it would be easy to parameterize
     *       for TM.
     */
    public static class Config {
       
        private final ScaleOutTripleStore tripleStore;
        
        private final LexiconRelation lexiconRelation;

        private final SPORelation spoRelation;
        
        final int indexWriteQueueCapacity = BlockingBuffer.DEFAULT_PRODUCER_QUEUE_CAPACITY;

        final int indexPartitionWriteQueueCapacity = BlockingBuffer.DEFAULT_PRODUCER_QUEUE_CAPACITY;

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
        final int statementChunkCapacity = BlockingBuffer.DEFAULT_CONSUMER_CHUNK_SIZE;
        
        private final IIndex term2Id;
        private final BlockingBuffer<KVO<BigdataValue>[]> buffer_t2id;
        private final BlockingBuffer<KVO<BigdataValue>[]> buffer_text;
        private final BlockingBuffer<KVO<ISPO>[]> buffer_spo;
        private final BlockingBuffer<KVO<ISPO>[]> buffer_pos;
        private final BlockingBuffer<KVO<ISPO>[]> buffer_osp;
        
        private final LongAggregator statementResultHandler = new LongAggregator();

        /**
         * Return a new {@link IStatementBuffer} which may be used to bulk load
         * RDF data. The writes will proceed asynchronously using buffers shared
         * by all {@link IStatementBuffer}s returned by this factory for a
         * given instance of this class. Each {@link IStatementBuffer} MAY be
         * recycled using {@link IBuffer#reset()} or simply discarded after its
         * use.
         */
        public IStatementBuffer<Statement> newStatementBuffer() {

            return new AsynchronousStatementBufferWithoutSids(this);

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

        public Config(final ScaleOutTripleStore tripleStore) {

            if (tripleStore == null)
                throw new IllegalArgumentException();
            
            this.tripleStore = tripleStore;
            
            this.lexiconRelation = tripleStore.getLexiconRelation();

            this.spoRelation = tripleStore.getSPORelation();

            this.term2Id = lexiconRelation.getTerm2IdIndex();

            if (tripleStore.isStatementIdentifiers()) {

                throw new UnsupportedOperationException("SIDs not supported");
                
            }
            
            // FIXME ID2T and Text indices.
            this.buffer_t2id = null;
            this.buffer_text = null;
            
//            this.buffer_t2id = ((IScaleOutClientIndex) lexiconRelation.getId2TermIndex()).newWriteBuffer(
//                    indexWriteQueueCapacity, indexPartitionWriteQueueCapacity,
//                    resultHandler, new DefaultDuplicateRemover<BigdataValue>(
//                            true/* testRefs */), ctor);
//
//            if (tripleStore.getLexiconRelation().isTextIndex()) {
//
//                this.buffer_text = ((IScaleOutClientIndex) lexiconRelation.getId2TermIndex())
//                        .newWriteBuffer(
//                                indexWriteQueueCapacity,
//                                indexPartitionWriteQueueCapacity,
//                                resultHandler,
//                                new DefaultDuplicateRemover<BigdataValue>(true/* testRefs */),
//                                ctor);
//                
//            } else {
//                
//                this.buffer_text = null;
//                
//            }

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
        
        /**
         * Cancel all {@link Future}s.
         * 
         * @param mayInterruptIfRunning
         */
        public void cancelAll(final boolean mayInterruptIfRunning) {

            buffer_t2id.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_text != null)
                buffer_text.getFuture().cancel(mayInterruptIfRunning);

            buffer_spo.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_pos != null)
                buffer_pos.getFuture().cancel(mayInterruptIfRunning);

            if (buffer_osp != null)
                buffer_osp.getFuture().cancel(mayInterruptIfRunning);

        }

        /**
         * Close the {@link BlockingBuffer}s and await their {@link Future}s.
         * 
         * @throws ExecutionException
         *             if any {@link Future} fails.
         * 
         * @throws InterruptedException
         *             if interrupted while awaiting any of the {@link Future}s.
         */
        public void awaitAll() throws InterruptedException, ExecutionException {

            /*
             * Close buffers. Once closed, they will not accept further input.
             */
            
            buffer_t2id.close();
            
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
            
            buffer_t2id.getFuture().get();

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
