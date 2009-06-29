/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2009
 */

package com.bigdata.rdf.rio;

import java.beans.Statement;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
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
import org.openrdf.rio.RDFFormat;

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
import com.bigdata.journal.AbstractTask;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer;
import com.bigdata.rdf.lexicon.Term2IdWriteProc;
import com.bigdata.rdf.lexicon.Id2TermWriteProc.Id2TermWriteProcConstructor;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer.LexiconKeyBuilder;
import com.bigdata.rdf.lexicon.Term2IdWriteProc.Term2IdWriteProcConstructor;
import com.bigdata.rdf.lexicon.Term2IdWriteTask.AssignTermId;
import com.bigdata.rdf.load.ConcurrentDataLoader;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.AsynchronousStatementBufferWithoutSids2.AsynchronousWriteBufferFactoryWithoutSids2;
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
import com.bigdata.service.AbstractFederation;
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
import com.bigdata.util.concurrent.Latch;
import com.bigdata.util.concurrent.ShutdownHelper;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Configuration object specifies the {@link BlockingBuffer}s which will be used
 * to write on each of the indices and the reference for the TERM2ID index since
 * we will use synchronous RPC on that index. The same
 * {@link AsynchronousWriteBufferFactoryWithoutSids2} may be used for multiple
 * concurrent {@link AsynchronousStatementBufferImpl} instances.
 * <p>
 * The asynchronous statement buffer w/o SIDs is much simpler that w/. If we
 * require that the document is fully buffered in memory, then we can simplify
 * this to just:
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
 *          FIXME Modify to support SIDs. We basically need to loop in the
 *          {@link #workflowLatch_bufferTerm2Id} workflow state until all SIDs
 *          have been assigned. However, the termination conditions will be a
 *          little more complex. During termination, if we have the TIDs but not
 *          yet the SIDs then we need to flush the SID requests rather than
 *          allowing them to timeout. Since SID processing is cyclic, we may
 *          have to do this one or more times.
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
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <S>
 *            The generic type of the statement objects.
 */
public class AsynchronousStatementBufferFactory<S extends BigdataStatement>
    implements IAsynchronousWriteStatementBufferFactory<S> {

    final protected transient static Logger log = Logger
            .getLogger(AsynchronousStatementBufferFactory.class);
    
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
    private final int valuesInitialCapacity;

    /**
     * The initial capacity of the canonicalizing mapping for RDF
     * {@link BNode}s.
     */
    private final int bnodesInitialCapacity;
    
    /**
     * The chunk size used by the producer to break the terms and statements
     * into chunks before writing them onto the {@link BlockingBuffer} for
     * the master.
     */
    private final int producerChunkSize;
    
    /**
     * The default {@link RDFFormat}.
     */
    private final RDFFormat defaultFormat;
    
    /**
     * Validate the RDF interchange syntax when <code>true</code>.
     */
    private final boolean verifyData;

    /**
     * Delete files after they have been successfully loaded when
     * <code>true</code>.
     */
    private final boolean deleteAfter;

    /**
     * Validate the RDF interchange syntax when <code>true</code>.
     */
    protected boolean isVerifyData() {
        
        return verifyData;
        
    }

    /**
     * Delete files after they have been successfully loaded when
     * <code>true</code>.
     */
    protected boolean isDeleteAfter() {
 
        return deleteAfter;
        
    }

    /**
     * The default RDF interchange format that will be used when the format can
     * not be determined.
     */
    protected RDFFormat getDefaultRDFFormat() {

        return defaultFormat;
        
    }

    /*
     * Asynchronous index write buffers.
     */
    
    private IRunnableBuffer<KVO<BigdataValue>[]> buffer_t2id;
    private IRunnableBuffer<KVO<BigdataValue>[]> buffer_id2t;
    private IRunnableBuffer<KVO<BigdataValue>[]> buffer_text;
    private IRunnableBuffer<KVO<ISPO>[]> buffer_spo;
    private IRunnableBuffer<KVO<ISPO>[]> buffer_pos;
    private IRunnableBuffer<KVO<ISPO>[]> buffer_osp;

    /**
     * If the TERM2ID asynchronous write buffer is open, then close it to flush
     * any buffered writes and, regardless, re-open the buffer if it is
     * configured for use.
     */
    private void reopenBuffer_term2Id() {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (buffer_t2id != null) {

            buffer_t2id.close();

        }

        buffer_t2id = ((IScaleOutClientIndex) lexiconRelation.getTerm2IdIndex())
                .newWriteBuffer(
                        new Term2IdWriteProcAsyncResultHandler(false/* readOnly */),
                        new DefaultDuplicateRemover<BigdataValue>(true/* testRefs */),
                        new Term2IdWriteProcConstructor(false/* readOnly */,
                                lexiconRelation.isStoreBlankNodes(),
                                lexiconRelation.getTermIdBitsToReverse()));

    }

    /**
     * For each of the non-TERM2ID asynchronous write buffers, if it is open,
     * then close it to flush any buffered writes and, regardless, re-open the
     * buffer if it is configured for use.
     */
    private final void reopenBuffer_others() {

        if (buffer_id2t != null) {

            buffer_id2t.close();

            buffer_id2t = null;

        }

        buffer_id2t = ((IScaleOutClientIndex) lexiconRelation.getId2TermIndex())
                .newWriteBuffer(
                        null/* resultHandler */,
                        new DefaultDuplicateRemover<BigdataValue>(true/* testRefs */),
                        Id2TermWriteProcConstructor.INSTANCE);

        if (buffer_text != null) {

            buffer_text.close();

            buffer_text = null;

        }
        if (tripleStore.getLexiconRelation().isTextIndex()) {

            /*
             * FIXME text index. This can be partly handled by factoring out a
             * filter to be applied to a striterator. However, true async writes
             * need to reach into the full text index package. Probably there
             * should be a KVO ctor which knows how to form the key and value
             * from the object for a given index, e.g., using the
             * tupleSerializer. I could probably clean things up enormously in
             * that manner and just write filters rather than custom glue for
             * sync and async index writes.
             */
            throw new UnsupportedOperationException();
            // this.buffer_text = ((IScaleOutClientIndex)
            // lexiconRelation.getId2TermIndex())
            // .newWriteBuffer(
            // indexWriteQueueCapacity,
            // indexPartitionWriteQueueCapacity,
            // resultHandler,
            // new DefaultDuplicateRemover<BigdataValue>(true/* testRefs */),
            // ctor);

        } else {

            buffer_text = null;

        }

        if (buffer_spo != null) {

            buffer_spo.close();

            buffer_spo = null;

        }

        buffer_spo = ((IScaleOutClientIndex) spoRelation.getSPOIndex())
                .newWriteBuffer(statementResultHandler,
                        new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                        SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

        if (!tripleStore.getSPORelation().oneAccessPath) {

            if (buffer_pos != null) {

                buffer_pos.close();

                buffer_pos = null;

            }

            buffer_pos = ((IScaleOutClientIndex) spoRelation.getPOSIndex())
                    .newWriteBuffer(
                            null/* resultHandler */,
                            new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                            SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

            if (buffer_osp != null) {

                buffer_osp.close();

                buffer_osp = null;

            }

            buffer_osp = ((IScaleOutClientIndex) spoRelation.getOSPIndex())
                    .newWriteBuffer(
                            null/* resultHandler */,
                            new DefaultDuplicateRemover<ISPO>(true/* testRefs */),
                            SPOIndexWriteProc.IndexWriteProcConstructor.INSTANCE);

        } else {

            buffer_pos = null;

            buffer_osp = null;

        }

    }
    
    /**
     * Counts statements written on the database (applied only to the SPO index
     * so we do not double count).
     */
    private final LongAggregator statementResultHandler = new LongAggregator();

    /**
     * The timestamp set when {@link #notifyStart()} is invoked. This is done
     * when the factory is created.
     */
    private volatile long startTime;

    /**
     * The timestamp set when {@link #notifyEnd()} is invoked. This is done when
     * the factory is {@link #close()}d or when execution is
     * {@link #cancelAll(boolean) cancelled}.
     */
    private long endTime;

    /**
     * Notify that the factory will begin running tasks. This sets the
     * {@link #startTime} used by {@link #getElapsedMillis()} to report the run
     * time of the tasks.
     */
    protected void notifyStart() {

        /*
         * Note: uses the lock to make this atomic since we do this when we
         * accept each document and we already own the lock at that point.
         */
        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (startTime == 0L) {

            endTime = 0L;

            startTime = System.currentTimeMillis();

        }
        
    }

    /**
     * Notify that the factory is done running tasks (for now). This places a
     * cap on the time reported by {@link #elapsed()}.
     */
    protected void notifyEnd() {
        
        endTime = System.currentTimeMillis();

        parserService.shutdownNow();
        
        term2IdWriterService.shutdownNow();
        
        otherWriterService.shutdownNow();

        if (serviceStatisticsTask != null) {
            
            serviceStatisticsTask.cancel();
            
        }
        
    }

    /**
     * The elapsed milliseconds, counting only the time between
     * {@link #notifyStart()} and {@link #notifyEnd()}.
     */
    public long getElapsedMillis() {

        if (startTime == 0L)
            return 0L;

        if (endTime == 0L) {

            return System.currentTimeMillis() - startTime;
            
        }
        
        return endTime - startTime;
        
    }

    /*
     * Cumulative counters. These do not need to be protected by the lock as
     * they do not guard transitions between workflow states.
     */
    
    /**
     * The #of documents that have been parsed (cumulative total).
     */
    private final AtomicLong documentsParsedCount = new AtomicLong(0L);

    /**
     * The #of documents whose TIDs have been assigned (cumulative total).
     */
    private final AtomicLong documentTIDsReadyCount = new AtomicLong(0L);
    
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
     * The #of documents for which the {@link BufferOtherWritesTask} failed.
     */
    private final AtomicLong documentErrorCount = new AtomicLong();

    /*
     * Latches. The latches guard transitions between workflow states and must
     * be protected by the lock.
     */

    /**
     * The {@link #lock} is used to makes the observable state changes for the
     * factory atomic and guards the termination conditions in {@link #close()}.
     * You MUST own the {@link #lock} when incrementing or decrementing any of
     * the {@link Latch}s. The {@link Latch} transitions must be accomplished
     * while you are holding the lock. For example, the transition between
     * <i>parsing</i> and <i>buffering TERM2ID writes</i> requires that we
     * decrement one latch and increment the other while hold the {@link #lock}.
     * <p>
     * The counter associated with each {@link Latch} indicates the total #of
     * documents associated with that workflow state but does not differentiate
     * between documents waiting on the work queue for the corresponding thread
     * pool (e.g., the {@link #parserService}), documents assigned to a worker
     * thread and running in the thread pool, and documents waiting for some
     * state change (e.g., the return from an asynchronous write) before they
     * can be transferred to the next workflow state. However, you can gain
     * additional information about the various thread pools from their
     * counters, including their work queue size, the #of active tasks, etc.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * A global {@link Latch} guarding all documents which have been accepted for
     * processing and have not yet reached an absorbing state (either an error
     * state or been made restart safe).
     */
    private final Latch workflowLatch_document = new Latch("document",lock);

    /**
     * A {@link Latch} guarding documents which have been accepted for parsing
     * but have not been transferred to the {@link #workflowLatch_bufferTerm2Id}
     * .
     * 
     * @todo We could add a resolver latch for network IO required to buffer the
     *       document locally. E.g., a read from a DFS or a web page.
     */
    private final Latch workflowLatch_parser = new Latch("parser", lock);

    /**
     * A {@link Latch} guarding documents that have begun to buffering their
     * writes on the TERM2ID index but have not been transferred to the
     * {@link #workflowLatch_bufferOther}.
     */
    private final Latch workflowLatch_bufferTerm2Id = new Latch(
            "bufferTerm2Id", lock);

    /**
     * A {@link Latch} guarding documents that have begun to buffer their writes
     * on the other indices but have not yet completed their processing.
     */
    private final Latch workflowLatch_bufferOther = new Latch("bufferOther",
            lock);

    /*
     * Latches used to guard tasks buffering writes. There is one such latch for
     * TERM2ID and one for the rest of the buffers. During close() we will close
     * the buffers to flush their writes as soon as these latches hit zero.
     * 
     * Note: These latches allow us to close the buffers in a timely manner. The
     * other latches guard the workflow state transitions. However, if we do not
     * close the buffers in a timely manner then close() will hang until a chunk
     * or idle timeout (if any) causes the buffers to be flushed!
     */

    /**
     * {@link Latch} guarding tasks until they have buffered their writes on the
     * TERM2ID index.  This latch is decremented as soon as the writes for a given
     * document have been buffered.  This is used to close the TERM2ID buffer in
     * a timely manner in {@link #close()}.
     */
    private final Latch bufferGuard_term2Id = new Latch("guard_term2Id",
            lock);

    /**
     * {@link Latch} guarding tasks until they have buffered their writes on the
     * remaining index buffers. This latch is decremented as soon as the writes
     * for a given document have been buffered. This is used to close the other
     * buffers in a timely manner in {@link #close()}.
     */
    private final Latch bufferGuard_other = new Latch("guard_other",
            lock);
    
    /*
     * Parser service pause/resume.
     */

    /**
     * The maximum #of statements which may be buffered before the
     * {@link #parserService} will be paused in order to permit buffered
     * statements to drain from memory. In order to ensure liveness, pausing the
     * write service MUST be combined with flushing writes through the
     * asynchronous write buffers, which will otherwise wait for an idle timeout
     * or a chunk timeout if they are not being driven by demand.
     * 
     * FIXME Ensure liveness by closing and re-opening the asynchronous write
     * buffers if the parser service is paused and the work queue for the
     * corresponding stage (term2Id or other index writes) is empty. The
     * close/open needs to be atomic of course, and it should be triggered when
     * parser tasks are blocking (paused) AND the master is empty. That could be
     * done with a timeout in the {@link ParserThreadPoolExecutor} when it is
     * awaiting the {@link #unpaused} {@link Condition}.
     * <p>
     * Use small value on U1 test variant to look for fence posts.
     */
    private final long bufferedStatementThreshold;
    
    /**
     * The #of RDF {@link Statement}s that are buffered. This is incremented
     * when all statements for a given document have been parsed by the #of
     * distinct statements in that document. This is decremented all writes for
     * that document have been made restart safe on the database, or if
     * processing fails for that document. This may be used as a proxy for the
     * amount of data which is unavailable for garbage collection and thus for
     * the size of the heap entailed by processing.
     */
    private final AtomicLong bufferedStatementCount = new AtomicLong();

    /**
     * In order to prevent runaway demand on RAM, new parser tasks must await
     * this {@link Condition} if the #of buffered statements is GTE the
     * configured {@link #bufferedStatementThreshold} threshold.
     */
    private Condition unpaused = lock.newCondition();

    /**
     * The #of threads which are currently paused awaiting the {@link #unpaused}
     * {@link Condition}.
     */
    private AtomicLong pausedThreadCount = new AtomicLong();

    /**
     * The #of times the {@link #parserService} has been paused.
     */
    private AtomicLong poolPausedCount = new AtomicLong();
    
    /**
     * Verify counters for latches which must sum atomically to the
     * {@link #workflowLatch_document}.
     */
    private void assertSumOfLatchs() {
        
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        
        // sum latches for the distinct workflow states for a document.
        final long n1 = workflowLatch_parser.get()//
                + workflowLatch_bufferTerm2Id.get()//
                + workflowLatch_bufferOther.get()//
        ;
        
        final long n2 = workflowLatch_document.get();
        
        if (n1 != n2) {

            throw new AssertionError("Sum of Latches=" + n1
                    + ", but unfinished=" + n2 + " : "
                    + getCounters().toString());
            
        }
        
    }
    
    /**
     * Bounded thread pool using a bounded work queue to run the parser tasks.
     * If a backlog develops, then the thread pool is <em>paused</em>, and new
     * tasks will not start until the backlog is cleared. This will cause the
     * work queue to fill up, and the threads feeding that work queue to block.
     * This is done to place bounds on the memory demands of the total pipeline.
     */
    private final ParserThreadPoolExecutor parserService;
    
    /**
     * Bounded thread pool using an unbounded work queue to buffer writes for
     * the TERM2ID index. Tasks are added to the work queue by the parser task
     * in {@link AsynchronousStatementBufferImpl#flush()}.
     */
    private final ThreadPoolExecutor term2IdWriterService;

    /**
     * Bounded thread pool using an unbounded work queue to run
     * {@link BufferOtherWritesTask}s. Tasks are added to the work queue by
     * the "TIDs Ready" {@link KVOLatch}. Once the index writes have been
     * buffered, the statement buffer is placed onto the
     * {@link #docsWaitingQueue}. This {@link ExecutorService} MUST be
     * unbounded since tasks will be assigned by {@link KVOLatch#signal()}
     * and that method MUST NOT block.
     */
    private final ThreadPoolExecutor otherWriterService;

    /**
     * Bounded thread pool with an unbounded work queue used to delete files
     * once they are restart safe on the database. This thread pool is created
     * iff the {@link #deleteAfter} option was specified.
     */
    private final ThreadPoolExecutor deleteService;

    /**
     * {@link Runnable} collects performance counters on services used by the
     * factory.
     */
    private final ServiceStatisticsTask serviceStatisticsTask;
    
    /**
     * Return an estimate of the #of statements written on the indices.
     * <p>
     * This value is aggregated across any {@link IStatementBuffer} obtained
     * from {@link #newStatementBuffer()} for this instance.
     * <p>
     * This value actually reports the #of statements written on the SPO index
     * for the database. Statements are written asynchronously in chunks and the
     * writes MAY proceed at different rates for each of the statement indices.
     * The counter value will be stable once the {@link #awaitAll()} returns
     * normally.
     * 
     * @see SPOIndexWriteProc
     */
    public long getStatementCount() {

        return statementResultHandler.getResult().longValue();

    }

    /**
     * The #of documents submitted to the factory which could not be processed
     * due to some error.
     */
    public long getDocumentErrorCount() {

        return documentErrorCount.get();

    }

    /**
     * The #of documents submitted to the factory which have been processed
     * successfully.
     */
    public long getDocumentDoneCount() {

        return documentRestartSafeCount.get();

    }

    /**
     * Note: do not invoke this directly. It does not know how to set the
     * resource identifier on the statement buffer impl.
     */
    public IStatementBuffer<S> newStatementBuffer() {
        
        return newStatementBuffer(null/* resource */);

    }

    protected AsynchronousStatementBufferImpl newStatementBuffer(
            final String resource) {

        return new AsynchronousStatementBufferImpl(resource);

    }

    /**
     * Submit a resource for processing.
     * 
     * @param resource
     *            The resource (file or URL, but not a directory).
     * 
     * @throws Exception
     */
    public void submitOne(final String resource) throws Exception {

        lock.lock();
        try {

            assertSumOfLatchs();

            notifyStart();

            /*
             * Note: The total processing of the documents will not terminate
             * until this latch has been decremented back to zero.
             */
            workflowLatch_document.inc();

            workflowLatch_parser.inc();

            assertSumOfLatchs();

        } finally {

            lock.unlock();

        }

        /*
         * Submit resource for parsing.
         * 
         * @todo it would be nice to return a Future here that tracked the
         * document through the workflow.
         */

        parserService.submit(newParserTask(resource));

    }

    /**
     * Submit all files in a directory for processing via
     * {@link #submitOne(String)}.
     * 
     * @param fileOrDir
     *            The file or directory.
     * @param filter
     *            An optional filter. Only the files selected by the filter will
     *            be processed.
     * 
     * @return The #of files that were submitted for processing.
     * 
     * @throws Exception
     */
    public int submitAll(final File fileOrDir, final FilenameFilter filter)
            throws Exception {

        return new RunnableFileSystemLoader(fileOrDir, filter).call();

    }

    /**
     * Return a task to parse the document. The task should allocate an
     * {@link AsynchronousStatementBufferImpl} for the document. When
     * that buffer is flushed, the document will be queued for further
     * processing.
     * 
     * @param resource
     *            The resource to be parsed.
     * @return The task to execute.
     * 
     * @throws Exception
     */
    protected Callable<?> newParserTask(final String resource) throws Exception {

        if (log.isInfoEnabled())
            log.info("resource=" + resource);
        
        final RDFFormat defaultFormat= getDefaultRDFFormat();

        final RDFFormat rdfFormat = (defaultFormat == null //
                ? RDFFormat.forFileName(resource) //
                : RDFFormat.forFileName(resource, defaultFormat)//
        );
        
        if (rdfFormat == null) {

            throw new RuntimeException(
                    "Could not determine interchange syntax - skipping : file="
                            + resource);

        }

        // Convert the resource identifier to a URL.
        final String baseURI; ;
        if (getClass().getResource(resource) != null) {
            
            baseURI = getClass().getResource(resource).toURI()
                    .toString();
            
        } else {
            
            baseURI = new File(resource).toURI().toString();
            
        }

        return new ParserTask(resource, baseURI, rdfFormat);

    }

    /**
     * Tasks either loads a RDF resource or verifies that the told triples found
     * in that resource are present in the database. The difference between data
     * load and data verify is just the behavior of the {@link IStatementBuffer}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ParserTask implements Callable<Void> {

        /**
         * The resource to be loaded.
         */
        private final String resource;
        
        /**
         * The base URL for that resource.
         */
        private final String baseURL;
        
        /**
         * The RDF interchange syntax that the file uses.
         */
        private final RDFFormat rdfFormat;

        /**
         * 
         * @param resource
         *            The resource to be loaded (a plain file or URL, but not a
         *            directory).
         * @param baseURL
         *            The base URL for that resource.
         * @param rdfFormat
         *            The RDF interchange syntax that the file uses.
         */
        public ParserTask(final String resource, final String baseURL,
                final RDFFormat rdfFormat) {

            if (resource == null)
                throw new IllegalArgumentException();

            if (baseURL == null)
                throw new IllegalArgumentException();

            this.resource = resource;

            this.baseURL = baseURL;

            this.rdfFormat = rdfFormat;
            
        }

        public Void call() throws Exception {
            // Note: buffer will be pass along from queue to queue.
            final AsynchronousStatementBufferImpl buffer = AsynchronousStatementBufferFactory.this
                    .newStatementBuffer(resource);
            try {
                // open reader on the file.
                final InputStream rdfStream = new FileInputStream(resource);
                try {
                    // Obtain a buffered reader on the input stream.
                    final Reader reader = new BufferedReader(new InputStreamReader(
                            rdfStream));
                    try {
                        // run the parser.
                        new PresortRioLoader(buffer).loadRdf(reader, baseURL,
                                rdfFormat, verifyData);
                    } finally {
                        reader.close();
                    }
                } finally {
                    rdfStream.close();
                }
                lock.lock();
                try {
                    // done parsing this document.
                    documentsParsedCount.incrementAndGet();
                    // queue task to buffer the writes.
                    term2IdWriterService
                            .submit(new BufferTerm2IdWrites(buffer));
                    // increment #of buffered statements.
                    bufferedStatementCount.addAndGet(buffer.statementCount);
                } finally {
                    lock.unlock();
                }
            } catch (Throwable ex) {
                // error state.
                lock.lock();
                try {
                    workflowLatch_parser.dec();
                    documentError(resource, ex);
                    throw new Exception(ex);
                } finally {
                    lock.unlock();
                }
            }
            // done.
            if (log.isInfoEnabled())
                log.info("resource=" + resource + " : " + this);
            return null;
        }

    } // ParserTask

    /**
     * 
     * @param tripleStore
     * @param producerChunkSize
     *            The chunk size used when writing chunks onto the master for
     *            the asynchronous index write API. If this value is on the
     *            order of the #of terms or statements in the parsed documents,
     *            then all terms / statements will be written onto the master in
     *            one chunk. The master will split the chunk based on the
     *            separator keys for the index partitions and write splits onto
     *            the sink for each index partition. The master and sink
     *            configuration is specified via the {@link IndexMetadata} when
     *            the triple store indices are created.
     * @param valuesInitialCapacity
     *            The initial capacity of the map of the distinct RDF
     *            {@link Value}s parsed from a single document.
     * @param bnodesInitialCapacity
     *            The initial capacity of the map of the distinct RDF
     *            {@link BNode}s parsed from a single document.
     * @param defaultFormat
     * @param verifyData
     * @param deleteAfter
     * @param parserPoolSize
     *            The #of worker threads in the thread pool for parsing RDF
     *            documents.
     * @param parserQueueCapacity
     *            The capacity of the bounded work queue for the service running
     *            the parser tasks.
     * @param term2IdWriterPoolSize
     *            The #of worker threads in the thread pool for buffering
     *            asynchronous writes on the TERM2ID index.
     * @param otherWriterPoolSize
     *            The #of worker threads in the thread pool for buffering
     *            asynchronous index writes on the other indices.
     * @param bufferedStatementThreshold
     *            The maximum #of statements which can be buffered before new
     *            parser tasks are paused and the buffered writes are flushed to
     *            the database. This allows you to place a constraint on the RAM
     *            demand of the buffered documents.
     * 
     * @todo CDL still used for validation by some unit tests. Do a variant of
     *       this that does read-only TERM2ID requests and then validates the
     *       indices so we can drop the {@link ConcurrentDataLoader} class.
     */
    public AsynchronousStatementBufferFactory(//
            final ScaleOutTripleStore tripleStore,//
            final int producerChunkSize, //
            final int valuesInitialCapacity,//
            final int bnodesInitialCapacity, //
            final RDFFormat defaultFormat,//
            final boolean verifyData,//
            final boolean deleteAfter,//
            final int parserPoolSize,//
            final int parserQueueCapacity,//
            final int term2IdWriterPoolSize,//
            final int otherWriterPoolSize,//
            final long bufferedStatementThreshold//
            ) {

        if (tripleStore == null)
            throw new IllegalArgumentException();
        if (producerChunkSize <= 0)
            throw new IllegalArgumentException();
        if (valuesInitialCapacity <= 0)
            throw new IllegalArgumentException();
        if (bnodesInitialCapacity <= 0)
            throw new IllegalArgumentException();
        if (bufferedStatementThreshold <= 0)
            throw new IllegalArgumentException();
        
        this.tripleStore = tripleStore;
        
        this.lexiconRelation = tripleStore.getLexiconRelation();

        this.spoRelation = tripleStore.getSPORelation();

        this.producerChunkSize = producerChunkSize;

        this.valuesInitialCapacity = valuesInitialCapacity;
        
        this.bnodesInitialCapacity = bnodesInitialCapacity;
        
        this.defaultFormat  = defaultFormat;
        
        this.verifyData = verifyData;
        
        this.deleteAfter = deleteAfter;

        this.bufferedStatementThreshold = bufferedStatementThreshold;
        
        if (tripleStore.isStatementIdentifiers()) {

            throw new UnsupportedOperationException("SIDs not supported");
            
        }

        /*
         * Open the necessary buffers.
         * 
         * Note: Lock is required by reopenBuffer_xxx() methods.
         */
        lock.lock();
        try {

            reopenBuffer_term2Id();

            reopenBuffer_others();

        } finally {
            
            lock.unlock();
        
        }

        /*
         * Set iff this is a federation based triple store. The various queue
         * statistics are reported only for this case.
         */
        final AbstractFederation<?> fed;
        if (tripleStore.getIndexManager() instanceof AbstractFederation) {

            fed = (AbstractFederation<?>) tripleStore.getIndexManager();

        } else {

            fed = null;
        }

        /*
         * Note: This service must not reject tasks as long as the statement
         * buffer factory is open. It is configured with a bounded workQueue and
         * a bounded thread pool. The #of threads in the pool should build up to
         * the maximumPoolSize and idle threads will be retired, but only after
         * several minutes. 
         */

        parserService = new ParserThreadPoolExecutor(//
                1, // corePoolSize
                parserPoolSize, // maximumPoolSize
                4, // keepAliveTime
                TimeUnit.MINUTES, // keepAlive units.
                new LinkedBlockingQueue<Runnable>(parserQueueCapacity),// workQueue
                new DaemonThreadFactory(getClass().getName()+"_parserService") // threadFactory
        );

        /*
         * Note: This service MUST NOT block or reject tasks as long as the
         * statement buffer factory is open. It is configured with an
         * unbounded workQueue and a bounded thread pool. The #of threads in
         * the pool should build up to the maximumPoolSize and idle threads
         * will be retired, but only after several minutes.
         */

        term2IdWriterService = new ThreadPoolExecutor(//
                1, // corePoolSize
                term2IdWriterPoolSize, // maximumPoolSize
                4, // keepAliveTime
                TimeUnit.MINUTES, // keepAlive units.
                new LinkedBlockingQueue<Runnable>(/* unbounded */),// workQueue
                new DaemonThreadFactory(getClass().getName()+"_term2IdWriteService") // threadFactory
        );

        /*
         * Note: This service MUST NOT block or reject tasks as long as the
         * statement buffer factory is open. It is configured with an
         * unbounded workQueue and a bounded thread pool. The #of threads in
         * the pool should build up to the maximumPoolSize and idle threads
         * will be retired, but only after several minutes.
         */

        otherWriterService = new ThreadPoolExecutor(//
                1, // corePoolSize
                otherWriterPoolSize, // maximumPoolSize
                4, // keepAliveTime
                TimeUnit.MINUTES, // keepAlive units.
                new LinkedBlockingQueue<Runnable>(/* unbounded */),// workQueue
                new DaemonThreadFactory(getClass().getName()+"_otherWriteService") // threadFactory
        );

        if(deleteAfter) {
            
            deleteService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    1, new DaemonThreadFactory(getClass().getName()
                            + "_deleteService"));

        } else {
            
            deleteService = null;

        }

        /*
         * @todo If sampling should be done for non-federation cases then we
         * need to pass in the ScheduledExecutorService, expose a method to
         * start sampling on the caller's service, or create a
         * ScheduledExecutorService within this factory class.
         */
        serviceStatisticsTask = (fed == null ? null
                : new ServiceStatisticsTask(fed.getScheduledExecutorService()));

    } // ctor

    /**
     * {@link Runnable} samples the services and provides reporting via
     * {@link #getCounters()}.
     */
    private class ServiceStatisticsTask implements Runnable {

        private final Map<String, ThreadPoolExecutorBaseStatisticsTask> tasks = new LinkedHashMap<String, ThreadPoolExecutorBaseStatisticsTask>();
        
        private final ScheduledFuture<?> serviceStatisticsFuture;
        
        public ServiceStatisticsTask(final ScheduledExecutorService scheduledService) {

            /*
             * Add scheduled tasks to report the moving average of the queue
             * length, active count, etc. for the various services used by this
             * factory.
             */
            tasks.put("parserService",
                    new ThreadPoolExecutorBaseStatisticsTask(parserService));

            tasks.put("term2IdWriterService",
                    new ThreadPoolExecutorBaseStatisticsTask(
                            term2IdWriterService));

            tasks.put("otherWriterService",
                            new ThreadPoolExecutorBaseStatisticsTask(
                                    otherWriterService));

            if (deleteAfter) {

                tasks.put("deleteService",
                                new ThreadPoolExecutorBaseStatisticsTask(
                                        deleteService));

            }

            // schedule this task to sample performance counters.
            serviceStatisticsFuture = scheduledService.scheduleWithFixedDelay(
                    this, 0/* initialDelay */, 1000/* delay */,
                    TimeUnit.MILLISECONDS);

        }

        protected void finalize() throws Exception {

            cancel();

        }

        public void cancel() {

            serviceStatisticsFuture.cancel(true/* mayInterruptIfRunning */);

        }
        
        public void run() {

            for(Runnable r : tasks.values()) {

                try {

                    r.run();
                    
                } catch(Throwable t) {
                    
                    log.error(r,t);
                    
                }

            }

        }

        public CounterSet getCounters() {

            final CounterSet counterSet = new CounterSet();

            for (Map.Entry<String, ThreadPoolExecutorBaseStatisticsTask> e : tasks
                    .entrySet()) {

                counterSet.makePath(e.getKey()).attach(
                        e.getValue().getCounters());

            }

            return counterSet;

        }

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

        if (parserService.isTerminated())
            return true;

        if (term2IdWriterService.isTerminated())
            return true;

        if (otherWriterService.isTerminated())
            return true;

        if (deleteService != null && deleteService.isTerminated())
            return true;
                
        return false;
        
    }
    
    public void cancelAll(final boolean mayInterruptIfRunning) {

        if(log.isInfoEnabled())
            log.info("Cancelling futures.");
        
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

        notifyEnd();

    }

    /**
     * Awaits a signal that all documents which have queued writes are
     * finished and then closes the remaining buffers.
     */
    public void close() {
        
        log.info("");
        
        try {
            lock.lockInterruptibly();
            try {

                assertSumOfLatchs();
                
                // not decremented until doc fails parse or is doing TERM2ID writes.
                workflowLatch_parser.await();

                assertSumOfLatchs();

                /*
                 * No more tasks will request TIDs, so close the TERM2ID master.
                 * It will flush its writes. 
                 */
                bufferGuard_term2Id.await();
                {
                    if (buffer_t2id != null) {
                        if (log.isInfoEnabled()) {
                            log.info("Closing TERM2ID buffer.");
                        }
                        buffer_t2id.close();
                    }

                    workflowLatch_bufferTerm2Id.await();
                    term2IdWriterService.shutdown();
                    new ShutdownHelper(term2IdWriterService, 10L,
                            TimeUnit.SECONDS) {
                        protected void logTimeout() {
                            log
                                    .warn("Waiting for term2Id write service shutdown.");
                        }
                    };

                    assertSumOfLatchs();
                }

                /*
                 * No new index write tasks may start (and all should have
                 * terminated by now).
                 */
                bufferGuard_other.await();
                {
                    if (log.isInfoEnabled())
                        log.info("Closing remaining buffers.");

                    buffer_id2t.close();

                    if (buffer_text != null)
                        buffer_text.close();

                    buffer_spo.close();

                    if (buffer_pos != null)
                        buffer_pos.close();

                    if (buffer_osp != null)
                        buffer_osp.close();

                    workflowLatch_bufferOther.await();
                    otherWriterService.shutdown();
                    new ShutdownHelper(term2IdWriterService, 10L, TimeUnit.SECONDS) {
                        protected void logTimeout() {
                            log.warn("Waiting for term2Id write service shutdown.");
                        }
                    };

                    assertSumOfLatchs();

                }
                
                // wait for the global latch.
                workflowLatch_document.await();

                assertSumOfLatchs();

                if (deleteService != null) {
                    // shutdown and wait until all files have been delete.
                    deleteService.shutdown();
                    new ShutdownHelper(deleteService, 10L, TimeUnit.SECONDS) {
                        protected void logTimeout() {
                            log.warn("Waiting for delete service shutdown.");
                        }
                    };
                }

            } finally {
                lock.unlock();
                notifyEnd();
            }
            
        } catch (InterruptedException ex) {
            
            // @todo should declare this exception in the API.
            throw new RuntimeException(ex);
            
        }

    }

    public void awaitAll() throws InterruptedException, ExecutionException {

        if(log.isInfoEnabled())
            log.info("Start");

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

        if(log.isInfoEnabled())
            log.info("Done.");

    }

    /**
     * This method may be overridden to trigger additional processing when a
     * document has become restart safe on the database -- <strong>the
     * implementation MUST NOT block</strong>. One technique is to add the
     * <i>documentIdentifier</i> to an <em>unbounded</em> queue which is then
     * drained by another thread. The default implementation logs the event @
     * INFO.
     * 
     * @param resource
     *            The document identifier.
     */
    protected void documentDone(final String resource) {

        if (log.isInfoEnabled())
            log.info("resource=" + resource + " : "+this);

    }

    public String toString() {

        return super.toString() + "::" + getCounters();
        
    }
    
    /**
     * This method may be <em>extended</em> to trigger additional processing
     * when a document processing error has occurs -- <strong>the implementation
     * MUST NOT block</strong>. The default implementation logs the event @
     * ERROR.
     * 
     * @param resource
     *            The document identifier.
     * @param t
     *            The exception.
     */
    protected void documentError(final String resource, Throwable t) {

        lock.lock();
        try {

            documentErrorCount.incrementAndGet();
            
            /*
             * Note: this is responsible for decrementing the #of documents whose
             * processing is not yet complete. This must be done for each task whose
             * future is not watched. However, we MUST NOT do this twice for any
             * given document since that would mess with the counter. That counter
             * is critical as it forms part of the termination condition for the
             * total data load operation.
             */
            
            workflowLatch_document.dec();

        } finally {
            
            lock.unlock();
            
        }

        /*
         * Note: log error since no one watches the future for this task.
         */

        log.error("resource=" + resource + " : " + this, t);

    }

    public CounterSet getCounters() {
        
        final CounterSet counterSet = new CounterSet();

        /**
         * The elapsed milliseconds.
         */
        counterSet.addCounter("elapsedMillis", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(getElapsedMillis());
            }
        });

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
         * The #of documents whose TERM2ID writes are restart-safe on the
         * database. Each parser thread will block until the TERM2ID writes are
         * done and then proceed to write on the remaining indices.
         */
        counterSet.addCounter("documentTIDsReadyCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(documentTIDsReadyCount.get());
            }
        });

        /**
         * The #of triples written on the SPO index (this does not count triples
         * that were already present on the index).
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
        counterSet.addCounter("toldTriplesRestartSafeCount",
                new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(toldTriplesRestartSafeCount.get());
                    }
                });

        /**
         * The told triples per second rate which have been made restart safe by
         * this factory object. When you are loading using multiple clients,
         * then the total told triples per second rate is the aggregation across
         * all of those instances.
         */
        counterSet.addCounter("toldTriplesRestartSafePerSec",
                new Instrument<Long>() {

                    @Override
                    protected void sample() {

                        final long elapsed = getElapsedMillis();

                        final double tps = (long) (((double) toldTriplesRestartSafeCount
                                .get())
                                / ((double) elapsed) * 1000d);

                        setValue((long) tps);

                    }
                });

        /**
         * The #of documents which have been processed by this client and are
         * restart safe on the database by this client.
         */
        counterSet.addCounter("documentRestartSafeCount",
                new Instrument<Long>() {
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

        /*
         * The latches are used to guard the termination conditions for the
         * factory. If they are non-zero the factory can not terminate normally.
         */
        {

            final CounterSet workflowLatchSet = counterSet.makePath("workflowLatch");

            workflowLatchSet.addCounter("parser", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(workflowLatch_parser.get());
                }
            });

            workflowLatchSet.addCounter("bufferTerm2Id", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(workflowLatch_bufferTerm2Id.get());
                }
            });

            workflowLatchSet.addCounter("bufferOther", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(workflowLatch_bufferOther.get());
                }
            });

            // latch over the total life cycle for a document.
            workflowLatchSet.addCounter("document", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(workflowLatch_document.get());
                }
            });

        } // latches

        /**
         * Latches used to guard the buffers and close them in a timely manner.
         */
        {
            
            final CounterSet bufferGuardSet = counterSet.makePath("bufferGuard");

            bufferGuardSet.addCounter("guardTerm2Id", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(bufferGuard_term2Id.get());
                }
            });

            bufferGuardSet.addCounter("guardOther", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(bufferGuard_other.get());
                }
            });
            
        }

        /*
         * Counters pertaining to the logic which suspects new parser task
         * requests if too many statements are currently buffered.
         */
        {
            
            final CounterSet pauseSet = counterSet.makePath("pause");

            // The #of buffered RDF Statements.
            pauseSet.addCounter("bufferedStatementCount",
                    new Instrument<Long>() {
                        @Override
                        protected void sample() {
                            setValue(bufferedStatementCount.get());
                        }
                    });

            // The maximum #of statements before we suspend new parse requests.
            pauseSet.addCounter("bufferedStatementThreshold",
                    new Instrument<Long>() {
                        @Override
                        protected void sample() {
                            setValue(bufferedStatementThreshold);
                        }
                    });

            // The #of suspended parse request threads (current value).
            pauseSet.addCounter("pausedThreadCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(pausedThreadCount.get());
                }
            });

            // The #of suspended parse request threads (cumulative).
            pauseSet.addCounter("poolPausedCount", new Instrument<Long>() {
                @Override
                protected void sample() {
                    setValue(poolPausedCount.get());
                }
            });

        }
        
        // services
        {
            
            counterSet.makePath("services").attach(
                    serviceStatisticsTask.getCounters());
            
        }
        
        return counterSet;

    }

    /**
     * {@link Runnable} class applies the factory to either a single file or to
     * all files within a directory.
     */
    private class RunnableFileSystemLoader implements Callable<Integer> {

        volatile boolean done = false;

        private int count = 0;

        final File fileOrDir;

        final FilenameFilter filter;

        /**
         * 
         * @param fileOrDir
         *            The file or directory to be loaded.
         * @param filter
         *            An optional filter on files that will be accepted when
         *            processing a directory.
         */
        public RunnableFileSystemLoader(final File fileOrDir,
                final FilenameFilter filter) {

            if (fileOrDir == null)
                throw new IllegalArgumentException();

            this.fileOrDir = fileOrDir;

            this.filter = filter; // MAY be null.

        }

        /**
         * Creates a task using the {@link #taskFactory}, submits it to the
         * {@link #loader} and and waits for the task to complete. Errors are
         * logged, but not thrown.
         * 
         * @throws RuntimeException
         *             if interrupted.
         */
        public Integer call() throws Exception {

            process2(fileOrDir);

            return count;

        }

        /**
         * Scans file(s) recursively starting with the named file, and, for each
         * file that passes the filter, submits the task.
         * 
         * @param file
         *            Either a URL, a plain file or directory containing files
         *            to be processed.
         * 
         * @throws InterruptedException
         *             if the thread is interrupted while queuing tasks.
         */
        private void process2(final File file) throws InterruptedException {

            if (file.isHidden()) {

                // ignore hidden files.
                return;

            }

            if (file.isDirectory()) {

                if (log.isInfoEnabled())
                    log.info("Scanning directory: " + file);

                // filter is optional.
                final File[] files = filter == null ? file.listFiles() : file
                        .listFiles(filter);

                for (final File f : files) {

                    process2(f);

                }

            } else {

                /*
                 * Processing a standard file.
                 */

                try {

                    submitOne(file.getPath());

                    count++;

                } catch (InterruptedException ex) {

                    throw ex;

                } catch (Exception ex) {

                    log.error(file, ex);

                }

            }

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
         *            if readOnly was specified for the {@link Term2IdWriteProc}
         *            .
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

                    if (chunk[i] instanceof KVOList) {

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
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
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
        public AsyncTerm2IdIndexWriteTask(final KVOLatch latch,
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
            final LexiconKeyBuilder keyBuilder = tupleSer
                    .getLexiconKeyBuilder();

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
                        // {
                        // @Override
                        // public void done() {
                        // /*
                        // * verify that the term identifier is assigned
                        // * before we decrement the latch.
                        // */
                        // if (obj.getTermId() == IRawTripleStore.NULL)
                        // throw new AssertionError("No termid? "
                        // + this);
                        // super.done();
                        // }
                        // }
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
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
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
        public AsyncId2TermIndexWriteTask(final KVOLatch latch,
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
            final BigdataValueSerializer<BigdataValue> ser = valueFactory
                    .getValueSerializer();

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
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class AsyncSPOIndexWriteTask implements Callable<Void> {

        final protected transient static Logger log = Logger
                .getLogger(AsyncSPOIndexWriteTask.class);

        private final KVOLatch latch;

        private final IKeyOrder<ISPO> keyOrder;

        /* Note: problem with java 1.6.0_07 and _12 on linux when typed. */
        private final IChunkedOrderedIterator/* <ISPO> */src;

        private final IRunnableBuffer<KVO<ISPO>[]> writeBuffer;

        private final SPOTupleSerializer tupleSer;

        public AsyncSPOIndexWriteTask(final KVOLatch latch,
                final IKeyOrder<ISPO> keyOrder, final SPORelation spoRelation,
                /* Note: problem with java 1.6.0_07 and _12 on linux when typed. */
                final IChunkedOrderedIterator/* <ISPO> */src,
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
     * Inner class provides the statement buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <S>
     * @param <F>
     */
    protected class AsynchronousStatementBufferImpl implements
            IStatementBuffer<S> {

        /** The document identifier. */
        private final String resource;

        private final AbstractTripleStore database;

        private final BigdataValueFactory valueFactory;

        /**
         * A canonicalizing map for RDF {@link Value}s. The use of this map
         * provides a ~40% performance gain.
         */
        private LinkedHashMap<Value, BigdataValue> values;

        /**
         * A canonicalizing map for blank nodes. This map MUST be cleared before
         * you begin to add statements to the buffer from a new "source"
         * otherwise it will co-reference blank nodes from distinct sources. The
         * life cycle of the map is the life cycle of the document being loaded,
         * so if you are loading a large document with a lot of blank nodes the
         * map will also become large.
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
         * Note: This implementation does not support the concept of a
         * focusStore so it can not be used for truth maintenance.
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
        public String getDocumentIdentifier() {

            return resource;

        }

        /**
         * @param resource
         *            The document identifier.
         */
        protected AsynchronousStatementBufferImpl(final String resource) {

            this.resource = resource;

            this.database = AsynchronousStatementBufferFactory.this.tripleStore;

            this.valueFactory = database.getValueFactory();

        }

        /**
         * Note: this implementation always returns ZERO (0).
         * 
         * @see ParserTask
         */
        public long flush() {

            return 0L;

        }

        /**
         * Clears all buffered data, including the canonicalizing mapping for
         * blank nodes and deferred provenance statements.
         */
        public void reset() {

            if (log.isInfoEnabled())
                log.info("resource=" + getDocumentIdentifier());

            /*
             * Note: clear the reference NOT the contents of the map! This makes
             * it possible for the caller to reuse the same map across multiple
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

            add(e.getSubject(), e.getPredicate(), e.getObject(),
                    e.getContext(),
                    (e instanceof BigdataStatement ? ((BigdataStatement) e)
                            .getStatementType() : null));

        }

        /**
         * Canonicalizing mapping for blank nodes.
         * <p>
         * Note: This map MUST stay in effect while reading from a given source
         * and MUST be cleared (or set to null) before reading from another
         * source.
         */
        private BigdataBNodeImpl getCanonicalBNode(final BigdataBNodeImpl bnode) {

            // the BNode's ID.
            final String id = bnode.getID();

            if (bnodes == null) {

                /*
                 * Allocate a canonicalizing map for blank nodes. Since this
                 * will be a private map it does not need to be thread-safe.
                 */
                bnodes = new HashMap<String, BigdataBNodeImpl>(
                        bnodesInitialCapacity);

                // fall through.

            }

            /*
             * Specialized for a concurrent hash map.
             */
            if (bnodes instanceof ConcurrentHashMap) {

                final BigdataBNodeImpl tmp = ((ConcurrentHashMap<String, BigdataBNodeImpl>) bnodes)
                        .putIfAbsent(id, bnode);

                if (tmp != null) {

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
         * Note: Blank nodes are made canonical with the scope of the source
         * from which the data are being read. See {@link #bnodes}. All other
         * kinds of terms are made canonical within the scope of the buffer's
         * current contents in order to keep down the demand on the heap with
         * reading either very large documents or a series of small documents.
         * 
         * @param term
         *            A term.
         * 
         * @return Either the term or the pre-existing term in the buffer with
         *         the same data.
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
                 * Note: This also records the blank node in the values map so
                 * that we can process the values map without having to consider
                 * the blank nodes as well.
                 */

            } else {

                // not a blank node.
                term = term0;

            }

            if (values == null) {

                /*
                 * Create a private (non-thread safe) canonicalizing mapping for
                 * RDF Values.
                 * 
                 * Note: A linked hash map is used to make the iterator faster.
                 */

                values = new LinkedHashMap<Value, BigdataValue>(
                        valuesInitialCapacity);

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
        private void handleStatement(final Resource s, final URI p,
                final Value o, final Resource c, final StatementEnum type) {

            _handleStatement(
                    (Resource) getCanonicalValue((BigdataResource) valueFactory
                            .asValue(s)),//
                    (URI) getCanonicalValue((BigdataURI) valueFactory
                            .asValue(p)),//
                    (Value) getCanonicalValue((BigdataValue) valueFactory
                            .asValue(o)),//
                    (Resource) getCanonicalValue((BigdataResource) valueFactory
                            .asValue(c)), //
                    type);

        }

        /**
         * Form the BigdataStatement object using the valueFactory now that we
         * bindings which were (a) allocated by the valueFactory and (b) are
         * canonical for the scope of this document.
         */
        private void _handleStatement(final Resource s, final URI p,
                final Value o, final Resource c, final StatementEnum type) {

            final BigdataStatement stmt = valueFactory.createStatement(
                    (BigdataResource) s, (BigdataURI) p, (BigdataValue) o,
                    (BigdataResource) c, type);

            if (statements == null) {

                statements = new UnsynchronizedUnboundedChunkBuffer<S>(
                        producerChunkSize);

            }

            statements.add((S) stmt);

            // total #of statements accepted.
            statementCount++;

            if (log.isDebugEnabled())
                log.debug("n=" + statementCount + ", added: " + stmt);

        }

        /**
         * Buffers the asynchronous writes on the TERM2ID index.
         * 
         * @throws Exception
         */
        private void bufferTerm2IdWrites() throws Exception {

            if (log.isInfoEnabled()) {
                log.info("bnodeCount=" + (bnodes == null ? 0 : bnodes.size())
                        + ", values=" + values.size() + ", statementCount="
                        + statementCount);
            }

            if (isAnyDone()) {

                throw new RuntimeException("Factory closed?");

            }

            /*
             * Note: This is responsible for assigning the TIDs (term
             * identifiers) to the {@link BigdataValue}s. We CAN NOT write on
             * the other indices until we have those TIDs.
             * 
             * Run task which will queue BigdataValue[] chunks onto the TERM2ID
             * async write buffer.
             * 
             * Note: If there is not enough load being placed the async index
             * write then it can wait up to its idle/chunk timeout. Normally we
             * want to use an infinite chunk timeout so that all chunks written
             * on the index partitions are as full as possible. Therefore, the
             * TERM2ID async writer should use a shorter idle timeout or it can
             * live lock. Ideally, there should be some explicit notice when we
             * are done queuing writes on TERM2ID across all source documents.
             * Even then we can live lock if the input queue is not large
             * enough.
             */

            /*
             * Latch notifies us when all writes for _this_ document on TERM2ID
             * are complete such that we have the assigned term identifiers for
             * all BigdataValues appearing in the document. This event is used
             * to transfer the document to another queue.
             */
            final KVOLatch tidsLatch = new KVOLatch() {

                public String toString() {

                    return super.toString() + " : tidsLatch";

                }

                @Override
                protected void signal() throws InterruptedException {

                    super.signal();

                    documentTIDsReadyCount.incrementAndGet();

                    otherWriterService.submit(new BufferOtherWritesTask(
                            AsynchronousStatementBufferImpl.this));

                }

            };

            // pre-increment to avoid notice on transient zeros.
            tidsLatch.inc();

            try {

                final Callable<Void> task = new AsyncTerm2IdIndexWriteTask(
                        tidsLatch, lexiconRelation, newT2IdIterator(values
                                .values().iterator(), producerChunkSize),
                        buffer_t2id);

                // queue chunks onto the write buffer.
                task.call();

            } finally {

                /*
                 * Decrement now that all chunks have been queued for
                 * asynchronous writes.
                 */

                tidsLatch.dec();

            }

            /*
             * Note: At this point the writes on TERM2ID have been buffered.
             */

        }

        /**
         * Buffers write requests for the remaining indices (everything except
         * TERM2ID).
         * 
         * @throws InterruptedException
         * @throws ExecutionException
         */
        private void bufferOtherWrites() throws InterruptedException,
                ExecutionException {

            if (log.isDebugEnabled()) {

                log.debug("Writing on remaining indices.");

            }

            /*
             * Setup tasks which can run asynchronously. These tasks have no
             * dependencies. They can each proceed at their own rate. However,
             * we can not return from within this method until they are all
             * done.
             * 
             * Note: Each task runs in parallel.
             * 
             * Note: Each task uses the asynchronous write API. When the Future
             * for that task is complete all it means is that the data are now
             * buffered on the asynchronous write buffer for the appropriate
             * index. It DOES NOT mean that those writes are complete. However,
             * the [documentStableLatch] DOES indicate when the data is restart
             * safe.
             * 
             * Note: These tasks all process iterators. This approach was chosen
             * to isolate the tasks (which queue data for asynchronous writes)
             * from the data structures in this IStatementBuffer implementation.
             * An example of something which WOULD NOT work is if these tasks
             * were inner classes accessing the instance fields on this class
             * since reset() would clear those fields which might cause
             * spontaneous failures within ongoing processing.
             */
            final List<Callable> tasks = new LinkedList<Callable>();

            /*
             * The #of triples parsed from this document. This is added to the
             * total #of restart safe told triples loaded by this client when
             * the latch is triggered. Of course, the actual #of triples on the
             * database is only available by querying the database since the
             * same triple can occur in more than one document, and documents
             * are loaded by distributed clients so there is no way to correct
             * for such duplicate told triples short of querying the database.
             */
            final int toldTriplesThisDocument = statementCount;

            /*
             * Latch is signaled when all data buffered for this document is
             * RESTART SAFE on the database.
             * 
             * Note: In order for the latch to have those semantics we have to
             * include it on each KVO object buffered for all remaining indices.
             * The semantics are valid in the presence of duplicate removes IFF
             * they obey the contract for KVOList and link together the
             * duplicates such that the latch is decremented for each distinct
             * KVOC instance, including those which were eliminated as
             * duplicates.
             */
            final KVOLatch documentRestartSafeLatch = new KVOLatch() {

                public String toString() {

                    return super.toString() + " : documentRestartSafeLatch";

                }

                @Override
                protected void signal() throws InterruptedException {

                    super.signal();

                    lock.lock();
                    try {
                        workflowLatch_bufferOther.dec();
                        workflowLatch_document.dec();
                        assertSumOfLatchs();
                        documentRestartSafeCount.incrementAndGet();
                        toldTriplesRestartSafeCount
                                .addAndGet(toldTriplesThisDocument);
                        bufferedStatementCount
                                .addAndGet(-toldTriplesThisDocument);
                    } finally {
                        lock.unlock();
                    }

                    // notify that the document is done.
                    AsynchronousStatementBufferFactory.this
                            .documentDone(getDocumentIdentifier());

                }

            };

            tasks.add(new AsyncId2TermIndexWriteTask(documentRestartSafeLatch,
                    valueFactory, newId2TIterator(values.values().iterator(),
                            producerChunkSize), buffer_id2t));

            if (buffer_text != null) {

                // FIXME full text index.
                throw new UnsupportedOperationException();
                // tasks.add(new AsyncTextWriteTask());

            }

            tasks.add(new AsyncSPOIndexWriteTask(documentRestartSafeLatch,
                    SPOKeyOrder.SPO, spoRelation,
                    // (IChunkedOrderedIterator<ISPO>)
                    statements.iterator(), buffer_spo));

            if (buffer_pos != null) {

                tasks.add(new AsyncSPOIndexWriteTask(documentRestartSafeLatch,
                        SPOKeyOrder.POS, spoRelation,
                        // (IChunkedOrderedIterator<ISPO>)
                        statements.iterator(), buffer_pos));

            }

            if (buffer_osp != null) {

                tasks.add(new AsyncSPOIndexWriteTask(documentRestartSafeLatch,
                        SPOKeyOrder.OSP, spoRelation,
                        // (IChunkedOrderedIterator<ISPO>)
                        statements.iterator(), buffer_osp));

            }

            /*
             * Submit all tasks. They will run in parallel. If they complete
             * successfully then all we know is that the data has been buffered
             * for asynchronous writes on the various indices.
             * 
             * Note: java 1.6.0_07/12 build problems under linux when typed as
             * <Future> or any other combination that I have tried.
             */
            final List futures;

            /*
             * This latch is incremented _before_ buffering writes, and within
             * each routine that buffers writes, to avoid false triggering. This
             * is done to ensure that the latch will be positive until we exit
             * the try / finally block. We do this around the submit of the
             * tasks and do not decrement the latch until the futures are
             * available so we known that all data is buffered.
             */
            documentRestartSafeLatch.inc();
            try {

                futures = tripleStore.getExecutorService().invokeAll(
                        (List) tasks);

            } finally {

                // decrement so that the latch can be triggered.
                documentRestartSafeLatch.dec();

            }

            // make sure that no errors were reported by those tasks.
            for (Object f : futures) {

                ((Future) f).get();

            }

            /*
             * At this point all writes have been buffered. We now discard the
             * buffered data (RDF Values and statements) since it will no longer
             * be used.
             */
            reset();

        }

    }// StatementBuffer impl.

    /**
     * Task buffers the asynchronous writes on the TERM2ID index.
     */
    private class BufferTerm2IdWrites implements Callable<Void> {

        private final AsynchronousStatementBufferImpl buffer;

        public BufferTerm2IdWrites(AsynchronousStatementBufferImpl buffer) {

            if (buffer == null)
                throw new IllegalArgumentException();

            this.buffer = buffer;

        }
        
        public Void call() throws Exception {

            // new workflow state.
            lock.lock();
            try {
                bufferGuard_term2Id.inc();
                workflowLatch_parser.dec();
                workflowLatch_bufferTerm2Id.inc();
                assertSumOfLatchs();
            } finally {
                lock.unlock();
            }

            try {

                buffer.bufferTerm2IdWrites();

                lock.lock();
                try {
                    bufferGuard_term2Id.dec();
                } finally {
                    lock.unlock();
                }
                
                return null;
                
            } catch (Throwable t) {

                lock.lock();
                try {
                    bufferGuard_term2Id.dec();
                    workflowLatch_bufferTerm2Id.dec();
                    documentError(buffer.getDocumentIdentifier(), t);
                    bufferedStatementCount.addAndGet(-buffer.statementCount);
                    throw new Exception(t);
                } finally {
                    lock.unlock();
                }

            }

        }

    }

    /**
     * Task which buffers index writes for the remaining indices (everything
     * other than TERM2ID).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class BufferOtherWritesTask implements Callable<Void> {

        private final AsynchronousStatementBufferImpl buffer;

        public BufferOtherWritesTask(AsynchronousStatementBufferImpl buffer) {

            if (buffer == null)
                throw new IllegalArgumentException();

            this.buffer = buffer;

        }

        public Void call() throws Exception {

            // new workflow state.
            lock.lock();
            try {
                bufferGuard_other.inc();
                workflowLatch_bufferTerm2Id.dec();
                workflowLatch_bufferOther.inc();
                assertSumOfLatchs();
            } finally {
                lock.unlock();
            }

            try {

                buffer.bufferOtherWrites();

                lock.lock();
                try {
                    bufferGuard_other.dec();
                } finally {
                    lock.unlock();
                }
                
                return null;
                
            } catch (Throwable t) {

                lock.lock();
                try {
                    bufferGuard_other.dec();
                    workflowLatch_bufferOther.dec();
                    documentError(buffer.getDocumentIdentifier(), t);
                    bufferedStatementCount.addAndGet(-buffer.statementCount);
                    throw new Exception(t);
                } finally {
                    lock.unlock();
                }

            }

        }

    }

    /**
     * Thread pool with pause/resume semantics based on the amount of buffered
     * state for the outer class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class ParserThreadPoolExecutor extends ThreadPoolExecutor {

        /**
         * @param corePoolSize
         * @param maximumPoolSize
         * @param keepAliveTime
         * @param unit
         * @param workQueue
         * @param threadFactory
         */
        public ParserThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                long keepAliveTime, TimeUnit unit,
                BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {

            super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                    workQueue, threadFactory);

        }
        
        /**
         * <code>true</code> if worker tasks must wait in
         * {@link #beforeExecute(Thread, Runnable)}
         */
        private boolean isPaused() {

            return bufferedStatementCount.get() >= bufferedStatementThreshold;

        }

        /**
         * Overridden to have worker threads pause if {@link #isPaused()}
         * returns true.
         * 
         * @param t
         *            The thread that will run the task.
         * @param r
         *            The {@link Runnable} wrapping the {@link AbstractTask} -
         *            this is actually a {@link FutureTask}. See
         *            {@link AbstractExecutorService}.
         */
        protected void beforeExecute(final Thread t, final Runnable r) {

            // Note: [r] is the FutureTask.

            lock.lock();

            try {

                if(isPaused()) {
                    
                    pausedThreadCount.incrementAndGet();

                    poolPausedCount.incrementAndGet();
                    
                    System.err.println("PAUSE : "+AsynchronousStatementBufferFactory.this.toString());
                    
                    while (isPaused()) {

                        unpaused.await();

                    }

                    pausedThreadCount.decrementAndGet();

                    System.err.println("RESUME: "+AsynchronousStatementBufferFactory.this.toString());

                }

            } catch (InterruptedException ie) {

                t.interrupt();

            } finally {

                lock.unlock();

            }

            super.beforeExecute(t, r);

        }

    }
    
} // StatementBufferFactory impl.
