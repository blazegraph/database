/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.jsr166.LinkedBlockingQueue;
import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.ChangeRecord;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * A write buffer for absorbing the output of the RIO parser or other
 * {@link Statement} source and writing that output onto an
 * {@link AbstractTripleStore} using the batch API.
 * <p>
 * Note: there is a LOT of {@link Value} duplication in parsed RDF and we get a
 * significant reward for reducing {@link Value}s to only the distinct
 * {@link Value}s during processing. On the other hand, there is little
 * {@link Statement} duplication. Hence we pay an unnecessary overhead if we try
 * to make the statements distinct in the buffer.
 * <p>
 * Note: This also provides an explanation for why neither this class nor writes
 * of SPOs do better when "distinct" statements is turned on - the "Value"
 * objects in that case are only represented by long integers and duplication in
 * their values does not impose a burden on either the heap or the index
 * writers. In contrast, the duplication of {@link Value}s in the
 * {@link StatementBuffer} imposes a burden on both the heap and the index
 * writers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class StatementBuffer<S extends Statement> implements IStatementBuffer<S>, ICounterSetAccess {

    final private static Logger log = Logger.getLogger(StatementBuffer.class);
   
//    final static private boolean INFO = log.isInfoEnabled();
    final static private boolean DEBUG = log.isDebugEnabled();
    
    /**
     * Buffer for parsed RDF {@link Value}s.
     */
    protected final BigdataValue[] values;
    
    /**
     * Buffer for parsed RDF {@link Statement}s.
     */
    protected final BigdataStatement[] stmts;

    /**
     * #of valid entries in {@link #values}.
     */
    protected int numValues;
    
    /**
     * #of valid entries in {@link #stmts}.
     */
    protected int numStmts;

	/**
	 * The total number of statements accepted by the {@link StatementBuffer}.
	 * This can include statements that are currently buffered as well as those
	 * that have already been queued or written. This is a running total and
	 * does not attempt to avoid counting duplicates.
	 */
    private long numTotalStmts;
    
    /**
     * @todo consider tossing out these counters - they only add complexity to
     * the code in {@link #handleStatement(Resource, URI, Value, StatementEnum)}.
     */
    protected int numURIs, numLiterals, numBNodes;
    
    /**
     * The #of blank nodes which appear in the context position and zero (0) if
     * statement identifiers are not enabled.
     */
    protected int numSIDs;
    
    /**
     * Map used to filter out duplicate terms.  The use of this map provides
     * a ~40% performance gain.
     */
	final private Map<Value, BigdataValue> distinctTermMap;

    /**
     * A canonicalizing map for blank nodes. This map MUST be cleared before you
     * begin to add statements to the buffer from a new "source" otherwise it
     * will co-reference blank nodes from distinct sources. The life cycle of
     * the map is the life cycle of the document being loaded, so if you are
     * loading a large document with a lot of blank nodes the map will also
     * become large.
     */
    private Map<String, BigdataBNode> bnodes;
    
    /**
     * The #of blank nodes, which are not resolved and thus will require adding
     * to values array while running {@link #incrementalWrite()} 
	 * @see https://jira.blazegraph.com/browse/BLZG-1708 (DataLoader fails with ArrayIndexOutOfBoundsException)
     */
    private int bnodesTotalCount;

    /**
     * The #of blank nodes, which were resolved and thus will not require adding
     * to values array while running {@link #incrementalWrite()} 
     * This variable is made volatile, as it is updated from {@link DrainQueueCallable}
     * and should be available at the time of access from {@link #nearCapacity()} 
	 * @see https://jira.blazegraph.com/browse/BLZG-1708 (DataLoader fails with ArrayIndexOutOfBoundsException)
     */
    private volatile int bnodesResolvedCount;

    /**
     * Statements which use blank nodes in their {s,p,o} positions must be
     * deferred when statement identifiers are enabled until (a) either the
     * blank node is observed in the context position of a statement; or (b)
     * {@link #flush()} is invoked, indicating that no more data will be loaded
     * from the current source and therefore that the blank node is NOT a
     * statement identifier. This map is used IFF statement identifiers are
     * enabled. When statement identifiers are NOT enabled blank nodes are
     * always blank nodes and we do not need to defer statements, only maintain
     * the canonicalizing {@link #bnodes} mapping.
     */
    private Set<BigdataStatement> deferredStmts;
    
    /**
     * RDR statements.  Map to a bnode used in other statements.  Need to defer
     * both the reified statement (since it comes in piecemeal) and the 
     * statements about it (since we need to make sure the ground version is 
     * present).
     */
    private Map<BigdataBNodeImpl, ReifiedStmt> reifiedStmts;

    /**
     * <code>true</code> if statement identifiers are enabled.
     * <p>
     * Note: This is set by the ctor but temporarily overridden during
     * {@link #processDeferredStatements()} in order to reuse the
     * {@link StatementBuffer} for batch writes of the deferred statement as
     * well.
     * 
     * @see AbstractTripleStore#getStatementIdentifiers()
     */
    private boolean statementIdentifiers;
    
    /**
	 * When non-<code>null</code> the statements will be written on this store.
	 * When <code>null</code> the statements are written onto the
	 * {@link #database}. (This is used to support incremental truth
	 * maintenance.)
	 */
    private final AbstractTripleStore statementStore;

    /**
     * The optional store into which statements will be inserted when non-
     * <code>null</code>.
     */
    @Override
    public final AbstractTripleStore getStatementStore() {

        return statementStore;

    }

    /**
     * The database that will be used to resolve terms. When
     * {@link #statementStore} is <code>null</code>, statements will be written
     * into this store as well.
     */
    protected final AbstractTripleStore database;

    /**
     * The arity of the SPORelation for the {@link #getDatabase()}.
     */
    private final int arity;

    /**
     * The database that will be used to resolve terms. When
     * {@link #getStatementStore()} is <code>null</code>, statements will be
     * written into this store as well.
     */
    @Override
    public final AbstractTripleStore getDatabase() {
        
        return database;
        
    }

    protected final BigdataValueFactory valueFactory;

    /**
     * Reification vocabulary.
     */
    private final BigdataURI RDF_SUBJECT;
    private final BigdataURI RDF_PREDICATE;
    private final BigdataURI RDF_OBJECT;
    private final BigdataURI RDF_STATEMENT;
    private final BigdataURI RDF_TYPE;
    
    /**
     * The maximum #of Statements, URIs, Literals, or BNodes that the buffer can
     * hold. The minimum capacity is three (3) since that corresponds to a
     * single triple where all terms are URIs.
     */
	private final int bufferCapacity;

    /**
     * The maximum #of Statements, URIs, Literals, or BNodes that the buffer can
     * hold. The minimum capacity is three (3) since that corresponds to a
     * single triple where all terms are URIs.
     */
	public int getCapacity() {
		
		return bufferCapacity;
		
	}
	
//    /**
//     * When true only distinct terms are stored in the buffer (this is always
//     * true since this condition always outperforms the alternative).
//     */
//    protected final boolean distinct = true;
    
    /**
	 * The capacity of the optional {@link #queue} used to overlap the parser
	 * with the index writer -or- ZERO (0) iff the queue is disabled and index
	 * writes will be synchronous and alternate with the parser (the historical
	 * behavior).
	 */
	private final int queueCapacity;

	/**
	 * The #of batches added to the {@link #queue}.
	 */
	private int batchAddCount;
	
	/**
	 * The #of batches taken from the {@link #queue}.
	 */
	private int batchTakeCount;

	/**
	 * The number of batches merged.
	 */
	private int batchMergeCount;

	/**
	 * The #of batches written onto the database.
	 */
	private int batchWriteCount;
	
    /**
	 * When non-null, this is a deque that will be used allow the parser to race
	 * ahead. Once the writes on the statement indices are done, the queue can
	 * be drained to a thread that will then merge the batches and batch them
	 * through to the database.
	 * 
	 * @see BLZG-641
	 * @see BLZG-1522
	 */
    private final LinkedBlockingQueue<Batch<S>> queue;

    /**
	 * When non-null, this is a single threaded executor that will be used to
	 * drain {@link #queue} and batch updates through to the database.
	 */
    private final Executor executor;

    /**
	 * When the {@link #queue} is being used, this is the {@link Future} of the
	 * current task (if any) that is writing the current {@link Batch} onto the
	 * database.
	 * <p>
	 * Note: This is lazily initialized since {@link #reset()} does not have the
	 * semantics of "close()" and the {@link StatementBuffer} MIGHT be reused.
	 */
	private volatile FutureTask<Void> ft;

	/**
	 * The capacity of the optional queue used to overlap the parser with the
	 * index writer -or- ZERO (0) iff the queue is disabled and index writes
	 * will be synchronous and alternate with the parser (the historical
	 * behavior).
	 * 
	 * @see BLZG-1552
	 */
	public int getQueueCapacity() {
		
		return queueCapacity;
		
	}
	
    @Override
    public boolean isEmpty() {
        
        return numStmts == 0;
        
    }
    
    @Override
    public int size() {
        
        return numStmts;
        
    }
    
    @Override
    public String toString() {
    	
    	return "numURIs=" + numURIs
    			+ ", numLiterals=" + numLiterals
    			+ ", numBNodes=" + numBNodes
    			+ ", numStmts=" + numStmts
    			+ ", numValues=" + numValues
    			+ ", numSids=" + numSIDs
    			+ ", values.length=" + (values != null ? String.valueOf(values.length) : "null")
    			+ ", stmts.length=" + (stmts != null ? String.valueOf(stmts.length) : "null")
    			+ ", bnodes.size()=" + (bnodes != null ? String.valueOf(bnodes.size()) : "null")
    			+ ", distinctTermMap.size()=" + (distinctTermMap != null ? String.valueOf(distinctTermMap.size()) : "null")
    			+ ", reifiedStmts.size()=" + (reifiedStmts != null ? String.valueOf(reifiedStmts.size()) : "null")
    			+ ", deferredStmts.size()=" + (deferredStmts != null ? String.valueOf(deferredStmts.size()) : "null")//
				+ (queue == null ? "" : ", queue.size=" + queue.size())//
    			;
    	
    }

	@Override
	public CounterSet getCounters() {

		final CounterSet counters = new CounterSet();

		counters.addCounter("readOnly", new OneShotInstrument<Boolean>(readOnly));

		counters.addCounter("bnodesSize", new Instrument<Integer>() {
			@Override
			public void sample() {
				final Map<String, BigdataBNode> t = bnodes;
				if (t != null)
					setValue(t.size());
			}
		});
		
		counters.addCounter("bnodesTotalCount", new Instrument<Integer>() {
			@Override
			public void sample() {
				setValue(bnodesTotalCount);
			}
		});

		counters.addCounter("bnodesResolvedCount", new Instrument<Integer>() {
			@Override
			public void sample() {
				setValue(bnodesResolvedCount);
			}
		});
		
		counters.addCounter("distinctTermMapSize", new Instrument<Integer>() {
			@Override
			public void sample() {
				final Map<Value, BigdataValue> t = distinctTermMap;
				if (t != null)
					setValue(t.size());
			}
		});
		
		counters.addCounter("bufferCapacity", new OneShotInstrument<Integer>(bufferCapacity));

		// Note: tracked even when the queue is not enabled.
		counters.addCounter("batchAddCount", new Instrument<Integer>() {
			@Override
			public void sample() {
				setValue(batchAddCount);
			}
		});

		// Note: tracked even when the queue is not enabled.
		counters.addCounter("batchWriteCount", new Instrument<Integer>() {
			@Override
			public void sample() {
				setValue(batchWriteCount);
			}
		});

		if (queue != null) {

			// Only defined when the queue is enabled.
			
			counters.addCounter("queueCapacity", new OneShotInstrument<Integer>(queueCapacity));

			counters.addCounter("queueSize", new Instrument<Integer>() {
				@Override
				public void sample() {
					final LinkedBlockingQueue<Batch<S>> t = queue;
					if (t != null)
						setValue(t.size());
				}
			});

			counters.addCounter("batchTakeCount", new Instrument<Integer>() {
				@Override
				public void sample() {
					setValue(batchTakeCount);
				}
			});

			counters.addCounter("batchMergeCount", new Instrument<Integer>() {
				@Override
				public void sample() {
					setValue(batchMergeCount);
				}
			});

		}

		return counters;
		
	}

    /**
     * When invoked, the {@link StatementBuffer} will resolve terms against the
     * lexicon, but not enter new terms into the lexicon. This mode can be used
     * to efficiently resolve terms to {@link SPO}s.
     * 
     * @todo Use an {@link IBuffer} pattern can be used to make the statement
     *       buffer chunk-at-a-time. The buffer has a readOnly argument and will
     *       visit SPOs for the source statements. When readOnly, new terms will
     *       not be added to the database.
     * 
     * @todo Once we have the {@link SPO}s we can just feed them into whatever
     *       consumer we like and do bulk completion, bulk filtering, write the
     *       SPOs onto the database,etc.
     * 
     * @todo must also support the focusStore patterns, which should not be too
     *       difficult.
     */
    public void setReadOnly() {
        
        this.readOnly = true;
        
    }
    
    private boolean readOnly = false;
    
	/**
	 * Set an {@link IChangeLog} listener that will be notified about each
	 * statement actually written onto the backing store.
	 * 
	 * @param changeLog
	 *            The change log listener.
	 */
    public void setChangeLog(final IChangeLog changeLog) {
        
        this.changeLog = changeLog;
        
    }
    
    /**
	 * When non-null, this is an {@link IChangeLog} listener that will be
	 * notified about each statement actually written onto the backing store.
	 */
    private IChangeLog changeLog;

    /**
	 * Note: The use of this interface is NOT encouraged. It is used to hook the
	 * axioms in {@link com.bigdata.rdf.axioms.BaseAxioms}. Ideally this could
	 * be backed out in favor of using the {@link IChangeLog} but I was not able
	 * to make that work out very easily.
	 * 
	 * @author bryan
	 *
	 * @param <S>
	 * 
	 * @see BLZG-1552
	 */
	public interface IWrittenSPOArray {
		
	    /**
		 * A callback that is invoked with the statements actually written onto the
		 * backing store. The default implementation is a NOP.
		 * 
		 * @param stmts
		 *            An array of the statements written onto the backing store.
		 * @param numStmts
		 *            The number of entries in that array that were written.
		 */
		void didWriteSPOs(final SPO[] stmts, final int numStmts);

	}
	protected IWrittenSPOArray didWriteCallback = null;
	
    /**
     * Create a buffer that converts Sesame {@link Value} objects to {@link SPO}
     * s and writes on the <i>database</i> when it is {@link #flush()}ed. This
     * may be used to perform efficient batch write of Sesame {@link Value}s or
     * {@link Statement}s onto the <i>database</i>. If you already have
     * {@link SPO}s then use
     * {@link IRawTripleStore#addStatements(IChunkedOrderedIterator, IElementFilter)}
     * and friends.
     * 
     * @param database
     *            The database into which the termS and statements will be
     *            inserted.
     * @param capacity
     *            The #of statements that the buffer can hold.
     */
	public StatementBuffer(final AbstractTripleStore database, final int capacity) {

		this(database, capacity, 10/* defaultQueueCapacity */);

	}

	public StatementBuffer(final AbstractTripleStore database, final int capacity, final int queueCapacity) {

		this(null/* statementStore */, database, capacity, queueCapacity);

	}

    /**
	 * Create a buffer that writes on a {@link TempTripleStore} when it is
	 * {@link #flush()}ed. This variant is used during truth maintenance since
	 * the terms are written on the database lexicon but the statements are
	 * asserted against the {@link TempTripleStore}.
	 * 
	 * @param statementStore
	 *            The store into which the statements will be inserted
	 *            (optional). When <code>null</code>, both statements and terms
	 *            will be inserted into the <i>database</i>. This optional
	 *            argument provides the ability to load statements into a
	 *            temporary store while the terms are resolved against the main
	 *            database. This facility is used during incremental load+close
	 *            operations.
	 * @param database
	 *            The database. When <i>statementStore</i> is <code>null</code>,
	 *            both terms and statements will be inserted into the
	 *            <i>database</i>.
	 * @param capacity
	 *            The #of statements that the buffer can hold.
	 * @param queueCapacity
	 *            The capacity of blocking queue used by the
	 *            {@link StatementBuffer} -or- ZERO (0) to disable the blocking
	 *            queue and perform synchronous writes (default is
	 *            {@value #DEFAULT_QUEUE_CAPACITY} statements). The blocking
	 *            queue holds parsed data pending writes onto the backing store
	 *            and makes it possible for the parser to race ahead while
	 *            writer is blocked writing onto the database indices.
	 * 
	 * @see BLZG-1552 (added blocking queue)
	 */
    public StatementBuffer(final TempTripleStore statementStore,
            final AbstractTripleStore database, final int capacity, 
            final int queueCapacity
            ) {
        
        if (database == null)
            throw new IllegalArgumentException();

        if (capacity <= 0)
            throw new IllegalArgumentException();
        
        if (queueCapacity < 0)
            throw new IllegalArgumentException();
        
        this.statementStore = statementStore; // MAY be null.
        
        this.database = database;

        this.arity = database.getSPOKeyArity();
        
        this.valueFactory = database.getValueFactory();
        
        this.bufferCapacity = capacity;

        this.queueCapacity = queueCapacity;
        
        values = new BigdataValue[capacity * arity + 5];
        
        stmts = new BigdataStatement[capacity];

        /*
         * initialize capacity to N times the #of statements allowed. this
         * is the maximum #of distinct terms and would only be realized if
         * each statement used distinct values. in practice the #of distinct
         * terms will be much lower. however, also note that the map will be
         * resized at .75 of the capacity so we want to over-estimate the
         * maximum likely capacity by at least 25% to avoid re-building the
         * hash map.
         */
        
        distinctTermMap = new HashMap<Value, BigdataValue>(capacity * arity);
            
        this.statementIdentifiers = database.getStatementIdentifiers();
        
        if(log.isInfoEnabled()) {
            
            log.info("capacity=" + capacity + ", sids=" + statementIdentifiers
                    + ", statementStore=" + statementStore + ", database="
                    + database + ", arity=" + arity);
            
        }
        
        this.RDF_SUBJECT = valueFactory.asValue(RDF.SUBJECT);
        this.RDF_PREDICATE = valueFactory.asValue(RDF.PREDICATE);
        this.RDF_OBJECT = valueFactory.asValue(RDF.OBJECT);
        this.RDF_STATEMENT = valueFactory.asValue(RDF.STATEMENT);
        this.RDF_TYPE = valueFactory.asValue(RDF.TYPE);
        
    	/*
    	 * Get the reification vocabulary into the distinct term map.
    	 */
    	getDistinctTerm(RDF_SUBJECT, true);
    	getDistinctTerm(RDF_PREDICATE, true);
    	getDistinctTerm(RDF_OBJECT, true);
    	getDistinctTerm(RDF_STATEMENT, true);
    	getDistinctTerm(RDF_TYPE, true);
    
		/**
		 * TODO BLZG-1522. There is some odd interaction with SIDS that causes a
		 * thrown exception from BigdataBNodeImpl.getIV() when the queue is used
		 * with sids....
		 * 
		 * <pre>
		 * throw new UnificationException("illegal self-referential sid");
		 * </pre>
		 */
		if (true && !statementIdentifiers && queueCapacity != 0) {
			
			/*
			 * Setup a deque that will be used allow the parser to race ahead.
			 * Once the writes on the statement indices are done, the queue can
			 * be drained to a thread that will then merge the batches and batch
			 * them through to the database.
			 * 
			 * @see BLZG-641
			 * 
			 * @see BLZG-1522
			 * 
			 * @see BLZG-1813
			 */
			queue = new LinkedBlockingQueue<Batch<S>>(queueCapacity/* capacity */);

			/*
			 * Setup executor used to drain the queue, merge the batches and
			 * write on the backing store.
			 * 
			 * Note: executor is backed by the database executor service and has
			 * a maximum parallelism of one.
			 */
			executor = new LatchedExecutor(database.getExecutorService(), 1/* nparallel */);
			
		} else {
			
			/*
			 * Do not use the queue. incrementalWrite() will synchronously write
			 * onto the backing store.
			 */
			queue = null;
			executor = null;
			ft = null;
    	}
		
		
    }

	/**
	 * Added to ensure that the {@link FutureTask} is cancelled in case the
	 * caller does not shutdown the {@link StatementBuffer} normally.
	 */
    @Override
    protected void finalize() throws Throwable {
    	super.finalize();
		if (ft != null) {
			reset();
		}
	}
    
//	/**
//	 * Evict a batch (blocking put, but spins to look for an error in
//	 * {@link Future} for the thread draining the queue.
//	 * 
//	 * @param batch
//	 *            A batch (required).
//	 * 
//	 * @throws InterruptedException
//	 */
//	private void putOnQueue(final Batch<S> batch) throws InterruptedException {
//
//		Future<Void> f;
//		while ((f = ft) != null && !f.isDone()) {
//
//			if (queue.offer(batch, 100L, TimeUnit.MILLISECONDS)) {
//
//				return;
//
//			}
//
//		}
//
//		if (f == null) {
//
//			/*
//			 * The Future of the task draining the queue has been cleared (most
//			 * likely due to an error or interrupt). At this point nothing more
//			 * will be drained from the queue.
//			 */
//
//			throw new RuntimeException("Writer is done, but reader still working?");
//
//		} else if (f.isDone()) {
//
//			/*
//			 * This is most likely to indicate either an error or interrupt in
//			 * the writer. At this point nothing more will be drained from the
//			 * queue.
//			 */
//
//			throw new RuntimeException("Writer is done, but reader still working?");
//
//		}
//		
//	}

	/**
	 * Drains {@link Batch}es from the queue and writes on the database.
	 * 
	 * @author bryan
	 * 
	 * @see BLZG-1522
	 */
	private class DrainQueueCallable implements Callable<Void> {

		private boolean exhausted = false;
		
		@Override
		public Void call() throws Exception {

			while (!exhausted) {

				// Block and wait for a batch.
				final Batch<S> batch = queue.take();

				if (batch == Batch.POISON_PILL) {

					// Done.
					exhausted = true;
					
					continue;

				} else batchTakeCount++;

				if (queue.isEmpty()) {

					// Nothing else in the queue. Write out the batch immediately.
					final BatchResult batchResult = batch.writeNow();
					bnodesResolvedCount += batchResult.getNumBNodesResolved();
					batchWriteCount++;
					
					continue;

				}

				drainQueueAndMergeBatches(batch);
				
			} // block and wait for the next batch.
			
			// done.
			return null;

		} // call()

		/**
		 * There is more in the queue. Drain it. Watch out for that poison pill!
		 * 
		 * Note: Maximum from drainTo() is queueCapacity. Plus 1 since we
		 * already have one batch on hand.
		 */
		private void drainQueueAndMergeBatches(final Batch<S> batch) {

			if (batch == null)
				throw new IllegalArgumentException();
			
			if (batch == Batch.POISON_PILL) // DO NOT pass the poisen pill!
				throw new IllegalArgumentException();
			
			final List<Batch<S>> avail = new LinkedList<Batch<S>>();

			// add the batch already on hand (from caller)
			avail.add(batch);

			// drain the queue while queue is *known* to contain something.
			while (!exhausted && !queue.isEmpty()) {

				// non-blocking take. should be available immediately. but
				// there *might* have been a clear() call.
				final Batch<S> anotherBatch = queue.poll();

				if (anotherBatch == null) {

					// Note: This could arise through a concurrent clear of
					// the queue.
					exhausted = true;

				} else if (anotherBatch == Batch.POISON_PILL) {

					// Done.
					exhausted = true;

				} else {

					// Add to the set that we will merge together.
					avail.add(anotherBatch);
					batchTakeCount++;
					
				}

			}

			if (avail.size() == 1) {

				/*
				 * Safety check. Do not merge a single batch.
				 */
				final BatchResult batchResult = avail.get(0).writeNow();
				bnodesResolvedCount += batchResult.getNumBNodesResolved();
				batchWriteCount++;

			} else {

				// Merge the batches together and then write them out.
			    final BatchResult batchResult = new MergeUtility<S>().merge(avail).writeNow();
				bnodesResolvedCount += batchResult.getNumBNodesResolved();
				batchMergeCount += avail.size();
				batchWriteCount++;

			}
			
		}
		
	} // DrainQueueCallable

    /**
     * Signals the end of a source and causes all buffered statements to be
     * written.
     * <p>
     * Note: The source limits the scope within which blank nodes are
     * co-referenced by their IDs. Calling this method will flush the buffer,
     * cause any deferred statements to be written, and cause the canonicalizing
     * mapping for blank nodes to be discarded.
     * 
     * @todo this implementation always returns ZERO (0).
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    public long flush() {
       
//        log.warn("");

        /*
         * Process deferred statements (NOP unless using statement identifiers).
         */
//        processDeferredStatements();

        // flush anything left in the buffer.
        incrementalWrite();
        
		if (queue != null) {
			
			// Drop a poison pill on the queue.
			try {
				
				queue.put((Batch) Batch.POISON_PILL);

				// block and wait until the flush is done.
				final Future<Void> ft = this.ft;
				if (ft != null) {
					ft.get();
				}
				
			} catch (InterruptedException e) {
				
				// Cancel task and propagate interrupt.
				
				ft.cancel(true/* mayInterruptIfRunning */);
				
				Thread.currentThread().interrupt();
				
			} catch (ExecutionException e) {
				
				// Wrap and throw.
				throw new RuntimeException(e);
				
			}
			
        }
        
        // discard all buffer state (including bnodes and deferred statements).
        reset();
        
        return 0L;

    }
    
//    /**
//     * Processes the {@link #deferredStmts deferred statements}.
//     * <p>
//     * When statement identifiers are enabled the processing of statements using
//     * blank nodes in their subject or object position must be deferred until we
//     * know whether or not the blank node is being used as a statement
//     * identifier (blank nodes are not allowed in the predicate position by the
//     * RDF data model). If the blank node is being used as a statement
//     * identifier then its {@link IV}  will be assigned based on
//     * the {s,p,o} triple. If it is being used as a blank node, then the
//     * {@link IV} is assigned using the blank node ID.
//     * <p>
//     * Deferred statements are processed as follows:
//     * <ol>
//     * 
//     * <li>Collect all deferred statements whose blank node bindings never show
//     * up in the context position of a statement (
//     * {@link BigdataBNode#getStatementIdentifier()} is <code>false</code>).
//     * Those blank nodes are NOT statement identifiers so we insert them into
//     * the lexicon and the insert the collected statements as well.</li>
//     * 
//     * <li>The remaining deferred statements are processed in "cliques". Each
//     * clique consists of all remaining deferred statements whose {s,p,o} have
//     * become fully defined by virtue of a blank node becoming bound as a
//     * statement identifier. A clique is collected by a full pass over the
//     * remaining deferred statements. This process repeats until no statements
//     * are identified (an empty clique or fixed point).</li>
//     * 
//     * </ol>
//     * If there are remaining deferred statements then they contain cycles. This
//     * is an error and an exception is thrown.
//     * 
//     * @todo on each {@link #flush()}, scan the deferred statements for those
//     *       which are fully determined (bnodes are flagged as statement
//     *       identifiers) to minimize the build up for long documents?
//     */
//    protected void processDeferredStatements() {
//
//        if (!statementIdentifiers || deferredStmts == null
//                || deferredStmts.isEmpty()) {
//
//            // NOP.
//            
//            return;
//            
//        }
//
//        if (log.isInfoEnabled())
//            log.info("processing " + deferredStmts.size()
//                    + " deferred statements");
//
//        /*
//         * Need to flush the terms out to the dictionary or the reification 
//         * process will not work correctly.
//         */
//        incrementalWrite();
//        
//        try {
//            
//            // Note: temporary override - clear by finally{}.
//            statementIdentifiers = false;
//            
//            // stage 0
//            if (reifiedStmts != null) {
//            	
//            	for (Map.Entry<BigdataBNodeImpl, ReifiedStmt> e : reifiedStmts.entrySet()) {
//            	
//            		final BigdataBNodeImpl sid = e.getKey();
//            		
//            		final ReifiedStmt reifiedStmt = e.getValue();
//            		
//            		if (!reifiedStmt.isFullyBound(arity)) {
//            			
//            			log.warn("unfinished reified stmt: " + reifiedStmt);
//            			
//            			continue;
//            			
//            		}
//
//            		final BigdataStatement stmt = valueFactory.createStatement(
//            				reifiedStmt.getSubject(), 
//            				reifiedStmt.getPredicate(), 
//            				reifiedStmt.getObject(), 
//            				reifiedStmt.getContext(), 
//							StatementEnum.Explicit);
//            		
//            		sid.setStatement(stmt);
//            		
//            		sid.setIV(new SidIV(new SPO(stmt)));
//            		
//            		if (log.isInfoEnabled()) {
//            			log.info("reified sid conversion: sid=" + sid + ", stmt=" + stmt);
//            		}
//            		
//            	}
//            	
//            	if (log.isInfoEnabled()) {
//            	
//            		for (BigdataBNodeImpl sid : reifiedStmts.keySet()) {
//            	
//            			log.info("sid: " + sid + ", iv=" + sid.getIV());
//            			
//            		}
//            		
//            	}
//            	
//            }
//            
//            // stage 1.
//            {
//                
//                final int nbefore = deferredStmts.size();
//                
//                int n = 0;
//                
//                final Iterator<BigdataStatement> itr = deferredStmts.iterator();
//                
//                while(itr.hasNext()) {
//                    
//                    final BigdataStatement stmt = itr.next();
//
//                    if (stmt.getSubject() instanceof BNode
//                            && ((BigdataBNode) stmt.getSubject()).isStatementIdentifier())
//                        continue;
//
//                    if (stmt.getObject() instanceof BNode
//                            && ((BigdataBNode) stmt.getObject()).isStatementIdentifier())
//                        continue;
//
//                    if(log.isDebugEnabled()) {
//                        log.debug("grounded: "+stmt);
//                    }
//
//                    if (stmt.getSubject() instanceof BNode)
//                    	addTerm(stmt.getSubject());
//                    
//                    if (stmt.getObject() instanceof BNode)
//                    	addTerm(stmt.getObject());
//                    
//                    // fully grounded so add to the buffer.
//                    add(stmt);
//                    
//                    // the statement has been handled.
//                    itr.remove();
//                    
//                    n++;
//                    
//                }
//                
//                if (log.isInfoEnabled())
//                    log.info(""+ n
//                                + " out of "
//                                + nbefore
//                                + " deferred statements used only blank nodes (vs statement identifiers).");
//                
//                /*
//                 * Flush everything in the buffer so that the blank nodes that
//                 * are really blank nodes will have their term identifiers
//                 * assigned.
//                 */
//                
//                incrementalWrite();
//                
//            }
//            
//            // stage 2.
//            if(!deferredStmts.isEmpty()) {
//                
//                int nrounds = 0;
//                
//                while(true) {
//
//                    nrounds++;
//                    
//                    final int nbefore = deferredStmts.size();
//                    
//                    final Iterator<BigdataStatement> itr = deferredStmts.iterator();
//                    
//                    while(itr.hasNext()) {
//                        
//                        final BigdataStatement stmt = itr.next();
//
//                        if (log.isDebugEnabled()) {
//                        	log.debug(stmt.getSubject() + ", iv=" + stmt.s());
//                        }
//                        
//                        if (stmt.getSubject() instanceof BNode
//                                && ((BigdataBNode) stmt.getSubject()).isStatementIdentifier()
//                                && stmt.s() == null)
//                            continue;
//
//                        if (stmt.getObject() instanceof BNode
//                                && ((BigdataBNode) stmt.getObject()).isStatementIdentifier()
//                                && stmt.o() == null)
//                            continue;
//
//                        if (log.isDebugEnabled()) {
//                            log.debug("round="+nrounds+", grounded: "+stmt);
//                        }
//                        
//                        // fully grounded so add to the buffer.
//                        add(stmt);
//                        
//                        // deferred statement has been handled.
//                        itr.remove();
//                        
//                    }
//                    
//                    final int nafter = deferredStmts.size();
//
//                    if (log.isInfoEnabled())
//                        log.info("round=" + nrounds+" : #before="+nbefore+", #after="+nafter);
//                    
//                    if(nafter == nbefore) {
//                    
//                        if (log.isInfoEnabled())
//                            log.info("fixed point after " + nrounds
//                                    + " rounds with " + nafter
//                                    + " ungrounded statements");
//                        
//                        break;
//                        
//                    }
//                    
//                    /*
//                     * Flush the buffer so that we can obtain the statement
//                     * identifiers for all statements in this clique.
//                     */
//                    
//                    incrementalWrite();
//                    
//                } // next clique.
//                
//                final int nremaining = deferredStmts.size();
//
//                if (nremaining > 0) {
//
//                	if (log.isDebugEnabled()) {
//                		
//                		for (BigdataStatement s : deferredStmts) {
//                			log.debug("could not ground: " + s);
//                		}
//                		
//                	}
//                	
//                    throw new StatementCyclesException(
//                            "" + nremaining
//                            + " statements can not be grounded");
//                    
//                }
//                
//                
//            } // stage 2.
//
//        } finally {
//
//            // Note: restore flag!
//            statementIdentifiers = true;
//
//            deferredStmts = null;
//            
//            reifiedStmts = null;
//            
//        }
//        
//    }
    
    /**
     * Clears all buffered data, including the canonicalizing mapping for blank
     * nodes and deferred provenance statements.
     */
    @Override
    public void reset() {
        
        _clear();
        
        /*
         * Note: clear the reference NOT the contents of the map! This makes it
         * possible for the caller to reuse the same map across multiple
         * StatementBuffer instances.
         */

        bnodes = null;
        
        bnodesTotalCount = 0;
        
        bnodesResolvedCount = 0;
        
        deferredStmts = null;
        
        reifiedStmts = null;
        
		if (queue != null) {

			final Future<Void> ft = this.ft;

			if (ft != null) {

				// Cancel any running task.
				ft.cancel(true/* mayInterruptIfRunning */);

				this.ft = null;
				
			}
			
			// Clear the queue.
			queue.clear();
			
		}
        
    }
    
    /**
     * @todo could be replaced with {@link BigdataValueFactory
     */
    @Override
    public void setBNodeMap(final Map<String, BigdataBNode> bnodes) {
    
        if (bnodes == null)
            throw new IllegalArgumentException();
        
        if (this.bnodes != null)
            throw new IllegalStateException();
        
        this.bnodes = bnodes;
        
        bnodesTotalCount = 0;
        
        bnodesResolvedCount = 0;
        
        for (BigdataBNode bnode: bnodes.values()) {
        	if (bnode.getIV() == null) {
        		bnodesTotalCount++;
        	}
        }
        
    }
    
    /**
     * Invoked by {@link #incrementalWrite()} to clear terms and statements
     * which have been written in preparation for buffering more writes. This
     * does NOT discard either the canonicalizing mapping for blank nodes NOR
     * any deferred statements.
     */
    protected void _clear() {
        
        // Avoid potential IndexOutOfBoundsException in _clear().
        // @see https://jira.blazegraph.com/browse/BLZG-1708 (DataLoader fails with ArrayIndexOutOfBoundsException)
        final int nvalues = Math.min(values.length, numValues);
        final int nstmts = Math.min(stmts.length, numStmts);
        
        for (int i = 0; i < nvalues; i++) {

            values[i] = null;

        }

        for (int i = 0; i < nstmts; i++) {

            stmts[i] = null;

        }

        numURIs = numLiterals = numBNodes = numStmts = numValues = 0;

        numSIDs = 0;
        
        if (distinctTermMap != null) {
            
            distinctTermMap.clear();
         
        	/*
        	 * Get the reification vocabulary into the distinct term map.
        	 */
        	getDistinctTerm(RDF_SUBJECT, true);
        	getDistinctTerm(RDF_PREDICATE, true);
        	getDistinctTerm(RDF_OBJECT, true);
        	getDistinctTerm(RDF_STATEMENT, true);
        	getDistinctTerm(RDF_TYPE, true);
            
        }

//        clearBNodeMap();
        
    }
    
    /**
     * Batch insert buffered data (terms and statements) into the store.
     */
    protected void incrementalWrite() {

    	// Buffer a batch and then incrementally flush.
		if (queue == null) {

			final BatchResult batchResult = new Batch<S>(this, true/* avoidCloningIfPossible */).writeNow();
			bnodesResolvedCount += batchResult.getNumBNodesResolved();
			batchWriteCount++;

	        // Reset the state of the buffer (but not the bnodes nor deferred stmts).
	        _clear();

		} else {
			
			
			if (ft == null || ft.isDone() /* BLZG-1813 */) {
			
				/*
				 * If the future is done, get the future, and
				 * propogate any exceptions. 
				 * 
				 * @see BLZG-1813
				 */
				if (ft != null && ft.isDone()) {

					try {

						ft.get(); // get the future.

						/*
						 * Fall through. New Future will be created below.
						 */

					} catch (InterruptedException e) {

						// Propagate interrupt
                        throw new RuntimeException(e);

					} catch (ExecutionException ex) {

						throw new RuntimeException(ex);

					}
				}

				/*
				 * Note: Lazily initialized since reset() does not make the
				 * StatementBuffer object invalid for further use.
				 */

				ft = new FutureTask<Void>(new DrainQueueCallable());

				executor.execute(ft);

			}

			try {

				// Blocking put.
				queue.put(new Batch<S>(this, false/* avoidCloningIfPossible */));
				batchAddCount++;
				
			} catch (InterruptedException e) {
				
				throw new RuntimeException(e);
				
			}

		}
    	
    }
    
    /**
	 * A utility class to merge {@link Batch}es together while maintaining their
	 * distinct {@link Value}[]s.
	 * 
	 * @author bryan
	 * @see BLZG-1522
	 * @param <S>
	 */
    private static class MergeUtility<S extends Statement> {

		/*
		 * used by merge(). single threaded access.
		 */
		private int numValues;
		private BigdataValue[] values;
    	private Map<BigdataValue, BigdataValue> distinctTermMap;
    	
    	MergeUtility() {
    		
    	}
    	
    	/**
		 * Merge a set of batches together.
		 * 
		 * @param avail
		 *            The available batches.
		 *            
		 * @throws IllegalArgumentException
		 *             if the argument is null.
		 * @throws IllegalArgumentException
		 *             if the argument is an empty list.
		 * @throws IllegalArgumentException
		 *             if the argument does not contain at least two batches.
		 */
		public Batch<S> merge(final List<Batch<S>> avail) {
			
			if (avail == null)
				throw new IllegalArgumentException();
			if (avail.isEmpty())
				throw new IllegalArgumentException();
			if (avail.size() < 2)
				throw new IllegalArgumentException();

			if (distinctTermMap != null) {
				// An attempt to reuse a MergeUtility object.
				throw new IllegalStateException();				
			}
			
			/*
			 * We need to create a new combined Statement[] containing only the
			 * distinct Values and a new Value[] in which those distinct values
			 * are entered. This removes duplicates from the Value[] which is
			 * quite important for throughput. It is not as critical to de-dup
			 * the Statement[] as duplicate statements are uncommon and do not
			 * incur much overhead since we will sort the statements before
			 * writing on the indices.
			 * 
			 * TODO We could potentially run into problems with a very large
			 * capacity since the combined size of the values[] (without
			 * duplicate removal) could exceed an int32 value. But this is not
			 * likely.
			 */

			// find maximum size for arrays.
			int maxValues = 0;
			int maxStmts = 0;
			{
				for (Batch<S> sb : avail) {
					maxValues += sb.numValues;
					maxStmts += sb.numStmts;
				}
				
				if(DEBUG) {
					log.debug("maxValues=" + maxValues);
				}

				// we will de-dup the values below.
				values = new BigdataValue[maxValues];

				// set map to find the distinct Values.
				distinctTermMap = new HashMap<BigdataValue, BigdataValue>(maxValues);

			}

			// copy statements, finding distinct Values.
			final int numStmts;
			final BigdataStatement[] stmts;
			{
				// we will not de-dup the statements.
				stmts = new BigdataStatement[maxStmts];

				int n = 0;
				for (Batch<S> sb : avail) {
					for (int i = 0; i < sb.numStmts; i++, n++) {
						// Create new statement using distinct values.
						final BigdataStatement stmt = (BigdataStatement) sb.stmts[i];
						final BigdataResource s = (BigdataResource) getDistinctTerm(stmt.getSubject());
						final BigdataURI p = (BigdataURI) getDistinctTerm(stmt.getPredicate());
						final BigdataValue o = getDistinctTerm(stmt.getObject());
						final BigdataResource c = stmt.getContext() == null ? null
								: (BigdataResource) getDistinctTerm(stmt.getContext());
						stmts[n] = s.getValueFactory().createStatement(s, p, o, c, stmt.getStatementType());
					}
				}
				numStmts = n;
			}

			final Batch<S> sb = avail.get(0);
			return new Batch<S>(sb.database, // copy by reference
					sb.statementStore, // copy by reference
					sb.readOnly, // copy by reference
					sb.changeLog, // copy by reference
					sb.didWriteCallback, // copy by reference
					numValues, // copied the data.
					values, // copied the data.
					numStmts, // copied the data.
					stmts // copied the data.
			);

		} // merge()

	    /**
		 * Canonicalizing mapping for a term when merging {@link Batch}es
		 * together. This is simpler than the general case since we have already
		 * handled blank nodes, SIDs, etc. in the outer context.
		 * 
		 * @param term
		 *            A term.
		 * 
		 * @return Either the term or the pre-existing term in the {@link Batch}
		 *         with the same data.
		 * 
		 * @throws IllegalArgumentException
		 *             if the argument is null (so do not pass a null context in
		 *             here!)
		 */
		private BigdataValue getDistinctTerm(final BigdataValue term) {

			if (term == null)
				throw new IllegalArgumentException();
			
			if (term instanceof BigdataBNode && term.getIV() != null) {
				// Skip already resolved blank nodes
				// @see https://jira.blazegraph.com/browse/BLZG-1889 (ArrayIndexOutOfBound Exception)
				return term;
			}

			// TODO BLZG-1532 (JAVA8) replace with putIfAbsent()
			final BigdataValue existingTerm = distinctTermMap.get(term);

			if (existingTerm != null) {

				/*
				 * Term already exists, do not add.
				 */
				return existingTerm;

			}

			// put the new term in the map.
			if (distinctTermMap.put(term, term) != null) {

				throw new AssertionError();

			}
			
			if(log.isDebugEnabled()) {
				log.debug("numValues="+numValues + ": " + term);
			}

			values[numValues++] = term;

			// return the new term.
			return term;

	    }
	    

    }

    /**
     * Result of the {@link Batch} execution, consists of #of statements written
     * to the database and #of bnodes, which do have their IVs assigned after
     * incremental write
     * 
     * @author kim
     *
     * @see BLZG-1708
     */
	private static class BatchResult {

		private final long nwritten;
		private final long nBnodesResolved;

		public BatchResult(final long nwritten, final long nBnodesResolved) {
			this.nwritten = nwritten;
			this.nBnodesResolved = nBnodesResolved;
		}
		
		public long getNumWritten() {
			return nwritten;
		}
		
		public long getNumBNodesResolved() {
			return nBnodesResolved;
		}
	}

	/**
	 * A batch of statements together with their distinct values to be written
	 * onto the database.
	 * 
	 * @author bryan
	 * 
	 * @see BLZG-1522
	 */
    private static class Batch<S extends Statement> {

    	/**
		 * Singleton instance used to indicate that no more elements will be
		 * added to the queue.
		 */
    	@SuppressWarnings("rawtypes")
		private static final Batch<?> POISON_PILL = new Batch();

		/*
		 * All of these are fields from the outer class. They have either been
		 * copied by reference or cloned depending on the constructor call.
		 * 
		 * Note: I have explicitly replicated them here to provide a boundary
		 * around the state that can be dropped into the queue and the state
		 * that is being used to absorb statements from the parser. This is why
		 * Batch is a *static* class.
		 * 
		 * Note: One benefit of this boundary is that we known that the
		 * StatementBuffer bnodes map is NOT used from within this class so we
		 * do not need to worry about concurrency control for that collection.
		 */
    	
    	private final AbstractTripleStore database;
    	
    	private final AbstractTripleStore statementStore;
    	
    	private final boolean readOnly;
    	
    	private final IChangeLog changeLog;
    	
    	private final IWrittenSPOArray didWriteCallback;

    	private final int numValues;
    	
    	private final BigdataValue[] values;
    	
    	private final int numStmts;
    	
    	private final BigdataStatement[] stmts;

    	/**
    	 * Singleton instance constructor.
    	 */
    	private Batch() {
			database = null;
			statementStore = null;
			readOnly = true;
			changeLog = null;
			didWriteCallback = null;
			numValues = 0;
			values = null;
			numStmts = 0;
			stmts = null;
    	}

    	/**
    	 * Constructor used when merging multiple batches.
    	 */
    	private Batch(  final AbstractTripleStore database, //
				final AbstractTripleStore statementStore, //
				final boolean readOnly, //
				final IChangeLog changeLog, //
				final IWrittenSPOArray didWriteCallback, //
				final int numValues, //
				final BigdataValue[] values, //
				final int numStmts, //
				final BigdataStatement[] stmts//
		) {
			this.database = database;
			this.statementStore = statementStore;
			this.readOnly = readOnly;
			this.changeLog = changeLog;
			this.didWriteCallback = didWriteCallback;
			this.numValues = numValues;
			this.values = values;
			this.numStmts = numStmts;
			this.stmts = stmts;
    	}

    	/**
		 * 
		 * @param sb
		 * @param avoidCloningIfPossible
		 *            When false, the backing arrays are cloned in order to allow
		 *            them to be cleared by the caller. When true, values backing array
		 *            will be used as-is if it is possible (there is enough capacity
		 *            for the blank nodes, which has to be written to DB);
		 *            in this case the caller MUST invoke {@link #writeNow()}
		 *            synchronously. (This is used to make it easier to compare
		 *            the two approaches without introducing any new overhead).
		 *            @see https://jira.blazegraph.com/browse/BLZG-1889 (ArrayIndexOutOfBound Exception)
		 */
    	Batch(final StatementBuffer<S> sb, final boolean avoidCloningIfPossible) {

			if (sb == null)
				throw new IllegalArgumentException();

			/*
			 * All of these fields can be copied by reference. They are not
			 * carrying any interesting state information from the parser.
			 */
			this.database = sb.database;
			this.statementStore = sb.statementStore;
			this.readOnly = sb.readOnly;
			this.changeLog = sb.changeLog;
			this.didWriteCallback = sb.didWriteCallback;

	    	/*
	    	 * Look for non-sid bnodes and add them to the values to be written
	    	 * to the database (if they haven't already been written).
	    	 */
    		List<BigdataValue> bnodes = new ArrayList<>();
    		
	    	if (sb.bnodes != null) {

		    	for (BigdataBNode bnode : sb.bnodes.values()) {
		    		
		    		// sid, skip
		    		if (bnode.isStatementIdentifier())
		    			continue;
		    		
		    		// already written, skip
		    		if (bnode.getIV() != null)
		    			continue;
		    		
		    		bnodes.add(bnode);
		    		
		    	}
		    	
	    	}
	    	
	    	boolean cloned = false;
	    	    
			if (avoidCloningIfPossible && bnodes.isEmpty()) {
				
				// Direct use of StatementBuffer values array is possible, as there are no blank nodes to be written
				
				this.values = sb.values;
				this.numValues = sb.numValues;
				
			} else {
				
				if (avoidCloningIfPossible && (sb.numValues + bnodes.size() <= sb.values.length)) {

					// Could use StatementBuffer values array, as its capacity is sufficient for blank nodes
					
					this.values = sb.values;

			    } else {
				
					// Need to recreate values array to ensure capacity for blank nodes
					this.values = new BigdataValue[sb.numValues + bnodes.size()];
					
					cloned = true;
					
				}
				
				System.arraycopy(sb.values/* src */, 0/* srcPos */, this.values/* dest */, 0/* destPos */,
						sb.numValues/* length */);
	
				// append bnodes
				for (int i = 0; i < bnodes.size(); i++) {
					
					this.values[sb.numValues + i] = bnodes.get(i);
					
				}
				
				this.numValues = sb.numValues + bnodes.size();
			}

			if (avoidCloningIfPossible) {

				// Copy array references.
				
				this.numStmts = sb.numStmts;
				this.stmts = sb.stmts;

			} else {

				// Clone array data.
				
				this.numStmts = sb.numStmts;
				this.stmts = new BigdataStatement[sb.numStmts];
				System.arraycopy(sb.stmts/* src */, 0/* srcPos */, this.stmts/* dest */, 0/* destPos */,
						sb.numStmts);
				
			}
			
			if (cloned) {
				/*
				 * The data was cloned, so reset the statement of the buffer in
				 * the outer context (but not the bnodes nor deferred stmts).
				 */
				sb._clear();

			}
			
    	}

	    /**
         * Flush the batch.
         * 
         * @return A summary of the #of statements actually written and blank
         *         nodes actually resolved.
         */
		private BatchResult writeNow() {

            final long begin = System.currentTimeMillis();

            long nBnodesResolved = 0;

            if (log.isInfoEnabled())
				log.info("numValues=" + numValues + ", numStmts=" + numStmts);

            // Insert terms (batch operation).
            if (numValues > 0) {
                if (DEBUG) {
                    for (int i = 0; i < numValues; i++) {
                        log
                                .debug("adding term: "
                                        + values[i]
                                        + " (iv="
                                        + values[i].getIV()
                                        + ")"
                                        + ((values[i] instanceof BNode) ? "sid="
                                                + ((BigdataBNode) values[i]).isStatementIdentifier()
                                                : ""));
                    }
                }
                // Calculate #of bnodes, which were unresolved
                for (BigdataValue v: values) {
                	if (v instanceof BigdataBNode && v.getIV() == null) {
                		nBnodesResolved++;
                	}
                }
                addTerms(database, values, numValues, readOnly);
                // Substract #of bnodes, which remain unresolved
                // as a result we have number of bnodes, which were resolved
                for (BigdataValue v: values) {
                	if (v instanceof BigdataBNode && v.getIV() == null) {
                		nBnodesResolved--;
                	}
                }
                if (DEBUG) {
                    for (int i = 0; i < numValues; i++) {
                        log
                                .debug(" added term: "
                                        + values[i]
                                        + " (iv="
                                        + values[i].getIV()
                                        + ")"
                                        + ((values[i] instanceof BNode) ? "sid="
                                                + ((BigdataBNode) values[i]).isStatementIdentifier()
                                                : ""));
                    }
                }
            }

            // Insert statements (batch operation).
            final long nwritten;
			if (numStmts > 0) {
				if (DEBUG) {
					for (int i = 0; i < numStmts; i++) {
						log.debug("adding stmt: " + stmts[i]);
					}
				}
				nwritten = addStatements(database, statementStore, stmts, numStmts, changeLog, didWriteCallback);
				if (DEBUG) {
					for (int i = 0; i < numStmts; i++) {
						log.debug(" added stmt: " + stmts[i]);
					}
				}
            } else nwritten = 0;
            
            if (log.isInfoEnabled()) {
                
                final long elapsed = System.currentTimeMillis() - begin;
                
                log.info("numValues=" + numValues + ", numStmts=" + numStmts
                        + ", elapsed=" + elapsed + "ms");
                
            }
            
            return new BatchResult(nwritten, nBnodesResolved);
            
    	}
    	
    	static private void addTerms(
    			final AbstractTripleStore database, //
    			final BigdataValue[] terms, //
    			final int numTerms,//
    			final boolean readOnly//
    			) {

    	    if(log.isInfoEnabled())
    	        log.info("writing " + numTerms);
            
            if (DEBUG) {

                for (int i = 0; i < numTerms; i++) {
                	log.debug("term: " + terms[i] + ", iv: " + terms[i].getIV());
                }

            }
            
            final long l =
                    database.getLexiconRelation().addTerms(terms, numTerms, readOnly);
            
            if (log.isInfoEnabled()) {
                log.info("# reported from addTerms: " + l);
            }
            
        }
        
        /**
         * Adds the statements to each index (batch api, NO truth maintenance).
         * <p>
         * Pre-conditions: The {s,p,o} term identifiers for each
         * {@link BigdataStatement} are defined.
         * <p>
         * Note: If statement identifiers are enabled and the context position is
         * non-<code>null</code> then it will be unified with the statement
         * identifier assigned to that statement. It is an error if the context
         * position is a URI (since it can not be unified with the assigned
         * statement identifier). It is an error if the context position is a blank
         * node which is already bound to a term identifier whose value is different
         * from the statement identifier assigned/reported by the {@link #database}.
         * 
    	 * @param database
    	 *            The database that will be used to resolve terms. When
    	 *            <i>statementStore</i> is <code>null</code>, statements will be
    	 *            written into this store as well.
    	 * @param statementStore
    	 *            When non-<code>null</code> the statements will be written on
    	 *            this store. When <code>null</code> the statements are written
    	 *            onto the <i>database</i>. (This is used to support incremental
    	 *            truth maintenance.)
         * @param stmts
         *            An array of statements in any order.
         *            @param numStmts The number of statements in that array.
         *            @param changeLog
         *            
         * 
         * @return The #of statements written on the database.
         */
    	final private static <S> long addStatements(final AbstractTripleStore database,
    			final AbstractTripleStore statementStore, final BigdataStatement[] stmts, final int numStmts,
    			final IChangeLog changeLog,
    			final IWrittenSPOArray didWriteCallback) {

            final SPO[] tmp = new SPO[numStmts];

            for (int i = 0; i < tmp.length; i++) {

                final BigdataStatement stmt = stmts[i];
                
                final SPO spo = new SPO(stmt);

                if (DEBUG) 
                    log.debug("adding: " + stmt.toString() + " (" + spo + ")");
                
                if(!spo.isFullyBound()) {
                    
                    throw new AssertionError("Not fully bound? : " + spo);
                    
                }
                
                tmp[i] = spo;

            }
            
            /*
             * Note: When handling statement identifiers, we clone tmp[] to avoid a
             * side-effect on its order so that we can unify the assigned statement
             * identifiers below.
             * 
             * Note: In order to report back the [ISPO#isModified()] flag, we also
             * need to clone tmp[] to avoid a side effect on its order. Therefore we
             * now always clone tmp[].
             */
//            final long nwritten = writeSPOs(sids ? tmp.clone() : tmp, numStmts);
            final long nwritten = writeSPOs(database, statementStore, tmp.clone(), numStmts, didWriteCallback);

//            if (sids) {
    //
//                /*
//                 * Unify each assigned statement identifier with the context
//                 * position on the corresponding statement.
//                 */
    //
//                for (int i = 0; i < numStmts; i++) {
//                    
//                    final SPO spo = tmp[i];
//                    
//                    final BigdataStatement stmt = stmts[i];
    //
//                    // verify that the BigdataStatement and SPO are the same triple.
//                    assert stmt.s() == spo.s;
//                    assert stmt.p() == spo.p;
//                    assert stmt.o() == spo.o;
//                    
//                    final BigdataResource c = stmt.getContext();
//                    
//                    if (c == null)
//                        continue;
    //
////                    if (c instanceof URI) {
    ////
////                        throw new UnificationException(
////                                "URI not permitted in context position when statement identifiers are enabled: "
////                                        + stmt);
////                        
////                    }
//                    
//                    if( c instanceof BNode) {
    //
//                        final IV sid = spo.getStatementIdentifier();
//                        
//                        if(c.getIV() != null) {
//                            
//                            if (!sid.equals(c.getIV())) {
    //
//                                throw new UnificationException(
//                                        "Can not unify blankNode "
//                                                + c
//                                                + "("
//                                                + c.getIV()
//                                                + ")"
//                                                + " in context position with statement identifier="
//                                                + sid + ": " + stmt + " (" + spo
//                                                + ")");
//                                
//                            }
//                            
//                        } else {
//                            
//                            // assign the statement identifier.
//                            c.setIV(sid);
//                            
//                            if (log.isDebugEnabled()) {
//                                
//                                log.debug("Assigned statement identifier: " + c
//                                        + "=" + sid);
//                                
//                            }
    //
//                        }
//                        
//                    }
//                    
//                }
//                    
//            }

            // Copy the state of the isModified() flag
            for (int i = 0; i < numStmts; i++) {

                if (tmp[i].isModified()) {

                    stmts[i].setModified(tmp[i].getModified());
                    
                    if (changeLog != null) {
                        
                        switch(stmts[i].getModified()) {
                        case INSERTED:
                            changeLog.changeEvent(new ChangeRecord(stmts[i], ChangeAction.INSERTED));
                            break;
                        case UPDATED:
                            changeLog.changeEvent(new ChangeRecord(stmts[i], ChangeAction.UPDATED));
                            break;
                        case REMOVED:
                            throw new AssertionError();
                        default:
                            break;
                        }
                        
                    }

                }
                
            }
            
            return nwritten;
            
        }

        /**
    	 * Adds the statements to each index (batch api, NO truth maintenance).
    	 * 
    	 * @param database
    	 *            The database that will be used to resolve terms. When
    	 *            <i>statementStore</i> is <code>null</code>, statements will be
    	 *            written into this store as well.
    	 * @param statementStore
    	 *            When non-<code>null</code> the statements will be written on
    	 *            this store. When <code>null</code> the statements are written
    	 *            onto the <i>database</i>. (This is used to support incremental
    	 *            truth maintenance.)
    	 * @param stmts
    	 *            An array of the statements to be written onto the backing
    	 *            store.
    	 * @param numStmts
    	 *            The number of entries in that array to be written.
    	 * 
    	 * @return The #of statements written on the database.
    	 * 
    	 * @see AbstractTripleStore#addStatements(AbstractTripleStore, boolean,
    	 *      IChunkedOrderedIterator, IElementFilter)
    	 */
		static private <S> long writeSPOs(final AbstractTripleStore database, final AbstractTripleStore statementStore,
				final SPO[] stmts, final int numStmts, final IWrittenSPOArray callback) {

            final IChunkedOrderedIterator<ISPO> itr = new ChunkedArrayIterator<ISPO>(
                    numStmts, stmts, null/* keyOrder */);

            final AbstractTripleStore sink = statementStore != null ? statementStore
                    : database;

            if (log.isInfoEnabled()) {

                log.info("writing " + numStmts + " on "
                        + (statementStore != null ? "statementStore" : "database"));
                
                if(DEBUG) {
                    for (int i = 0; i < numStmts; i++) {
                    	log.debug("spo: " + stmts[i]);
                    }
                }

            }

            // synchronous write on the target.
            final long nwritten = database
                    .addStatements(sink, false/* copyOnly */, itr, null /* filter */);

			if (callback != null) {

				callback.didWriteSPOs(stmts, numStmts);
				
            }
            
            return nwritten;
            
        }

    } // class Batch

    /**
     * Add an "explicit" statement to the buffer (flushes on overflow, no
     * context).
     * 
     * @param s
     * @param p
     * @param o
     */
    @Override
    public void add(final Resource s, final URI p, final Value o) {
        
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
    @Override
    public void add(final Resource s, final URI p, final Value o, final Resource c) {
        
        add(s, p, o, c, StatementEnum.Explicit);
        
    }
    
    /**
     * Add a statement to the buffer (core impl, flushes on overflow).
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     */
    @Override
    public void add(final Resource s, final URI p, final Value o,
            final Resource c, final StatementEnum type) {
        
        if (nearCapacity()) {

            // bulk insert the buffered data into the store.
            if (true) {
                // THIS IS THE CORRECT ACTION!
                incrementalWrite();
            } else {
                /*
                 * This will flush all blank nodes. It may be necessary on very
                 * large files. It also resets the blank node and deferred
                 * statement maps afterwards (since they are set to null by
                 * reset()).
                 */
                flush();
                bnodes = new HashMap<String, BigdataBNode>(bufferCapacity);
                deferredStmts = new HashSet<BigdataStatement>(stmts.length);
            }
        }
        
        // add to the buffer.
        handleStatement(s, p, o, c, type);

    }
    
    @Override
    public void add(final Statement e) {

        add(e.getSubject(), e.getPredicate(), e.getObject(), e.getContext(),
                (e instanceof BigdataStatement ? ((BigdataStatement) e)
                        .getStatementType() : null));

    }

    /**
     * Returns true if the bufferQueue has less than three slots remaining for
     * any of the value arrays (URIs, Literals, or BNodes) or if there are no
     * slots remaining in the statements array. Under those conditions adding
     * another statement to the bufferQueue could cause an overflow.
     * 
     * @return True if the bufferQueue might overflow if another statement were
     *         added.
     */
    public boolean nearCapacity() {

        if (numStmts + 1 > bufferCapacity)
            return true;

        // This check takes into account dynamically calculated #of unresolved bnodes,
        // which will get added to values array while running incrementalWrite
        // @see https://jira.blazegraph.com/browse/BLZG-1708
        if (numValues + bnodesTotalCount - bnodesResolvedCount + arity > values.length)
            return true;

        return false;
        
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
    private BigdataValue getDistinctTerm(final BigdataValue term, final boolean addIfAbsent) {

        if (term == null)
        	return null;
        
        if (term instanceof BNode) {

            /*
             * Canonicalizing map for blank nodes.
             * 
             * Note: This map MUST stay in effect while reading from a given
             * source and MUST be cleared (or set to null) before reading from
             * another source.
             */
            
            final BigdataBNode bnode = (BigdataBNode)term;
            
        	final BigdataStatement stmt = bnode.getStatement();
        	
            if (stmt != null) {
            	
            	bnode.setStatement(valueFactory.createStatement(
            			(BigdataResource) getDistinctTerm(stmt.getSubject(), true),
            			(BigdataURI) getDistinctTerm(stmt.getPredicate(), true),
            			(BigdataValue) getDistinctTerm(stmt.getObject(), true)
            			));
            	
            	/*
            	 * Do not "add if absent".  This is not a real term, just a
            	 * composition of other terms.
            	 */
            	return bnode;
            	
            } else {
            
	            // the BNode's ID.
	            final String id = bnode.getID();
	
	            if (bnodes == null) {
	
	                /*
                     * Allocating canonicalizing map for blank nodes. Note:
                     * Using linked hash map since we have to iterate over this
                     * in order to decide how many resolved and unresolved blank
                     * nodes remain in the map per
                     * https://jira.blazegraph.com/browse/BLZG-1708 (DataLoader
                     * fails with ArrayIndexOutOfBoundsException).
                     */
	                bnodes = new LinkedHashMap<String, BigdataBNode>(bufferCapacity);
	                
	                bnodesTotalCount = 0;
	                
	                bnodesResolvedCount = 0;
	
	                // insert this blank node into the map.
	                bnodes.put(id, bnode);
	
	            } else {
	
	                // test canonicalizing map for blank nodes.
	                final BigdataBNode existingBNode = bnodes.get(id);
	
	                if (existingBNode != null) {
	
	                    /*
	                     * Return existing blank node with same ID, do not
	                     * add since not absent.
	                     */
	                    return existingBNode;
	
	                }
	
	                // insert this blank node into the map.
	                bnodes.put(id, bnode);
	                
	            }
	            
	            // keep track on #of unresolved IVs, which require
	            // adding to values array on running incrementalWrite
	            // @see https://jira.blazegraph.com/browse/BLZG-1708
	            // (DataLoader fails with ArrayIndexOutOfBoundsException)
	            if (bnode.getIV() == null) {
	            	
	            	bnodesTotalCount++;
	            	
	            }
	            
            }
            
//            return term;
            
        } else {
        
	        /*
	         * Other kinds of terms use a map whose scope is limited to the terms
	         * that are currently in the buffer. This keeps down the heap demand
	         * when reading very large documents.
	         */
	        
			// TODO BLZG-1532 (JAVA8) replace with putIfAbsent()
	        final BigdataValue existingTerm = distinctTermMap.get(term);
	        
	        if (existingTerm != null) {
	            
	            // return the pre-existing term.
	            
	            if(DEBUG) {
	                
	                log.debug("duplicate: "+term);
	                
	            }
	            
	            if (equals(existingTerm, RDF_SUBJECT, RDF_PREDICATE, RDF_OBJECT, RDF_TYPE, RDF_STATEMENT)) {
	            	
	                if (addIfAbsent) {
	                	
	                	addTerm(term);
	                	
	                }
	                
	            }
	            
	            /*
	             * Term already exists, do not add.
	             */
	            return existingTerm;
	            
	        }
	
            if(DEBUG) {
                
                log.debug("new term: "+term);
                
            }
            
	        // put the new term in the map.
	        if (distinctTermMap.put(term, term) != null) {
	            
	            throw new AssertionError();
	            
	        }
	        
        }
        
        if (addIfAbsent) {
        	
        	addTerm(term);
        	
        }
        
        // return the new term.
        return term;
        
    }
    
    private void addTerm(final BigdataValue term) {
    	
    	if (term == null)
    		return;
    	
        if (term instanceof URI) {

            numURIs++;

            values[numValues++] = term;

        } else if (term instanceof BNode) {

        	/*
        	 * Handle bnodes separately, in incrementalWrite().
        	 */
        	
//            if (!statementIdentifiers) {
//
//                numBNodes++;
//
//                values[numValues++] = term;
//
//            }

        } else {

            numLiterals++;

            values[numValues++] = term;

        }

    }
    
    /**
     * Adds the values and the statement into the buffer.
     * 
     * @param _s
     *            The subject.
     * @param _p
     *            The predicate.
     * @param _o
     *            The object.
     * @param _c
     *            The context (may be null).
     * @param type
     *            The statement type.
     * 
     * @throws IndexOutOfBoundsException
     *             if the buffer capacity is exceeded.
     * 
     * @see #nearCapacity()
     */
    protected void handleStatement(Resource _s, URI _p, Value _o, Resource _c,
            final StatementEnum type) {
        
    	// silently strip context in quads mode. See #1086.
    	_c = database.isQuads() ? _c : null;
       
    	if (DEBUG) {
    		
    		log.debug("handle stmt: " + _s + ", " + _p + ", " + _o + ", " + _c);
    		
    	}
    	
//    	if (arity == 3) c = null;
    	
        final BigdataResource s = (BigdataResource) 
        		getDistinctTerm(valueFactory.asValue(_s), true);
        final BigdataURI p = (BigdataURI) 
        		getDistinctTerm(valueFactory.asValue(_p), true);
        final BigdataValue o = 
        		getDistinctTerm(valueFactory.asValue(_o), true);
        final BigdataResource c = (BigdataResource) 
        		getDistinctTerm(valueFactory.asValue(_c), true);
        
        /*
         * Form the BigdataStatement object now that we have the bindings.
         */

        final BigdataStatement stmt = valueFactory.createStatement(s, p, o, c, type);

        /*
         * Specifically looking for reification syntax:
         * _:sid rdf:type Statement .
         * _:sid rdf:subject <S> .
         * _:sid rdf:predicate <P> .
         * _:sid rdf:object <O> .
         */
        if (statementIdentifiers && s instanceof BNode) {
        	
        	if (equals(p, RDF_SUBJECT, RDF_PREDICATE, RDF_OBJECT)) {
        		
	    		final BigdataBNodeImpl sid = (BigdataBNodeImpl) s;
	    		
	        	if (sid.getStatement() != null) {

	        		checkSid(sid, p, o);
	        		
	        		log.warn("seeing a duplicate value for " + sid + ": " + p +"=" + o);
	        		
	        		return;
	        		
	        	}
	
	    		if (reifiedStmts == null) {
	    			
	    			reifiedStmts = new HashMap<BigdataBNodeImpl, ReifiedStmt>();
	    			
	    		}
	    		
	    		final ReifiedStmt reifiedStmt;
	    		if (reifiedStmts.containsKey(sid)) {
	    			
	    			reifiedStmt = reifiedStmts.get(sid);
	    			
	    		} else {
	    			
	    			reifiedStmt = new ReifiedStmt();
	    			
	    			reifiedStmts.put(sid, reifiedStmt);
	    			
	    		}
	    				
	    		reifiedStmt.set(p, o);	
	    		
	            if (DEBUG) 
	                log.debug("reified piece: "+stmt);
	            
	            if (reifiedStmt.isFullyBound(arity)) {
	            	
	            	sid.setStatement(reifiedStmt.toStatement(valueFactory));
	            	
	            	reifiedStmts.remove(sid);
	            	
	            }
	            
	            return;
	            
        	} else if (equals(o, RDF_STATEMENT) && equals(p, RDF_TYPE)) {
        		
        		/*
        		 * Ignore these statements.
        		 * 
        		 * _:sid rdf:type rdf:Statement .
        		 */
        		return;
        		
        	}

        }
        
        // add to the buffer.
        stmts[numStmts++] = stmt;

        numTotalStmts++;

		final Future<Void> f = ft;

		if (f != null && f.isDone()) {

			/*
			 * We are transferring batches to a queue, but the task draining the
			 * queue is no longer running.
			 */

			try {
			
				f.get(); // get the future. Expect ExecutionException.
				
				// Nothing thrown, but task draining queue is not running so it
				// is still a problem.
				throw new RuntimeException("Writer is done?");
				
			} catch (InterruptedException e) {
				
				// Propagate interrupt
				Thread.currentThread().interrupt();
				
			} catch (ExecutionException ex) {
				
				throw new RuntimeException(ex);
				
			}
			
        }
        
//        if (c != null && statementIdentifiers && c instanceof BNode) {
//        	
//        	((BigdataBNodeImpl) c).setStatement(stmt);
//        	
//        }

    }
    
    private void checkSid(final BigdataBNode sid, final URI p, final Value o) {
    	
    	final BigdataStatement stmt = sid.getStatement();
    	
    	if ((p == RDF_SUBJECT && stmt.getSubject() != o) ||
    			(p == RDF_PREDICATE && stmt.getPredicate() != o) ||
    				(p == RDF_OBJECT && stmt.getObject() != o)) {
    		
			throw new UnificationException("sid cannot refer to multiple statements");
    		
    	}
    	
    }
    
    private boolean equals(final BigdataValue v1, final BigdataValue... v2) {
    	
		if (v2.length == 1) {
			
			return _equals(v1, v2[0]);
			
		} else {
			
			for (BigdataValue v : v2) {
				
				if (_equals(v1, v))
					return true;
				
			}
			
			return false;
			
		}

    }
    
    private boolean _equals(final BigdataValue v1, final BigdataValue v2) {
		
		return v1 == v2;
		
//    	if (distinct) {
//
//    		return v1 == v2;
//
//    	} else {
//    		
//    		return v1.equals(v2);
//    		
//    	}
    	
    }
    
    private static class ReifiedStmt implements Statement {

    	/**
		 * 
		 */
		private static final long serialVersionUID = -7706421769807306702L;
		
		private BigdataResource s;
    	private BigdataURI p;
    	private BigdataValue o;
    	private BigdataResource c;
    	
    	public ReifiedStmt() {
    	}
    	
    	public boolean isFullyBound(final int arity) {
    		return s != null && p != null && o != null && (arity > 3 ? c != null : true);
    	}
    	
		@Override
		public BigdataResource getContext() {
			return c;
		}

		@Override
		public BigdataValue getObject() {
			return o;
		}

		@Override
		public BigdataURI getPredicate() {
			return p;
		}

		@Override
		public BigdataResource getSubject() {
			return s;
		}

		public void set(final URI p, final BigdataValue o) {
			
			if (p.toString().equals(RDF.SUBJECT.toString())) {
				
				setSubject((BigdataResource) o);
			
			} else if (p.toString().equals(RDF.PREDICATE.toString())) {
				
				setPredicate((BigdataURI) o);
			
			} else if (p.toString().equals(RDF.OBJECT.toString())) {
				
				setObject(o);
			
//			} else if (p.equals(RDF.CONTEXT)) {
//				
//				setPredicate((URI) c);
//			
			} else {

				throw new IllegalArgumentException();
				
			}
			
		}
		
		public void setSubject(final BigdataResource s) {
			this.s = s;
		}

		public void setPredicate(final BigdataURI p) {
			this.p = p;
		}

		public void setObject(final BigdataValue o) {
			this.o = o;
		}

//		public void setContext(final BigdataResource c) {
//			this.c = c;
//		}
		
	    @Override
	    public String toString() {
	        
	        return "<" + s + ", " + p + ", " + o + ", " + c + ">";

	    }
	    
	    public BigdataStatement toStatement(final BigdataValueFactory vf) {
	    	
	    	return vf.createStatement(s, p, o, c);
	    	
	    }
    	
	}

}
