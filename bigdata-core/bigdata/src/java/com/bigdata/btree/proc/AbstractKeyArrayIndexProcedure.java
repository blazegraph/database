/*

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
 * Created on Jan 7, 2008
 */

package com.bigdata.btree.proc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Errors;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.btree.raba.SubRangeRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.view.FusedView;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.service.ndx.NopAggregator;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

/**
 * Abstract base class supports compact serialization and compression for remote
 * {@link IKeyArrayIndexProcedure} execution (procedures may be executed on a
 * local index, but they are only (de-)serialized when executed on a remote
 * index).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see BLZG-1537 (Schedule more IOs when loading data)
 */
abstract public class AbstractKeyArrayIndexProcedure<T> extends
        AbstractIndexProcedure<T> implements IKeyArrayIndexProcedure<T>,
        Externalizable {
	
	private static final Logger log = Logger.getLogger(AbstractKeyArrayIndexProcedure.class);

	/*
	 * FIXME These parameters should be specified from the derived class and
	 * default from the environment or be set dynamically through ergonomics. It
	 * might be possible to do this by sharing a global reader thread pool for
	 * the journal.
	 * 
	 * Right now they can be set from the environment, but this is just for
	 * testing purposes. We need a better mechanisms / ergonomics. Some of the
	 * main drivers should be the #of keys, the size of the index (total range
	 * count), and the amount of observed scatter (inverse of locality) on the
	 * index (ID2TERM has none on write (but a fair amount on read), SPO has
	 * little on write, TERM2ID can have a lot of write if there are UUIDs, OSP
	 * has a lot on write).
	 */
    
	/**
	 * The index procedure will be read by at most this many reader tasks.
	 * Parallelizing this index reads let's us speed up the overall operation
	 * significantly. Set to ZERO (0) to always run in the caller's thread (this
	 * is the historical behavior).
	 */
	transient static private final int maxReaders = Integer
			.parseInt(System.getProperty(AbstractKeyArrayIndexProcedure.class.getName() + ".maxReaders", "0"));

	/**
	 * How many keys to skip over in the reader threads.
	 * <p>
	 * Note: This also sets the minimum number of keys in a batch that we hand
	 * off to the writer.
	 */
	transient static private final int skipCount = 
			Integer
			.parseInt(System.getProperty(AbstractKeyArrayIndexProcedure.class.getName() + ".skipCount", "256")); 

	/**
	 * This is multiplied by the branching factor of the index (when ZERO, the
	 * branching factor is multiplied by itself) to determine how many tuples
	 * must lie between the first key to enter a batch and the last key that may
	 * enter a batch before a reader evicts a batch to the queue. Since we get a
	 * lot of locality from the tree structure, we should not require that the
	 * key is in the same page as the first key, but only that it is close and
	 * will share most of the parents in the B+Tree ancestry.
	 */
	transient static private final int spannedRangeMultiplier = Integer
			.parseInt(System.getProperty(AbstractKeyArrayIndexProcedure.class.getName() + ".spannedRangeMultiplier", "10"));;
	
	/**
	 * The size of a sub-key-range that will be handed off by a reader to a
	 * queue. A writer will drain these key ranges and apply the index procedure
	 * to each sub-key-range in turn. This separation makes it possible to
	 * ensure that pages on in memory and that the writer only does work on
	 * pages that are already in memory.
	 * <p>
	 * 
	 * FIXME As a rule of thumb, performance is quite reasonable with a single
	 * thread running the batch until the indices grow relatively large. So we
	 * really want to increase the striping of the readers as a function of the
	 * index size and the proportion of scattered reads on the index. A large
	 * index with good update locality is not a problem. A large index with poor
	 * update locality is a big problem and requires a bunch of readers to
	 * prefetch the index pages for the writer.
	 */
	transient static private final int batchSize = Integer
			.parseInt(System.getProperty(AbstractKeyArrayIndexProcedure.class.getName() + ".batchSize", "10240"));
    
	/**
	 * The maximum depth of the queue -or- ZERO (0) to use
	 * <code>maxReaders * 2</code> (note that this is based on maxReaders, not
	 * the actual number of readers). This should be at least equal to the #of
	 * readers and could be a small multiple of that number.
	 */
	transient static private final int queueCapacity = Integer
			.parseInt(System.getProperty(AbstractKeyArrayIndexProcedure.class.getName() + ".queueCapacity", "0"));;

	static private class Stats {

		/**
		 * The #of reader batches that were assigned for the parallel execution
		 * of the index procedure.
		 */
		private final AtomicLong readerBatchCount = new AtomicLong();

		/**
		 * The #of batches that were processed by the writer.
		 */
		private final AtomicLong writerBatchCount = new AtomicLong();

	}
	
    /**
     * The object used to (de-)code the keys when they are sent to the remote
     * service.
     */
    private IRabaCoder keysCoder;
    
    /**
     * The object used to (de-)code the values when they are sent to the remote
     * service.
     */
    private IRabaCoder valsCoder;
    
    /**
     * The object used to (de-)code the keys when they are sent to the remote
     * service.
     */
    protected IRabaCoder getKeysCoder() {
    	
    	return keysCoder;
    	
    }

    /**
     * The object used to (de-)code the values when they are sent to the remote
     * service.
     */
    protected IRabaCoder getValuesCoder() {
    	
    	return valsCoder;
    	
    }

    /**
     * The keys.
     */
    private IRaba keys;

    /**
     * The values.
     */
    private IRaba vals;

    @Override
    final public IRaba getKeys() {
        
        return keys;
        
    }
    
    @Override
    final public IRaba getValues() {
        
        return vals;
        
    }

	/**
	 * Return an {@link IResultHandler} that will be used to combine the results
	 * if the index procedure is parallelized against a local index (including a
	 * scale-out shard). If a <code>null</code> is returned, then the index
	 * procedure WILL NOT be parallelized against the local index. To
	 * parallelize index procedures that do not return anything against a local
	 * index, just use {@link NopAggregator}. A non-<code>null</code> value will
	 * permit both index local parallelization of the index procedure and (in
	 * scale-out) parallelization of the index procedure across the shards as
	 * well. In order to be parallelized, the index procedure must also be
	 * marked as {@link IParallelizableIndexProcedure}.
	 * 
	 * @return The {@link IResultHandler} -or- <code>null</code>
	 * 
	 * @see NopAggregator
	 * @see IParallelizableIndexProcedure
	 * @see BLZG-1537 (Schedule more IOs when loading data)
	 */
	abstract protected IResultHandler<T, T> newAggregator();

    /**
     * De-serialization constructor.
     */
    protected AbstractKeyArrayIndexProcedure() {

    }

    /**
     * @param keysCoder
     *            The object used to serialize the <i>keys</i>.
     * @param valsCoder
     *            The object used to serialize the <i>vals</i> (optional IFF
     *            <i>vals</i> is <code>null</code>).
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            The keys (<em>unsigned</em> variable length byte[]s) MUST
     *            be in sorted order (the logic to split procedures across
     *            partitioned indices depends on this, plus ordered reads and
     *            writes on indices are MUCH more efficient).
     * @param vals
     *            The values (optional, must be co-indexed with <i>keys</i>
     *            when non-<code>null</code>).
     */
    protected AbstractKeyArrayIndexProcedure(final IRabaCoder keysCoder,
            final IRabaCoder valsCoder, final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals) {

        if (keysCoder == null)
            throw new IllegalArgumentException();
        
        if (valsCoder == null && vals != null)
            throw new IllegalArgumentException();
        
        if (keys == null)
            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);

        if (fromIndex < 0)
            throw new IllegalArgumentException(Errors.ERR_FROM_INDEX);

        if (fromIndex >= toIndex )
            throw new IllegalArgumentException(Errors.ERR_FROM_INDEX);

        if (toIndex > keys.length )
            throw new IllegalArgumentException(Errors.ERR_TO_INDEX);

        if (vals != null && toIndex > vals.length)
            throw new IllegalArgumentException(Errors.ERR_TO_INDEX);

        this.keysCoder = keysCoder;
        
        this.valsCoder = valsCoder;
        
		/*
		 * FIXME I ignoring the (fromIndex, toIndex) on the original keys and
		 * values? These should really be passed through. The correctness issue
		 * probably only shows up in scale-out.
		 */
        this.keys = new ReadOnlyKeysRaba(fromIndex, toIndex, keys);

        this.vals = (vals == null ? null : new ReadOnlyValuesRaba(fromIndex,
                toIndex, vals));

    }

    /**
	 * Applies the logic of the procedure.
	 * <p>
	 * Note: For invocations where the {@link IRaba#size()} of the
	 * {@link #getKeys() keys} is large, this class breaks down the {@link IRaba
	 * keys} into a multiple key ranges to parallelize the work. If the
	 * procedure is read-only, then we can trivially parallelize the operation.
	 * When the procedure is read-write, a prefetch pattern is used to ensure
	 * that the index pages are in cache and then work is handed off to a single
	 * thread that does the actual work while obeying the single-threaded for
	 * writer constraint on the index.
	 * 
	 * @author bryan
	 * 
	 * @see BLZG-1537 (Schedule more IOs when loading data)
	 */
    @Override
	final public T apply(final IIndex ndx) {

		if (ndx instanceof IClientIndex) {
			/*
			 * The client index views already parallelize index operations
			 * across the shards so we should never hit this code path. The code
			 * will throw an exception if we do hit this code path as an aid to
			 * tracking down invalid assumptions (among them that the
			 * IResultHandler would be null on the DS/MDS nodes in scale-out
			 * since only the client has access to that object).
			 * 
			 * TODO Note: It *is* safe to just uncomment the applyOnce() call
			 * rather than throwing an exception.
			 */
			// return applyOnce(ndx, keys, vals);
			throw new UnsupportedOperationException();
		}

		/*
		 * Note: Do not parallelize small batches. A single thread is enough.
		 * 
		 * FIXME We actually we might to run parallel threads even for smaller
		 * batches if the index is large enough since the parallelism will be
		 * required to drive the disk read queue. Otherwise we will be facing
		 * additive latency from sequential disk reads. This would show up in
		 * small updates to large indices.  The point of comparison would be
		 * that large updates to large indices were more efficient. If this is
		 * observed, then we do want to parallelize small batches also if the
		 * index is large.
		 */
		final boolean smallBatch = false;//keys.size() <= batchSize;

		if (maxReaders <= 0 || smallBatch || !(this instanceof IParallelizableIndexProcedure)) {

			// Disables parallelism entirely.
			return applyOnce(ndx, keys, vals);
			
		}
    	
    	/*
		 * Obtain an aggregator that can be used to combine the results across
		 * index local splits. This index-local aggregator was introduced to
		 * support parallelizing the operation against a local index or local
		 * index view.
		 */
    	final IResultHandler<T, T> resultHandler = newAggregator();

		if (resultHandler == null) {

			/*
			 * Can't use parallelism without an aggregator.
			 * 
			 * Note: Use NopAggregator to avoid this code path and strip an
			 * index procedure against a local index even if it is not going
			 * to return anything.
			 */
			return applyOnce(ndx, keys, vals);
			
    	}

		// FIXME IMPLEMENT PARALLEL OPERATION FOR FusedView. 
		final boolean isFusedView = (ndx instanceof ILocalBTreeView) && ((ILocalBTreeView) ndx).getSourceCount() > 1;
		
		if (isFusedView && !isReadOnly()) {

			// Do not parallelize mutations against a fused view (not yet implemented).
			return applyOnce(ndx, keys, vals);

		}

    	/* If it is not a client index view, then it is one of either:
    	 * 
    	 * - UnisolatedReadWriteIndex (wrapping a BTree)
    	 * 
    	 * - ILocalBTreeView, which in turn is one of:
    	 *   - AbstractBTree (BTree or IndexSegment)
    	 *   - FusedView (or IsolatedFusedView)
    	 */

		final IRawStore store;

		if (ndx instanceof ILocalBTreeView) {

			// Note: BTree, FusedLocalView, FusedIsolatedView.
			store = ((ILocalBTreeView) ndx).getMutableBTree().getStore();

		} else if (ndx instanceof UnisolatedReadWriteIndex) {

			store = ((UnisolatedReadWriteIndex) ndx).getStore();

		} else {
			
			/*
			 * Note: This is a trap for other cases that are not covered above.
			 * Not that I am aware of any.
			 */

			throw new AssertionError("Can't get backing store for " + ndx.getClass().getName());

		}

		final ExecutorService executorService;

		if (store instanceof IIndexManager) {

			executorService = ((IIndexManager) store).getExecutorService();

		} else {

			/*
			 * What typically hits this are unit tests that are using an
			 * SimpleMemoryStore or the like rather than a Journal. To avoid
			 * breaking those tests this does not parallelize the operation.
			 */
			
			return applyOnce(ndx, keys, vals);

//			throw new AssertionError("Can't get ExecutorService for " + store.getClass().getName());

		}
		
		try {

			if (isReadOnly()) {

				/*
				 * Simpler code path for parallelizing read-only operations. We
				 * just split up the keys among N readers. All work is done by a
				 * ReadOnlyTask for its own key-range and the results are
				 * aggregated. No locking is required since no mutation is
				 * involved.
				 */

				return applyMultipleReadersNoWriter(executorService, ndx, resultHandler);

			}

			/*
			 * Parallelize a mutable index procedure. Here we need to take
			 * additional precautions since underlying BTree class is only
			 * thread-safe for a single writer.
			 */

			return applyMultipleReadersOneWriter(executorService, ndx, false/* readOnly */, resultHandler);

		} catch (ExecutionException | InterruptedException ex) {

			throw new RuntimeException(ex);

		}

    }

	/**
	 * Read-only version with concurrent readers.
	 * 
	 * @param ndx
	 * @param resultHandler
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private T applyMultipleReadersNoWriter(final ExecutorService executorService, final IIndex ndx,
			final IResultHandler<T, T> resultHandler) throws InterruptedException, ExecutionException {

		// This is the #of keys in the keys IRaba.
		final int keysSize = keys.size();

		// Track statistics.
		final Stats stats = new Stats();
		
		// Setup readers.
		final List<Callable<Void>> readerTasks = new LinkedList<Callable<Void>>();
		{
			
			/*
			 * Determine how many tuples to assign to each reader. Round up. The
			 * last reader will wind up a bit short if the tuples can not be
			 * divided evenly by the #of readers.
			 * 
			 * Note: If there is not enough data for a single batch, then we use
			 * only one reader.
			 */
			final int readerSize = Math.max(batchSize, (int) Math.ceil(keysSize / (double) maxReaders));
			int fromIndex = 0, toIndex = -1;
			boolean done = false;
			while (!done) {
				toIndex = fromIndex + readerSize;
				if (toIndex > keysSize) {
					/*
					 * This will be the last reader.
					 * 
					 * Note: toIndex is an exclusive upper bound. Allowable
					 * values are in 0:rangeCount-1. Setting toIndex to nstmts
					 * (aka rangeCount) sets it to one more than the last legal
					 * toIndex. The reader will notice that the toIndex is GTE
					 * the rangeCount and use a [null] toKey to read until the
					 * last tuple in the index.
					 * 
					 * We validate that we have read and transferred rangeCount
					 * tuples to the mapgraph-runtime as a cross check.
					 */
					toIndex = keysSize;
					done = true;
				}
				readerTasks.add(new ReadOnlyTask(ndx, resultHandler, stats, new Batch(fromIndex, toIndex, keys, vals)));
				fromIndex = toIndex;
			}
			stats.readerBatchCount.set(readerTasks.size());
		}

		// Start readers. all readers are done by the time this returns (including if interrupted).
		final List<Future<Void>> readerFutures = executorService.invokeAll(readerTasks);

		// check reader futures.
		for (Future<Void> f : readerFutures) {

			f.get();

		}

		// configuration parameters. followed by invocation instance data.
		log.fatal("maxReaders=" + maxReaders //
				+ ", skipCount=" + skipCount //
				+ ", spannedRangeMultiplier=" + spannedRangeMultiplier //
				+ ", batchSize=" + batchSize //
				+ ", queueCapacity=" + queueCapacity
				// invocation instance data.
				+ ", nkeys=" + keysSize //
				+ ", nreaders=" + stats.readerBatchCount //
//				+ ", writerBatches=" + stats.writerBatchCount //
//				+ ", keys/writeBatch=" + (keysSize / stats.writerBatchCount.get()) //
				+ ", proc=" + getClass().getSimpleName()//
		);

		return resultHandler.getResult();
		
    }

	/**
	 * Task for a read-only index procedure. No locking is required. Each task
	 * just handles its sub-key-range of the original keys raba.
	 *
	 * @author bryan
	 */
	private class ReadOnlyTask implements Callable<Void> {

		private final IIndex view;
		private final Batch batch;
		private final IResultHandler<T, T> resultHandler;
		private final Stats stats; 

		/**
		 * 
		 * @param view
		 *            The index against which the procedure will be applied.
		 * @param resultHandler
		 *            Used to combine the intermediate results from the
		 *            application of the index procedure to each {@link Batch}.
		 * @param batch
		 *            A batch of keys (and optionally values) to be processed.
		 */
		ReadOnlyTask(final IIndex view, final IResultHandler<T, T> resultHandler, final Stats stats,
				final Batch batch) {

			if (view == null)
				throw new IllegalArgumentException();
			if (batch == null)
				throw new IllegalArgumentException();
			if (resultHandler== null)
				throw new IllegalArgumentException();
			if (stats== null)
				throw new IllegalArgumentException();
			
			this.view = view;
			this.batch = batch;
			this.resultHandler = resultHandler;
			this.stats = stats;
			
		}

		@Override
		public Void call() throws Exception {

			// Setup view onto the sub-key range of the keys / vals.
			
			final IRaba keysView = new SubRangeRaba(batch.keys, batch.fromIndex, batch.toIndex);

			final IRaba valsView = batch.vals == null ? null
					: new SubRangeRaba(batch.vals, batch.fromIndex, batch.toIndex);

			// Apply procedure to sub-key range.
			final T aResult = applyOnce(view, keysView, valsView);

			// aggregate results.
			resultHandler.aggregate(aResult, batch);

			return null; 

		}

	}

	/**
	 * MROW version (multiple readers, one writer).
	 * 
	 * @param ndx
	 *            A local index (any of {@link UnisolatedReadWriteIndex},
	 *            {@link BTree}, or {@link FusedView}).
	 * @param resultHandler
	 *            The handler used to aggregate results across the parallel
	 *            stripes.
	 *            
	 * @return The result.
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private T applyMultipleReadersOneWriter(final ExecutorService executorService, IIndex ndx, final boolean readOnly,
			final IResultHandler<T, T> resultHandler) throws InterruptedException, ExecutionException {

		/*
		 * Use concurrent readers.
		 * 
		 * Note: With concurrent readers the data are *NOT* transferred into a
		 * total ordering and the mapgraph runtime will need to sort on S (or
		 * SPO) before building the indices.
		 */

		/**
		 * Note: When this method is invoked for an
		 * {@link UnisolatedReadWriteIndex} , that class method hands off the
		 * inner {@link BTree} object. Since the invoking thread at the
		 * top-level owns the read or write lock for the
		 * {@link UnisolatedReadWriteIndex} (depending on whether the procedure
		 * is read only), we are not able to acquire the write lock inside of
		 * the {@link WriterTask} (unless it is run in the caller's thread) and
		 * we can not acquire the read lock in any of the {@link ReaderTask}
		 * threads. To work around this, we explicitly coordinate among the
		 * readers and with the writer thread using a read/write lock.
		 */
		final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

		// This is the #of keys in the keys IRaba.
		final int keysSize = keys.size();
		
		final int effectiveQueueCapacity;
		{
			if (queueCapacity <= 0) {
				/*
				 * Note: This is MAX readers, not the actual number of readers.
				 * We need to create the queue before we create the readers.
				 */
				effectiveQueueCapacity = maxReaders * 2;
			} else {
				effectiveQueueCapacity = queueCapacity;
			}
		}
		
		// Queue used to pass batches from readers to writer.
		final LinkedBlockingQueue<Batch> queue = new LinkedBlockingQueue<Batch>(effectiveQueueCapacity);
		
		// Track statistics.
		final Stats stats = new Stats();
		
		// Setup writer.
		final FutureTask<T> writerFuture = new FutureTask<T>(new WriterTask(lock, queue, ndx, resultHandler, stats));

		// Setup readers.
		final List<Callable<Void>> readerTasks = new LinkedList<Callable<Void>>();
		{
			
			/*
			 * Determine how many tuples to assign to each reader. Round up. The
			 * last reader will wind up a bit short if the tuples can not be
			 * divided evenly by the #of readers.
			 * 
			 * Note: If there is not enough data for a single batch, then we use
			 * only one reader.
			 */
			final int readerSize = Math.max(batchSize, (int) Math.ceil(keysSize / (double) maxReaders));
			int fromIndex = 0, toIndex = -1;
			boolean done = false;
			while (!done) {
				toIndex = fromIndex + readerSize;
				if (toIndex > keysSize) {
					/*
					 * This will be the last reader.
					 * 
					 * Note: toIndex is an exclusive upper bound. Allowable
					 * values are in 0:rangeCount-1. Setting toIndex to nstmts
					 * (aka rangeCount) sets it to one more than the last legal
					 * toIndex. The reader will notice that the toIndex is GTE
					 * the rangeCount and use a [null] toKey to read until the
					 * last tuple in the index.
					 * 
					 * We validate that we have read and transferred rangeCount
					 * tuples to the mapgraph-runtime as a cross check.
					 */
					toIndex = keysSize; 
					done = true;
				}
				readerTasks.add(new ReaderTask(readOnly, lock, queue, writerFuture, ndx, new Batch(fromIndex, toIndex, keys, vals)));
				fromIndex = toIndex;
			}
			stats.readerBatchCount.set(readerTasks.size());
		}

		try {

			// start writer.
			executorService.submit(writerFuture);

			// start readers. all readers are done by the time this returns (including if interrupted).
			final List<Future<Void>> readerFutures = executorService.invokeAll(readerTasks);

			// Readers are done. drop poison pill on writer so it will terminate. 
			// Note: will block if queue is full.
			// Note: will notice if the writer fails.
			putOnQueue(writerFuture, queue, Batch.POISON_PILL);
			
			// check reader futures.
			for (Future<Void> f : readerFutures) {

				f.get();
				
			}

			// check writer future.
			final T ret = writerFuture.get();

			// configuration parameters. followed by invocation instance data.
			log.fatal("maxReaders=" + maxReaders //
					+ ", skipCount=" + skipCount //
					+ ", spannedRangeMultiplier=" + spannedRangeMultiplier //
					+ ", batchSize=" + batchSize //
					+ ", queueCapacity=" + queueCapacity
					// invocation instance data.
					+ ", nkeys=" + keysSize //
					+ ", nreaders=" + stats.readerBatchCount //
					+ ", writerBatches=" + stats.writerBatchCount //
					+ ", keys/writeBatch=" + (keysSize / stats.writerBatchCount.get()) //
					+ ", proc=" + getClass().getSimpleName()//
			);

			return ret;

		} finally {

			/*
			 * Ensure writer is terminated.
			 * 
			 * Note: Readers will be done by the time invokeAll()
			 * returns via any code path so we do not need to cancel
			 * them here.
			 */
			writerFuture.cancel(true/* mayInterruptIfRunning */);

		}

    }
	
	/**
	 * A key-range of the caller's keys (and optionally values) to be operated
	 * on.
	 *
	 * @author bryan
	 */
	private static class Batch extends Split {

		/**
		 * The original keys and values. Code using a {@link Batch} MUST respect
		 * the {@link Split#fromIndex} when indexing into these data.
		 */
		private final IRaba keys, vals;
		
		/**
		 * 
		 * @param fromIndex
		 *            The inclusive lower bound index into the original
		 *            {@link IRaba}s (offset).
		 * @param toIndex
		 *            The exclusive upper bound index into the original
		 *            {@link IRaba}s.
		 * @param keys
		 *            The original {@link IRaba} for the keys.
		 * @param vals
		 *            The original {@link IRaba} for the values.
		 */
		Batch(final int fromIndex, final int toIndex, final IRaba keys, final IRaba vals) {
			
			super(null/* pmd */, fromIndex, toIndex);

			this.keys = keys;
			
			this.vals = vals;

		}

		private Batch() {
			super(null/* pmd */, 0, 0);
			this.keys = this.vals = null;
		}

		/**
		 * Singleton instance is used to signal the end of service for a queue.
		 */
		final private static Batch POISON_PILL = new Batch();
		
	}
	
	/**
	 * Task applies the index procedure to a specific key range.
	 * 
	 * @author bryan
	 */
	private class WriterTask implements Callable<T> {

		private final ReentrantReadWriteLock lock;
		private final IIndex ndx;
		private final LinkedBlockingQueue<Batch> queue;
		private final IResultHandler<T, T> resultHandler;
		private final Stats stats;

		/**
		 * 
		 * @param lock
		 *            Lock used to allow concurrent readers on the index or a
		 *            single thread that applies mutation to the index.
		 * @param queue
		 *            Queue used to hand off work to the writer.
		 * @param view
		 *            The index against which the procedure will be applied.
		 * @param resultHandler
		 *            Used to combine the intermediate results from the
		 *            application of the index procedure to each {@link Batch}.
		 */
		WriterTask(final ReentrantReadWriteLock lock, final LinkedBlockingQueue<Batch> queue,
				final IIndex view, final IResultHandler<T, T> resultHandler, final Stats stats) {

			if (lock == null)
				throw new IllegalArgumentException();

			if (view == null)
				throw new IllegalArgumentException();
			
			if (queue == null)
				throw new IllegalArgumentException();

			if (resultHandler == null)
				throw new IllegalArgumentException();
			
			if (stats == null)
				throw new IllegalArgumentException();
			
			this.lock = lock;
			
			this.queue = queue;
			
			this.ndx = view;
			
			this.resultHandler = resultHandler;
		
			this.stats = stats;

			if (isReadOnly()) {
				// No point. Other code path is used.
				throw new UnsupportedOperationException();
			}
			
		}
		
		@Override
		public T call() throws Exception {

			while (true) {

				// blocking take
				final Batch batch = queue.take();

				if (batch == Batch.POISON_PILL)
					break;
				if(batch.ntuples==0) throw new AssertionError("Empty batch");
				
				/*
				 * Setup sub-range for keys and values and invoke the index
				 * procedure on that sub-range.
				 */
	
				final IRaba keysView = new SubRangeRaba(batch.keys, batch.fromIndex, batch.toIndex);

				final IRaba valsView = batch.vals == null ? null
						: new SubRangeRaba(batch.vals, batch.fromIndex, batch.toIndex);

				/*
				 * Acquire write lock to avoid concurrent mutation errors in the
				 * B+Tree.
				 */
				final T aResult;

				lock.writeLock().lock();

				try {

					// invoke index procedure on sub-range.
					aResult = applyOnce(ndx, keysView, valsView);

				} finally {

					lock.writeLock().unlock();

				}

				// aggregate results.
				resultHandler.aggregate(aResult, batch);

				stats.writerBatchCount.incrementAndGet();

			}

			return resultHandler.getResult();

		}
		
	}
	
	/**
	 * Read a key-range of the SPO index into a sequence of {s,p,o} tuple
	 * {@link Batch}es and drop each one in turn onto the caller's queue.
	 * 
	 * @author bryan
	 */
	static private class ReaderTask<T> implements Callable<Void> {

		private final boolean readOnly;
		private final ReentrantReadWriteLock lock;
		private final LinkedBlockingQueue<Batch> queue;
		private final Future<T> writerFuture;
		private final IIndex view;
		private final Batch batch;

		/**
		 * 
		 * @param lock
		 *            Lock used to allow concurrent readers on the index or a
		 *            single thread that applies mutation to the index.
		 * @param queue
		 *            Queue used to hand off work to the writer.
		 * @param view
		 *            The index against which the procedure will be applied.
		 * @param batch
		 *            A batch of keys (and optionally values). The reader uses
		 *            the keys in the batch to pre-fetch the associated index
		 *            page(s) and then drops a batch (which might have only a
		 *            subset of those keys) onto the queue. The reader may
		 *            choose to break up batches when the keys do not have good
		 *            locality in the index.
		 */
		ReaderTask(final boolean readOnly, final ReentrantReadWriteLock lock, final LinkedBlockingQueue<Batch> queue,
				final Future<T> writerFuture, final IIndex view, final Batch batch) {
		
			if (lock == null)
				throw new IllegalArgumentException();
			if (queue == null)
				throw new IllegalArgumentException();
			if (writerFuture == null)
				throw new IllegalArgumentException();
			if (view == null)
				throw new IllegalArgumentException();
			if (batch == null)
				throw new IllegalArgumentException();
			
			this.readOnly = readOnly;
			this.lock = lock;
			this.queue = queue;
			this.writerFuture = writerFuture;
			this.view = view;
			this.batch = batch;
			
		}
		
		@Override
		public Void call() throws Exception {

			if (view instanceof UnisolatedReadWriteIndex
					|| (view instanceof ILocalBTreeView && ((ILocalBTreeView) view).getSourceCount() == 1)) {

				// A single B+Tree object.
				if (!(view instanceof ILinearList))
					throw new AssertionError("Unexpected index type: " + view.getClass().getName()
							+ " does not implement " + ILinearList.class.getName());

				// Note: Can't take the lock here.  Queue.put() will block if writer has lock.
				doSimpleBTree(lock, view, batch, queue);

			} else if (view instanceof ILocalBTreeView) {

				// A fused view of a mutable BTree and one or more additional B+Tree objects.
				doFusedView((ILocalBTreeView) view, batch, queue);

			} else {
				
				throw new AssertionError("Unexpected index type: " + view.getClass().getName());
				
			}

			return null;

		}

		/**
		 * This is the complex case. The index is not a single B+Tree, but some
		 * sort of fused ordered view of 2 or more B+Tree objects. For this case
		 * we do not have access to the {@link ILinearList} API.
		 * 
		 * @param view
		 * 
		 *            TODO How can we explicitly test this case and assess the
		 *            performance impact? Perhaps for a tx? This code path is
		 *            not used by scale-out since we are not parallelizing
		 *            within the thread for scale-out at this time (because the
		 *            {@link IResultHandler} is not available.)
		 */
		static private void doFusedView(final ILocalBTreeView view, final Batch batch,
				final LinkedBlockingQueue<Batch> queue) {

			if (view == null)
				throw new IllegalArgumentException();
			
			if (batch == null)
				throw new IllegalArgumentException();
			
			if (queue == null)
				throw new IllegalArgumentException();

//			 * @param firstKey
//			 *            The first key for this reader and never null (the keys are
//			 *            ordered but not necessarily dense and may not be null).
//			 * @param lastKey
//			 *            The last key for this reader and never null (the keys are
//			 *            ordered but not necessarily dense and may not be null).
//			
//			// Start at the beginning.  Proceed until then end. Then stop.
//			final byte[] firstKey = batch.keysView.get(0); // firstKey (inclusive)
//			final byte[] lastKey = batch.keysView.get(batch.keysView.size() - 1); // lastKey (inclusive)
	//
			// TODO Auto-generated method stub
//			final AbstractBTree sources[] = view.getSources();

//	    	final int effectiveBranchingFactor;
//	    	final IndexMetadata md = ndx.getIndexMetadata();
//			if (ndx instanceof ILocalBTreeView) {
//				int tmp = 0;
//				final int nsources = ((ILocalBTreeView) ndx).getSourceCount();
//				for (AbstractBTree btree : ((ILocalBTreeView) ndx).getSources()) {
//					tmp += btree.getBranchingFactor();
//				}
//				tmp /= nsources;
//				effectiveBranchingFactor = tmp;
//	    	} else {
//	    		effectiveBranchingFactor = md.getIndexSegmentBranchingFactor();
//	    	}
	    	
//			// Iterator used to seek along the index.
//			final ITupleCursor<?> itr = (ITupleCursor<?>) ndx.rangeIterator(firstKey, null/* toKey */, 1/* capacity */,
//					IRangeQuery.KEYS | IRangeQuery.VALS | IRangeQuery.CURSOR, null/* filterCtor */);
	//
//			// Note: itr.hasNext() will force in the page for the current key.
//			while (itr.hasNext()) {

			throw new UnsupportedOperationException();
			
		}

		/**
		 * This is the simple case. We have either an
		 * {@link UnisolatedReadWriteIndex} or an {@link AbstractBTree}.
		 * 
		 * @param ndx
		 *            Either an {@link UnisolatedReadWriteIndex} or an
		 *            {@link AbstractBTree}.
		 *            
		 * @throws InterruptedException
		 */
		private void doSimpleBTree(final ReentrantReadWriteLock lock, final IIndex ndx, final Batch batch,
				final LinkedBlockingQueue<Batch> queue) throws InterruptedException {

			if (lock == null)
				throw new IllegalArgumentException();

			if (ndx == null)
				throw new IllegalArgumentException();

			if (!(ndx instanceof UnisolatedReadWriteIndex) && !(ndx instanceof AbstractBTree)) {
				throw new IllegalArgumentException("Index may be either: " + UnisolatedReadWriteIndex.class.getName()
						+ " or " + AbstractBTree.class.getName() + ", but have " + ndx.getClass().getName());
			}

			if (batch == null)
				throw new IllegalArgumentException();

			/*
			 * The maximum #of keys in a leaf.
			 */
			final int branchingFactor = ndx.getIndexMetadata().getBranchingFactor();

			final long evictRange = branchingFactor
					* (spannedRangeMultiplier == 0 ? branchingFactor : spannedRangeMultiplier);

			// The linear list position into the B+Tree for the current key.
			long firstIndex = -1;

			// The index into the raba for the start of the batch.
			int firstRabaIndex = batch.fromIndex;

			// The current index into the raba.
			int currentRabaIndex = firstRabaIndex;

			// Loop over the current index into the raba.
			for (; currentRabaIndex < batch.toIndex; currentRabaIndex += skipCount) {

				final byte[] currentKey = batch.keys.get(currentRabaIndex);

				/*
				 * Advance index to the next caller's key.
				 * 
				 * Note: the return is an insert position. It will be negative
				 * if the key is not found in the index. If it is negative it is
				 * converted into an insert position. Either way it indicates
				 * where in the index the key exists / would be inserted.
				 */
				final long indexOf;
				lock.readLock().lock();
				try {
					if (writerFuture.isDone()) {
						/*
						 * If the writer hits an error condition, then the index
						 * can be left is an inconsistent state. At this point
						 * we MUST NOT read on the index.
						 */
						throw new RuntimeException("Writer is dead?");
					}
					long n = ((ILinearList) ndx).indexOf(currentKey);
					if (n < 0) {

						// Convert to an insert position.
						n = -(n + 1);

					}
					indexOf = n;

				} finally {
					lock.readLock().unlock();
				}

				if (firstIndex == -1) {

					firstIndex = indexOf;

				}

				/*
				 * The #of tuples that lie between the first key accepted and
				 * the current key (or the insert position for the current key).
				 */
				final long spannedRange = indexOf - firstIndex;
				
				assert spannedRange >= 0; // should not be negative.
				
				if (spannedRange >= evictRange) {

					// Evict a batch (blocking put).
					putOnQueue(new Batch(firstRabaIndex, currentRabaIndex, batch.keys, batch.vals));

					// start a new batch.
					firstRabaIndex = currentRabaIndex;
					
				}
				
			}

			if ((currentRabaIndex - firstRabaIndex) > 0) {

				// Last batch (blocking put).
				putOnQueue(new Batch(firstRabaIndex, batch.toIndex, batch.keys, batch.vals));

			}

		}
		
		/**
		 * Evict a batch (blocking put, but spins to look for an error in the
		 * writer {@link Future}).
		 * 
		 * @param batch
		 *            A batch.
		 * 
		 * @throws InterruptedException
		 */
		private void putOnQueue(final Batch batch) throws InterruptedException {

			AbstractKeyArrayIndexProcedure.putOnQueue(writerFuture, queue, batch);

		}
		
	} // ReaderTask

	/**
	 * Evict a batch (blocking put, but spins to look for an error in the
	 * <i>writerFuture</i> to avoid a deadlock if the writer fails).
	 * 
	 * @param writerFuture
	 *            The {@link Future} of the {@link WriterTask} (required).
	 * @param queue
	 *            The queue onto which the batches are being transferred
	 *            (required).
	 * @param batch
	 *            A batch (required).
	 * 
	 * @throws InterruptedException
	 */
	private static void putOnQueue(final Future<?> writerFuture, final LinkedBlockingQueue<Batch> queue,
			final Batch batch) throws InterruptedException {

		while (!writerFuture.isDone()) {

			if (queue.offer(batch, 100L, TimeUnit.MILLISECONDS)) {
				return;
			}

		}
		
		if (writerFuture.isDone()) {

			/*
			 * This is most likely to indicate either an error or interrupt in the writer.
			 */

			throw new RuntimeException("Writer is done, but reader still working?");

		}
		
	}
	
    /**
	 * Apply the procedure to the specified key range of the index.
	 * 
	 * @param ndx
	 *            The index.
	 * @return
	 */
	abstract protected T applyOnce(final IIndex ndx, final IRaba keys, final IRaba vals);

    @Override
    final public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        readMetadata(in);

        final boolean haveVals = in.readBoolean();

        {
            
            // the keys.
            
            final int len = in.readInt();
            
            final byte[] a = new byte[len];
            
            in.readFully(a);
            
            keys = keysCoder.decode(FixedByteArrayBuffer.wrap(a));
            
//            keys = new MutableValuesRaba(0, 0, new byte[n][]);
//
//            getKeysCoder().read(in, keys);
            
        }

		if (haveVals) {
        
            /*
             * Wrap the coded the values.
             */
            
            // the byte length of the coded values.
            final int len = in.readInt();
            
            // allocate backing array.
            final byte[] a = new byte[len];
            
            // read the coded record into the array.
            in.readFully(a);
            
            // wrap the coded record.
            vals = valsCoder.decode(FixedByteArrayBuffer.wrap(a));
            
//            vals = new MutableValuesRaba( 0, 0, new byte[n][] );
//        
//            getValuesCoder().read(in, vals);
            
        } else {
            
            vals = null;
            
        }
        
    }

    @Override
    final public void writeExternal(final ObjectOutput out) throws IOException {

        writeMetadata(out);

        out.writeBoolean(vals != null); // haveVals

        final DataOutputBuffer buf = new DataOutputBuffer();
        {

            // code the keys
            final AbstractFixedByteArrayBuffer slice = keysCoder.encode(keys,
                    buf);

            // The #of bytes in the coded keys.
            out.writeInt(slice.len());

            // The coded keys.
            slice.writeOn(out);

        }

        if (vals != null) {

            // reuse the buffer.
            buf.reset();
            
            // code the values.
            final AbstractFixedByteArrayBuffer slice = valsCoder.encode(vals,
                    buf);

            // The #of bytes in the coded keys.
            out.writeInt(slice.len());

            // The coded keys.
            slice.writeOn(out);

        }

    }

    /**
     * Reads metadata written by {@link #writeMetadata(ObjectOutput)}.
     * 
     * @param in
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected void readMetadata(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final byte version = in.readByte();

        switch (version) {
        case VERSION0:
            break;
        default:
            throw new IOException();
        }
        
//        fromIndex = 0;
//
//        toIndex = (int) LongPacker.unpackLong(in);

        keysCoder = (IRabaCoder) in.readObject();

        valsCoder = (IRabaCoder) in.readObject();

    }

    /**
     * Writes metadata (not the keys or values, but just other metadata used by
     * the procedure).
     * <p>
     * The default implementation writes out the {@link #getKeysCoder()} and the
     * {@link #getValuesCoder()}.
     * 
     * @param out
     * 
     * @throws IOException
     */
    protected void writeMetadata(final ObjectOutput out) throws IOException {

        out.write(VERSION0);
        
//        final int n = toIndex - fromIndex;
//        
//        LongPacker.packLong(out, n);
        
        out.writeObject(keysCoder);

        out.writeObject(valsCoder);
        
    }
    
    private static final byte VERSION0 = 0x00;

    /**
     * A class useful for sending some kinds of data back from a remote
     * procedure call (those readily expressed as a <code>byte[][]</code>).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class ResultBuffer implements Externalizable {
        
        /**
         * 
         */
        private static final long serialVersionUID = 3545214696708412869L;

        private IRaba vals;

        private IRabaCoder valsCoder;
        
        /**
         * De-serialization ctor.
         *
         */
        public ResultBuffer() {
            
        }
        
        /**
         * 
         * @param n
         *            #of values in <i>a</i> containing data.
         * @param a
         *            The data.
         * @param valSer
         *            The data are serialized using using this object. Typically
         *            this is the value returned by
         *            {@link ITupleSerializer#getLeafValuesCoder()}.
         */
        public ResultBuffer(final int n, final byte[][] a,
                final IRabaCoder valsCoder) {

            assert n >= 0;
            assert a != null;
            assert valsCoder != null;
                        
            this.vals = new ReadOnlyValuesRaba(0/* fromIndex */, n/* toIndex */, a);
            
            this.valsCoder = valsCoder;
            
        }
        
        public IRaba getValues() {
            
            return vals;
            
        }
        
        /**
         * @deprecated by {@link #getValues()}
         */
        public int getResultCount() {
            
            return vals.size();
            
        }
        
        /**
         * @deprecated by {@link #getValues()}
         */
        public byte[] getResult(final int index) {

            return vals.get(index);

        }

        @Override
        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            final byte version = in.readByte();
            
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new IOException();
            }

//            final int n = in.readInt();

            // The values coder.
            valsCoder = (IRabaCoder) in.readObject();

            // The #of bytes in the coded values.
            final int len = in.readInt();
            
            final byte[] b = new byte[len];
            
            in.readFully(b);
            
            // Wrap the coded values.
            vals = valsCoder.decode(FixedByteArrayBuffer.wrap(b));

//            a = new MutableValuesRaba(0/* fromIndex */, 0/* toIndex */,
//                    n/* capacity */, new byte[n][]);
//
//            valSer.read(in, a);
            
        }

        @Override
        public void writeExternal(final ObjectOutput out) throws IOException {

            out.writeByte(VERSION0);
            
//            out.writeInt(a.size());

            // The values coder.
            out.writeObject(valsCoder);
            
            // Code the values.
            final AbstractFixedByteArrayBuffer slice = valsCoder.encode(vals,
                    new DataOutputBuffer());
            
            // The #of bytes in the coded keys.
            out.writeInt(slice.len());

            // The coded keys.
            slice.writeOn(out);
            
//            valSer.write(out, a);

        }
        
        private static final byte VERSION0 = 0x00;
        
    }

    /**
     * A class useful for sending a logical <code>boolean[]</code> back from a
     * remote procedure call.
     * 
     * @todo provide run-length coding for bits?
     * 
     * @todo use {@link LongArrayBitVector} for more compact storage?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class ResultBitBuffer implements Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = 1918403771057371471L;

        private int n;

        /**
         * @todo represent using a {@link BitVector}. {@link LongArrayBitVector}
         *       when allocating. Directly write the long[] backing bits
         *       (getBits()) onto the output stream. Reconstruct from backing
         *       long[] when reading. Hide the boolean[] from the API by
         *       modifying {@link #getResult()} to accept the index of the bit
         *       of interest or to return the {@link BitVector} directly.
         */
//        private BitVector a;
        private boolean[] a;

        transient private int onCount;
        
        /**
         * De-serialization ctor.
         */
        public ResultBitBuffer() {
            
        }

        /**
         * 
         * @param n
         *            #of values in <i>a</i> containing data.
         * @param a
         *            The data.
         * @param onCount
         *            The #of bits which were on in the array.
         */
        public ResultBitBuffer(final int n, final boolean[] a, final int onCount) {

            if (n < 0)
                throw new IllegalArgumentException();
            
            if (a == null)
                throw new IllegalArgumentException();
            
            if (onCount < 0 || onCount > n)
                throw new IllegalArgumentException();
            
            this.n = n;

            this.a = a;

            /*
             * Note: The onCount is a require parameter because this class is
             * used in non-RMI contexts as well where it is not deserialized and
             * hence onCount will not be set unless it is done in this
             * constructor.
             */
 
            this.onCount = onCount;

        }

        public int getResultCount() {
            
            return n;
            
        }

        /**
         * 
         */
        public boolean[] getResult() {

            return a;

        }
        
        /**
         * Return the #of bits which are "on" (aka true).
         */
        public int getOnCount() {
            
            return onCount;
            
        }

        @Override
        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            final byte version = in.readByte();

            switch (version) {
            case VERSION0:
                break;
            default:
                throw new UnsupportedOperationException("Unknown version: "
                        + version);
            }

            @SuppressWarnings("resource")
            final InputBitStream ibs = new InputBitStream((InputStream) in,
                    0/* unbuffered */, false/* reflectionTest */);

            n = ibs.readNibble();

//            a = LongArrayBitVector.getInstance(n);
            a = new boolean[n];

            for (int i = 0; i < n; i++) {

                final boolean bit = ibs.readBit() == 1 ? true : false;
//                a.set(i, bit);

                if (a[i] = bit)
                    onCount++;
                
            }
            
        }

        @Override
        public void writeExternal(final ObjectOutput out) throws IOException {

            out.writeByte(VERSION);
            
            @SuppressWarnings("resource")
            final OutputBitStream obs = new OutputBitStream((OutputStream) out,
                    0/* unbuffered! */, false/*reflectionTest*/);

            obs.writeNibble(n);
            
//            obs.write(a.iterator());

            for (int i = 0; i < n; i++) {

                obs.writeBit(a[i]);

            }

            obs.flush();
            
        }

        /**
         * The initial version.
         */
        private static final transient byte VERSION0 = 0;

        /**
         * The current version.
         */
        private static final transient byte VERSION = VERSION0;

    }

    /**
     * Knows how to aggregate {@link ResultBuffer} objects.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class ResultBufferHandler implements
            IResultHandler<ResultBuffer, ResultBuffer> {

        private final byte[][] results;
        private final IRabaCoder valsCoder;

        public ResultBufferHandler(final int nkeys, final IRabaCoder valsCoder) {

            this.results = new byte[nkeys][];

            this.valsCoder = valsCoder;
            
        }

        @Override
        public void aggregate(final ResultBuffer result, final Split split) {

            final IRaba src = result.getValues();
            
            for (int i = 0, j = split.fromIndex; i < split.ntuples; i++, j++) {

                results[j] = src.get(i);
                
            }
            
        }

        /**
		 * The aggregated results.
		 */
        @Override
        public ResultBuffer getResult() {

            return new ResultBuffer(results.length, results, valsCoder);

        }

    }

    /**
     * Knows how to aggregate {@link ResultBitBuffer} objects.
     */
    public static class ResultBitBufferHandler implements
            IResultHandler<ResultBitBuffer, ResultBitBuffer> {

        private final boolean[] results;
        
        /**
         * I added this so I could encode information about tuple modification
         * that takes more than one boolean to encode.  For example, SPOs can
         * be: INSERTED, REMOVED, UPDATED, NO_OP (2 bits).
         */
        private final int multiplier;
        
        private final AtomicInteger onCount = new AtomicInteger();

        public ResultBitBufferHandler(final int nkeys) {
            
            this(nkeys, 1);
            
        }
        
        public ResultBitBufferHandler(final int nkeys, final int multiplier) {

			results = new boolean[nkeys * multiplier];

			this.multiplier = multiplier;

        }

        @Override
        public void aggregate(final ResultBitBuffer result, final Split split) {

            System.arraycopy(result.getResult(), 0, results, 
                    split.fromIndex*multiplier,
                    split.ntuples*multiplier);
            
            onCount.addAndGet(result.getOnCount());

        }

        /**
         * The aggregated results.
         */
        @Override
        public ResultBitBuffer getResult() {

            return new ResultBitBuffer(results.length, results, onCount.get());

        }

    }

    /**
     * Counts the #of <code>true</code> bits in the {@link ResultBitBuffer}(s).
     */
    public static class ResultBitBufferCounter implements
            IResultHandler<ResultBitBuffer, Long> {

        private final AtomicLong ntrue = new AtomicLong();

        public ResultBitBufferCounter() {

        }

        @Override
        public void aggregate(final ResultBitBuffer result, final Split split) {

            int delta = 0;

            for (int i = 0; i < result.n; i++) {

                if (result.a[i])
                    delta++;

            }

            this.ntrue.addAndGet(delta);

        }

        /**
         * The #of <code>true</code> values observed in the aggregated
         * {@link ResultBitBuffer}s.
         */
        @Override
        public Long getResult() {

            return ntrue.get();

        }

    }

}
