package com.bigdata.bop;

import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Interface for evaluating operations producing chunks of elements (tuples
 * materialized from some index of a relation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see IAccessPath
 * @see IChunkedOrderedIterator
 */
public interface ChunkedOrderedIteratorOp<E> extends BOp {

    /**
     * Well known annotations pertaining to the binding set pipeline.
     */
    public interface Annotations extends BOp.Annotations {

        /**
         * The maximum #of chunks that can be buffered before an the producer
         * would block (default {@value #DEFAULT_CHUNK_OF_CHUNKS_CAPACITY}).
         * Note that partial chunks may be combined into full chunks whose
         * nominal capacity is specified by {@link #CHUNK_CAPACITY}.
         */
        String CHUNK_OF_CHUNKS_CAPACITY = BlockingBuffer.class.getName()
                + ".chunkOfChunksCapacity";

        /**
         * Default for {@link #CHUNK_OF_CHUNKS_CAPACITY}
         */
        int DEFAULT_CHUNK_OF_CHUNKS_CAPACITY = 1000;

        /**
         * Sets the capacity of the {@link IBuffer}s used to accumulate a chunk
         * of {@link IBindingSet}s (default {@value #CHUNK_CAPACITY}). Partial
         * chunks may be automatically combined into full chunks.
         * 
         * @see #CHUNK_OF_CHUNKS_CAPACITY
         */
        String CHUNK_CAPACITY = IBuffer.class.getName() + ".chunkCapacity";

        /**
         * Default for {@link #CHUNK_CAPACITY}
         */
        int DEFAULT_CHUNK_CAPACITY = 100;

        /**
         * The timeout in milliseconds that the {@link BlockingBuffer} will wait
         * for another chunk to combine with the current chunk before returning
         * the current chunk (default {@value #DEFAULT_CHUNK_TIMEOUT}). This may
         * be ZERO (0) to disable the chunk combiner.
         */
        String CHUNK_TIMEOUT = BlockingBuffer.class.getName() + ".chunkTimeout";

        /**
         * The default for {@link #CHUNK_TIMEOUT}.
         * 
         * @todo this is probably much larger than we want. Try 10ms.
         */
        int DEFAULT_CHUNK_TIMEOUT = 1000;

        /**
         * If the estimated rangeCount for an
         * {@link AccessPath#iterator()} is LTE this threshold then use
         * a fully buffered (synchronous) iterator. Otherwise use an
         * asynchronous iterator whose capacity is governed by
         * {@link #CHUNK_OF_CHUNKS_CAPACITY}.
         */
        String FULLY_BUFFERED_READ_THRESHOLD = AccessPath.class
                .getName()
                + ".fullyBufferedReadThreadshold";

        /**
         * Default for {@link #FULLY_BUFFERED_READ_THRESHOLD}
         */
        int DEFAULT_FULLY_BUFFERED_READ_THRESHOLD = 20*Bytes.kilobyte32;

    }

    /**
     * Execute the operator, returning an iterator from which the element may be
     * read. Operator evaluation may be halted using
     * {@link ICloseableIterator#close()}.
     * 
     * @param fed
     *            The {@link IBigdataFederation} IFF the operator is being
     *            evaluated on an {@link IBigdataFederation}. When evaluating
     *            operations against an {@link IBigdataFederation}, this
     *            reference provides access to the scale-out view of the indices
     *            and to other bigdata services.
     * @param joinNexus
     *            An evaluation context with hooks for the <em>local</em>
     *            execution environment. When evaluating operators against an
     *            {@link IBigdataFederation} the {@link IJoinNexus} MUST be
     *            formulated with the {@link IIndexManager} of the local
     *            {@link IDataService} order perform efficient reads against the
     *            shards views as {@link ILocalBTreeView}s. It is an error if
     *            the {@link IJoinNexus#getIndexManager()} returns the
     *            {@link IBigdataFederation} since each read would use RMI. This
     *            condition should be checked by the operator implementation.
     * 
     * @return An iterator from which the elements may be read.
     */
    IChunkedOrderedIterator<E> eval(IBigdataFederation<?> fed,
            IJoinNexus joinNexus);

}
