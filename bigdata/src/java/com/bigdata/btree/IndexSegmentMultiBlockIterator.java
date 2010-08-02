package com.bigdata.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegment.IndexSegmentTupleCursor;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.io.DirectBufferPool;

/**
 * A fast iterator based on multi-block IO for the {@link IndexSegment}. This
 * iterator is designed for operations which will fully visit either all leaves
 * in the {@link IndexSegment} or a key-range corresponding to a substantial
 * proportion of those leaves. A direct {@link ByteBuffer} is allocated from the
 * caller's {@link DirectBufferPool} and an IO request is issued against the
 * {@link IndexSegment} to fill the {@link ByteBuffer} with as many leaves
 * spanned by the key-range as will fit into the buffer. The leaves laid out
 * contiguously in total key order in the {@link IndexSegment}. The addresses of
 * the leaves spanned by a key-range are easily identified by two key probes
 * into the nodes, and the nodes region is generally fully buffered. The #of
 * leaves spanned by a key range may be estimated as
 * (rangeCount/branchingFactor).
 * <p>
 * During traversal, each leaf is copied into a Java <code>byte[]</code> in
 * order to provide fast decode of the data in the leaf. When the buffered
 * leaves have been exhausted, another chunk of leaves will be read using
 * another multi-block IO.
 * <p>
 * You should choose this iterator if: (a) the iterator uses forward traversal
 * only; (b) the key-range includes the entire {@link IndexSegment} -or- a probe
 * reveals that more than a few leaves would be read; (c) the largest record in
 * the {@link IndexSegment} will fit within a buffer acquired from the selected
 * {@link DirectBufferPool}; and (d) it is reasonable to expect that the
 * iterator will be fully consumed by the caller.
 * <p>
 * The #of leaves which would be read can be estimated by dividing the range
 * count by the branching factor. If there are more than 2 full leaves worth of
 * data to be read this iterator will be faster than the linked leaf traversal
 * provided by {@link IndexSegmentTupleCursor} since this class will do one IO
 * rather than one per leaf.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 * 
 * @todo This is just fast forward traversal. We could support cursors based on
 *       this same model.
 * 
 *       FIXME Support compressed leaves (we have to decompress the record
 *       ourselves since the {@link IndexSegmentStore} is not being used to
 *       access the individual records).
 */
public class IndexSegmentMultiBlockIterator<E> implements ITupleIterator<E> {

    protected static final transient Logger log = Logger
            .getLogger(IndexSegmentMultiBlockIterator.class);
    
    /**
     * The {@link IndexSegment}.
     */
    private final IndexSegment seg;

    private final IndexSegmentStore store;
    
    /**
     * The pool from which we acquire the buffer and to which we will release
     * the buffer.
     */
    private final DirectBufferPool pool;

    /**
     * The buffer.
     */
    private volatile ByteBuffer buffer;

    /**
     * The inclusive lower bound -or- <code>null</code> if there is no lower
     * bound.
     */
    private final byte[] fromKey;

    /**
     * The exclusive upper bound -or- <code>null</code> if there is no upper
     * bound.
     */
    private final byte[] toKey;

    /*
     * Tuple stuff.
     */
    
    /**
     * <code>true</code> iff the iterator is exhausted (the last tuple has been
     * read from the last leaf).
     */
    private boolean exhausted = false;
    
    /**
     * The current {@link Tuple} for the {@link #tupleItr}.
     */
    private final Tuple<E> tuple;
    
    /**
     * Iterator used to scan each leaf in turn. It is <code>null</code> if there
     * is no {@link #currentLeaf} or if the {@link #currentLeaf} is exhausted.
     */
    private LeafTupleIterator<E> tupleItr = null;

    /*
     * Leaf stuff.
     */

    /**
     * The address of the first leaf to be read.
     */
    private final long firstLeafAddr;

    /**
     * The address of the last leaf to be read.
     * <p>
     * Note that the last byte to be read is obtained from
     * {@link IndexSegmentStore#getByteCount(long)}
     */
    private final long lastLeafAddr;

    /**
     * The current leaf -or- <code>null</code> if no leaves have been read. The
     * address of the current leaf is available from {@link Leaf#getIdentity()}.
     * The address of the next leaf is available from {@link Leaf#getNextAddr()}.
     */
    private ImmutableLeaf currentLeaf = null;

    /*
     * Block stuff.
     */
    
    /**
     * The byte offset of the current block in the {@link IndexSegment}.
     * Together with the {@link #blockLength}, this is used to determine which
     * leaves may be addressed within the block, when we need to read another
     * block in order to address a leaf, etc.
     */
    private long blockOffset = 0L;
    
    /**
     * The byte length of the current block.
     */
    private int blockLength = 0;

    /*
     * Counters
     */

    /** The #of leaves read so far. */
    private long leafReadCount = 0;

    /** The #of blocks read so far. */
    private long blockReadCount = 0;
    
    /**
     * 
     * @param seg
     *            The {@link IndexSegment}.
     * @param pool
     *            The pool from which a direct {@link ByteBuffer} will be
     *            acquired and into which blocks will be read from the backing
     *            file.
     * @param fromKey
     *            The inclusive lower bound -or- <code>null</code> if there is
     *            no lower bound.
     * @param toKey
     *            The exclusive upper bound -or- <code>null</code> if there is
     *            no upper bound.
     * @param flags
     */
    public IndexSegmentMultiBlockIterator(//
            final IndexSegment seg,//
            final DirectBufferPool pool,//
            final byte[] fromKey,//
            final byte[] toKey,//
            final int flags) {

        if (seg == null)
            throw new IllegalArgumentException();

        if (pool == null)
            throw new IllegalArgumentException();

        this.seg = seg;
        
        this.store = seg.getStore();

        this.pool = pool;

        this.fromKey = fromKey;

        this.toKey = toKey;

        /*
         * Check flags for unsupported options.
         */
        if ((flags & IRangeQuery.REVERSE) != 0)
            throw new IllegalArgumentException();
        if ((flags & IRangeQuery.REMOVEALL) != 0)
            throw new IllegalArgumentException();
        if ((flags & IRangeQuery.CURSOR) != 0)
            throw new IllegalArgumentException();

        this.tuple = new Tuple<E>(seg, flags);

        this.firstLeafAddr = (fromKey == null ? store.getCheckpoint().addrFirstLeaf
                : seg.findLeafAddr(fromKey));

        this.lastLeafAddr = (toKey == null ? store.getCheckpoint().addrLastLeaf
                : seg.findLeafAddr(toKey));

        if (pool.getBufferCapacity() < store.getCheckpoint().maxNodeOrLeafLength) {

            /*
             * If the buffers in the pool are too small to hold the largest
             * record in the index segment then you can not use this iterator.
             * 
             * Note: We presume that the largest record is therefore a leaf. In
             * practice this will nearly always be true as nodes have relatively
             * little metadata per tuple while leaves store the value associated
             * with the tuple.
             * 
             * Note: AbstractBTree checks for this condition before choosing
             * this iterator.
             */
            
            throw new UnsupportedOperationException(
                    "Record is larger than buffer: maxNodeOrLeafLength="
                            + store.getCheckpoint().maxNodeOrLeafLength
                            + ", bufferCapacity=" + pool.getBufferCapacity());

        }
        
        if (firstLeafAddr == 0L) {
            // Empty index segment.
            exhausted = true;
        }
        
        // these are zero since no block has been read yet.
        this.blockOffset = 0L;
        this.blockLength = 0;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is extended to ensure that the buffer is released back to the
     * {@link DirectBufferPool}.
     */
    protected void finalize() throws Throwable {

        releaseBuffer();
        
        super.finalize();
        
    }

    private ByteBuffer acquireBuffer() {
        if (buffer == null) {
            try {
                buffer = pool.acquire();
            } catch (InterruptedException e) {
                // We can not continue if the buffer is not acquired.
                throw new RuntimeException(e);
            }
        }
        return buffer;
    }

    private void releaseBuffer() {
        if (buffer != null) {
            try {
                pool.release(buffer);
            } catch (InterruptedException e) {
                // Propagate interrupt.
                Thread.currentThread().interrupt();
                return;
            }
            this.buffer = null;
        }
    }

    /**
     * Return the current leaf.
     * 
     * @return The current leaf -or- <code>null</code> iff no leaves have been
     *         read from the {@link IndexSegment}.
     */
    protected ImmutableLeaf getLeaf() {

        return currentLeaf;
        
    }
    
    public boolean hasNext() {

        return _hasNext();
        
    }

    public ITuple<E> next() {

        if (exhausted)
            throw new NoSuchElementException();
        
        return tupleItr.next();
        
    }

    public void remove() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Return <code>true</code> iff another tuple is available.
     * 
     * @return
     */
    private boolean _hasNext() {
        while (!exhausted) {
            if (tupleItr != null) {
                if (tupleItr.hasNext()) {
                    // More tuples are available from the current leaf.
                    return true;
                }
                // The current leaf is exhausted.
                tupleItr = null;
                if(log.isTraceEnabled())
                    log.trace("Current leaf is exhausted.");
            }
            if ((currentLeaf = nextLeaf()) != null) {
                // setup the tuple iterator for the next leaf.
                tupleItr = new LeafTupleIterator<E>(currentLeaf, tuple,
                        fromKey, toKey);
            } else {
                // done.
                exhausted = true;
            }
        }
        if(log.isTraceEnabled())
            log.trace("Exhausted.");
        // release the buffer back to the pool.
        releaseBuffer();
        // nothing left.
        return false;
    }

    /**
     * Return the next leaf from the {@link #buffer}. If the next leaf is not in
     * the buffer, read the next block of leaves from the backing file.
     * 
     * @return The next leaf -or- <code>null</code> iff there are no more leaves
     *         to be visited.
     */
    private ImmutableLeaf nextLeaf() {
        if (exhausted)
            throw new IllegalStateException();
        if (currentLeaf == null) {
            if (log.isTraceEnabled())
                log.trace("Reading initial leaf");
            // acquire the buffer from the pool.
            acquireBuffer();
            // Read the first block.
            nextBlock(firstLeafAddr, buffer);
            // Extract the first leaf.
            final ImmutableLeaf leaf = getLeaf(firstLeafAddr);
            // Return the first leaf.
            return leaf;
        }
        if (currentLeaf.identity == lastLeafAddr) {
            // No more leaves.
            if (log.isTraceEnabled())
                log.trace("No more leaves (end of key range)");
            return null;
        }
        /*
         * We need to return the next leaf. We get the address of the next leaf
         * from the nextAddr field of the current leaf.
         */
        final long nextLeafAddr = currentLeaf.getNextAddr();
        if (nextLeafAddr == 0L) {
            // No more leaves.
            if (log.isTraceEnabled())
                log.trace("No more leaves (end of segment)");
            return null;
        }
        /*
         * Figure out if the leaf is in the current buffer/block.
         */
        {
            final long offset = store.getOffset(nextLeafAddr);
            final int nbytes = store.getByteCount(nextLeafAddr);
            if (offset < blockOffset) {
                // going backwards in the file.
                throw new AssertionError();
            }
            if (offset + nbytes > blockOffset + blockLength) {
                // read the next block.
                nextBlock(nextLeafAddr, buffer);
            }
        }
        // extract the next leaf.
        final ImmutableLeaf leaf = getLeaf(nextLeafAddr);
        // return the current leaf.
        return leaf;
    }

    /**
     * Read a leaf from the {@link #buffer}.
     * 
     * @param addr
     *            The address of the leaf.
     *            
     * @return The leaf and never <code>null</code>.
     * 
     * @throws IllegalArgumentException
     *             if the leaf does not lie entirely within the buffer.
     */
    private ImmutableLeaf getLeaf(final long addr) {

        final long offset = store.getOffset(addr);
        
        final int nbytes = store.getByteCount(addr);
        
        if (offset < blockOffset)
            throw new IllegalArgumentException();
        
        if (offset + nbytes > blockOffset + blockLength)
            throw new IllegalArgumentException();
        
        // offset into the buffer.
        final int offsetWithinBuffer = (int)(offset - blockOffset);

        // read only view of the leaf in the buffer.
        final ByteBuffer tmp = buffer.asReadOnlyBuffer();
        tmp.limit(offsetWithinBuffer + nbytes);
        tmp.position(offsetWithinBuffer);

        // decode byte[] as ILeafData.
        final ILeafData data = (ILeafData) seg.nodeSer.decode(tmp);

        leafReadCount++;

        if (log.isTraceEnabled())
            log
                    .trace("read leaf: leafReadCount=" + leafReadCount
                            + ", addr=" + addr + "(" + store.toString(addr)
                            + "), blockOffset=" + blockOffset
                            + " offsetWithinBuffer=" + offsetWithinBuffer);

        // return as Leaf.
        return new ImmutableLeaf(seg, addr, data);
        
    }

    /**
     * Read as many leaves from the backing from into the buffer as will fit
     * using a multi-block IO.
     * <p>
     * Note: This implementation ensures that at least one full leaf will be
     * read into the block buffer. However, it does not guard against a partial
     * read of the last leaf within the buffer. In order to guarantee that we
     * only read complete leaves we would have to traverse the nodes of the
     * {@link IndexSegment} and locate the largest leaf address which could be
     * fully read. Rather than doing that, this allows a partial read of the
     * last leaf but the logic in {@link #nextLeaf()} checks to see whether a
     * leaf lies fully in the buffer and, if not, then demands the next block of
     * leaves starting with the address of the next leaf to be read. The
     * additional IO cost of a partial leaf read is trivial since this is
     * multi-block IO and we are operating at the disk transfer rate.
     * 
     * @param leafAddr
     *            The address of the leaf at which the block will start.
     * @param b
     *            The buffer into which the data will be read.
     */
    private void nextBlock(final long leafAddr, final ByteBuffer b) {
        final int minSize = store.getByteCount(leafAddr);
        if (minSize > b.capacity()) {
            /*
             * Note: This condition is checked by the constructor so you should
             * not see this error thrown from here.
             */
            throw new UnsupportedOperationException(
                    "Leaf is larger than buffer: leafSize=" + minSize
                            + ", bufferCapacity=" + b.capacity());
        }
        // the offset of the first byte we will read.
        final long startOffset = store.getOffset(leafAddr);
        // the offset of the last byte we are allowed to read.
        final long lastOffset = store.getOffset(lastLeafAddr)
                + store.getByteCount(lastLeafAddr);
        // the #of bytes that we will actually read.
        final int nbytes = (int) Math.min(lastOffset - startOffset, b
                .capacity());
        if(log.isTraceEnabled())
            log.trace("leafAddr=" + store.toString(leafAddr) + ", startOffset="
                    + startOffset + ", lastOffset=" + lastOffset + ", nbytes="
                    + nbytes);
        if (nbytes == 0) {
            throw new AssertionError("nbytes=0 : leafAddr"
                    + store.toString(leafAddr) + " : " + this);
        }
        // set the position to zero.
        b.position(0);
        // set the limit to the #of bytes to be read.
        b.limit(nbytes);
        // read the data from the file.
        try {
            store.readFromFile(startOffset, b);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        // update the offset/length in the store for the in memory block
        blockOffset = startOffset;
        blockLength = nbytes;
        blockReadCount++;
        if (log.isTraceEnabled())
            log.trace("read block: blockReadCount=" + blockReadCount
                    + ", leafAddr=" + store.toString(leafAddr)
                    + ", blockOffset=" + blockOffset + ", blockLength="
                    + blockLength);
    }

    public String toString() {
        return super.toString() + //
                "{file=" + store.getFile() + //
                ",checkpoint="+store.getCheckpoint()+//
                ",fromKey="+BytesUtil.toString(fromKey)+//
                ",toKey="+BytesUtil.toString(toKey)+//
                ",firstLeafAddr=" + store.toString(firstLeafAddr) + //
                ",lastLeafAddr=" + store.toString(lastLeafAddr) + //
                ",currentLeaf=" + (currentLeaf!=null?store.toString(currentLeaf.identity):"N/A") + //
                ",blockOffset="+blockOffset+//
                ",blockLength="+blockLength+//
                ",bufferCapacity="+pool.getBufferCapacity()+//
                ",leafReadCount="+leafReadCount+//
                ",blockReadCount="+blockReadCount+//
                "}";
        }

}
