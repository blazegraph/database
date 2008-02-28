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
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bigdata.btree.ITupleIterator;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Iterator visits {@link SPO}s reading from a statement index. The iterator
 * optionally supports asynchronous read ahead and may be used to efficiently
 * obtain the top N statements in index order.
 * 
 * @todo write tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOIterator implements ISPOIterator {

    static protected final Logger log = Logger.getLogger(SPOIterator.class);
    
    /**
     * The maximum #of statements that will be buffered by the iterator.
     */
    public static final transient int MAXIMUM_CAPACITY = 100 * Bytes.kilobyte32;
    
    private boolean open = true;
    
    /**
     * The object that encapsulates access to, and operations on, the statement
     * index that will be used by this iterator.
     */
    private final IAccessPath accessPath;
    
    /**
     * The maximum #of statements to read from the index -or- ZERO (0) if all
     * statements will be read.
     */
    private final int limit;
    
    /**
     * The actual capacity of the buffer (never zero).
     */
    private final int capacity;
    
    /**
     * Optional filter. When non-<code>null</code>, only matching statements
     * will be vistied.
     */
    private final ISPOFilter filter;
    
    /**
     * The #of statements that have been read <strong>from the source</strong>
     * and placed into the buffer. All such statements will also have passed the
     * optional {@link ISPOFilter}.
     */
    private int numBuffered;

    /**
     * The #of statements that have been read by the caller using
     * {@link #next()}.
     */
    private int numReadByCaller;
    
    /**
     * The #of chunks that have been read by the caller.
     */
    private int nchunks = 0;
    
    /**
     * Identifies the statement index that is being traversed.
     */
    private final KeyOrder keyOrder;
    
    /**
     * A buffer holding {@link SPO}s that have not been visited. Statements
     * that have been visited are taken from the buffer, making room for new
     * statements which can be filled in asynchronously by the {@link Reader}.
     */
    private ArrayBlockingQueue<SPO> buffer;
    
    /**
     * The source iterator reading on the selected statement index.
     */
    private ITupleIterator src;
    
    /**
     * The executor service for the {@link Reader} (iff the {@link Reader} runs
     * asynchronously).
     */
    private final ExecutorService readService;
    
    /**
     * Set to true iff an asynchronous {@link Reader} is used AND there is
     * nothing more to be read.
     */
    private AtomicBoolean readerDone = new AtomicBoolean(false);

    /**
     * The minimum desirable chunk size for {@link #nextChunk()}.
     */
    final int MIN_CHUNK_SIZE = 50000;
    
    /**
     * If NO results show up within this timeout then {@link #nextChunk()} will
     * throw a {@link RuntimeException} to abort the reader - the probably cause
     * is a network outage.
     */
    final long TIMEOUT = 3000;
    
    /**
     * This is always defined for this class.
     */
    public KeyOrder getKeyOrder() {
        
        return keyOrder;
        
    }
    
    /**
     * Create an {@link SPOIterator} that buffers an iterator reading from one
     * of the statement indices.
     * 
     * @param accessPath
     *            The access path to be used by the iterator (including the from
     *            and to keys formed from a triple pattern).
     * @param limit
     *            The maximum #of statements that will be read from the index
     *            -or- ZERO (0) to read all statements.
     * @param capacity
     *            The maximum #of statements that will be buffered. When ZERO
     *            (0) the iterator will range count the access path fully buffer
     *            if there are less than {@link #MAXIMUM_CAPACITY} statements
     *            selected by the triple pattern. When non-zero, the caller's
     *            value is used - this gives you control when you really, really
     *            want to have something fully buffered, e.g., for an in-memory
     *            self-join.
     * @param async
     *            When true, asynchronous read-ahead will be used to refill the
     *            buffer as it becomes depleted. When false, read-ahead will be
     *            synchronous (this is useful when you want to read at most N
     *            statements from the index).
     * @param filter
     *            An optional filter. When non-<code>null</code>, only
     *            matching statements will be visited.
     */
    public SPOIterator(IAccessPath accessPath, int limit, int capacity,
            boolean async, ISPOFilter filter) {

        assert accessPath != null;
        
        assert limit >= 0;
        
        assert capacity >= 0;
        
        this.limit = limit;
        
        if(limit != 0) {
            
            capacity = limit;
            
        }
        
        /*
         * The range count computed for the access path.
         * 
         * Note: The range count is generally an upper bound rather than an
         * exact value.
         */

        final long rangeCount = accessPath.rangeCount();
        
        if(capacity == 0) {

            /*
             * Attempt to fully buffer the statements.
             */
            
            if (capacity>MAXIMUM_CAPACITY || rangeCount > MAXIMUM_CAPACITY) {

                /*
                 * If the capacity would exceed the maximum then we limit
                 * the capacity to the maximum.
                 */
                
                capacity = MAXIMUM_CAPACITY;

            } else {
                
                capacity = (int) rangeCount;
                
            }

        } else {
            
            if (capacity > rangeCount) {
            
                /*
                 * If the caller has over-estimated the actual range count for
                 * the index then reduce the capacity to the real range count.
                 * This makes it safe for the caller to request a capacity of 1M
                 * SPOs and only a "right-sized" buffer will be allocated.
                 * 
                 * Note: The range count is generally an upper bound rather than
                 * an exact value.
                 */
                
                capacity = (int) rangeCount;

                /*
                 * Note: If the caller is making a best effort attempt to read
                 * everything into memory AND the data will fit within the
                 * caller's specified capacity, then we disable asynchronous
                 * reads so that they will get everything in one chunk.
                 */

                async = false;
                
            }
            
        }
        
        if (Math.min(limit, rangeCount) < 1000) {
            
            // Disable async reads if we are not reading much data.
            
            async = false;
            
        }
        
        if(capacity==0) {
            
            /*
             * Note: The ArrayBlockingQueue has a minimum capacity of ONE (1).
             * 
             * Note: You SHOULD use the SPOArrayIterator when the limit is small
             * as it is lighter weight to instantiate.
             */
            
            capacity = 1;
            
        }
        
        this.capacity = capacity;

        this.filter = filter;
        
        this.accessPath = accessPath;

        this.keyOrder = accessPath.getKeyOrder();
        
        // @todo in scale-out version send the filter to rangeQuery?
        this.src = accessPath.rangeQuery();
        
        this.buffer = new ArrayBlockingQueue<SPO>(capacity);

        if(async) {
            
            readService = Executors.newSingleThreadExecutor(DaemonThreadFactory
                    .defaultThreadFactory());

            readService.submit(new Reader());
            
        } else {
            
            /*
             * Pre-fill the buffer.
             */
            
            readService = null;
            
            fillBuffer();
            
        }
        
    }

    /**
     * Reads from the statement index, filling the {@link SPOIterator#buffer}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class Reader implements Callable<Object> {

        /**
         * Runs the {@link Reader}.
         * 
         * @return <code>null</code>.
         */
        public Object call() throws Exception {
       
            while (src.hasNext()) {

                final SPO spo = new SPO(keyOrder, src);

                if (filter != null && !filter.isMatch(spo)) {

                    // does not satisify the filter.
                    continue;
                    
                }

                try {

                    /*
                     * Note: This will block if the buffer is at capacity.
                     */
                    
                    buffer.put(spo);

                    numBuffered++;
                    
                } catch (InterruptedException ex) {

                    throw new RuntimeException(ex);

                }


                if(limit != 0 && numBuffered == limit) {
                    
                    // We have read all that we are going to read.

                    log.info("Reached limit="+limit);
                    
                    break;
                    
                }
                
            }

            // Nothing left to read.
            
            readerDone.set(true);

            return null;
            
        }
        
    }
    
    /**
     * (Re-)fills the buffer up to its capacity or the exhaustion of the source
     * iterator.
     * 
     * @return false if the buffer is still empty.
     */
    private boolean fillBuffer() {

        assertOpen();
        
        if(readService!=null) {
            
            // This method MUST NOT be invoked when using the async reader.
            
            throw new AssertionError();

        }

        try {
            // log.info("(Re-)filling buffer: remainingCapacity="
            // + buffer.remainingCapacity());

            while (src.hasNext() && buffer.remainingCapacity() > 0) {

                if (limit != 0 && numBuffered == limit) {

                    // We have read all that we are going to read.

                    log.info("Reached limit=" + limit);

                    return false;

                }

                final SPO spo = new SPO(keyOrder, src);

                if(filter != null && !filter.isMatch(spo)) {
                    
                    continue;
                    
                }
                
                try {

                    buffer.put(spo);

                    numBuffered++;

                } catch (InterruptedException ex) {

                    throw new RuntimeException(ex);

                }

            }

            // false if the buffer is still empty.

            return !buffer.isEmpty();

        } finally {

            log.info("(Re-)filled buffer: size=" + buffer.size()
                    + ", remainingCapacity=" + buffer.remainingCapacity()
                    + ", done=" + !src.hasNext());

        }

    }
    
    public boolean hasNext() {
        
        if(!open) return false;

        if(buffer.isEmpty()) {

            /*
             * The buffer is empty, but there may be more data available from
             * the underlying iterator.
             */
            
            if (readService != null) {
            
                // async reader - so wait on it.
                
                awaitReader();
            
            } else {
                
                // sync reader - so fill the buffer in this thread.
                
                fillBuffer();
                
            }
        
            if (buffer.isEmpty()) {

                // the buffer is still empty, so the iterator is exhausted.
                
                return false;
                
            }
            
        }

        // at least one SPO in the buffer.
        
        return true;
        
    }

    public SPO next() {

        if (!hasNext()) {

            throw new NoSuchElementException();
        
        }
        
        final SPO spo;
        
        try {
            
            spo = buffer.take();
            
        } catch(InterruptedException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        numReadByCaller++;
        
        return spo;
        
    }

    /**
     * Returns a chunk whose size is the #of statements currently in the buffer.
     * <p>
     * Note: When asynchronous reads are used, the buffer will be transparently
     * refilled and should be ready for a next chunk by the time you are done
     * with this one.
     */
    public SPO[] nextChunk() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }
        
        if(readService!=null) {
            
            // make sure that we fill the buffer before we deliver a chunk.
            
            awaitReader();
            
        }
        
        // there are at least this many in the buffer.
        
        final int n = buffer.size();
        
        // allocate the array.
        
        SPO[] stmts = new SPO[ n ];
        
        for(int i=0; i<n; i++) {
            
            stmts[ i ] = next();
            
        }

        log.info("chunkSize=" + n + ", nchunks=" + nchunks + ", #read(caller)="
                + numReadByCaller + ", #read(src)=" + numBuffered);
        
        return stmts;
        
    }

    public SPO[] nextChunk(KeyOrder keyOrder) {

        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] stmts = nextChunk();

        if (keyOrder != this.keyOrder) {

            // sort into the required order.

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        return stmts;

    }
    
    /**
     * Await some data from the {@link Reader}.
     * <p>
     * Note: If there is some data available this will continue to wait until at
     * least {@link #MIN_CHUNK_SIZE} statements are available from the
     * {@link Reader} -or- until the reader signals that it is
     * {@link #readerDone done}. This helps to keep up the chunk size and hence
     * the efficiency of batch operations when we might otherwise get into a
     * race with the {@link Reader}.
     */
    private void awaitReader() {
        
        if( readService == null) {
            
            /*
             * This method MUST NOT be invoked unless you are using the async
             * reader.
             */
            
            throw new AssertionError();
            
        }

        final long begin = System.currentTimeMillis();
        
        /*
         * Wait for at least N records to show up.
         */
        
        final int N = capacity < MIN_CHUNK_SIZE ? capacity : MIN_CHUNK_SIZE;
        
        while (buffer.size() < N && !readerDone.get()) {
            
            try {
                
                Thread.sleep(100);
                
            } catch (InterruptedException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            long elapsed = System.currentTimeMillis() - begin;
            
            if (elapsed > TIMEOUT && buffer.isEmpty()) {

                throw new RuntimeException("Timeout after " + elapsed + "ms");
                
            }
            
        }
        
    }
    
    /**
     * 
     * FIXME Once the problems with traversal with concurrent modification of
     * the BTree have been resolved this method should be modified such that
     * this class will buffer no more than some maximum #of statements and
     * {@link #remove()} should be implemented.
     * <p>
     * Note: Traversal with concurrent modification MUST declare semantics that
     * isolate the reader from the writer in order for clients to be able to
     * read ahead and buffer results from the iterator. Otherwise the buffered
     * statements would have to be discarded in there was a concurrent
     * modification somewhere in the future visitation of the iterator.
     * <p>
     * The one simple case is removal of the current statement, especially when
     * that statement has already been buffered.
     * <p>
     * 
     * @throws UnsupportedOperationException
     */
    public void remove() {
        
        assertOpen();

        throw new UnsupportedOperationException();
        
    }
    
    public void close() {
        
        if(!open) {
            
            // Already closed.
            
            return;
            
        }
        
        log.info("Closing iterator");
        
        open = false;
        
        if(readService!=null) {
            
            // immediate shutdown.
            readService.shutdownNow();
            
            try {

                readService.awaitTermination(500, TimeUnit.MILLISECONDS);
                
            } catch (InterruptedException e) {
                
                log.warn("Read service did not terminate: "+e);
                
            }
            
        }
        
        // discard buffer.
        
        buffer.clear();
        
        buffer = null;
        
        // discard the source iterator.
        
        src = null;
        
    }
    
    private final void assertOpen() {
        
        if (!open)
            throw new IllegalStateException();
        
    }
    
}
