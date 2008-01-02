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
package com.bigdata.rdf.rio;

import java.io.Reader;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.rdfxml.RdfXmlParser;

import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * Statement handler for the RIO RDF Parser that a producer-consumer queue to
 * divide the work load (not ready for use).
 * <p>
 * The producer runs rio fills in buffers, generates keys for terms, and sorts
 * terms by their keys and then places the bufferQueue onto the queue. The
 * consumer accepts a bufferQueue with pre-generated term keys and terms in
 * sorted order by those keys and (a) inserts the terms into the database; and
 * (b) generates the statement keys for each of the statement indices, ordered
 * the statement for each index in turn, and bulk inserts the statements into
 * each index in turn.
 * 
 * @todo The consumer lags behind the producer. Explore optimizations for the
 *       btree batch api to improve the insert rate. This might work better with
 *       a set of small (1-10k) {@link StatementBuffer} on the
 *       {@link ScaleOutTripleStore} in order to reduce latency.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MultiThreadedPresortRioLoader extends BasicRioLoader implements IRioLoader, StatementHandler
{
    /**
     * Terms and statements are inserted into this store.
     */
    protected final AbstractTripleStore store;
    
    /**
     * The bufferQueue capacity -or- <code>-1</code> if the {@link StatementBuffer}
     * object is signaling that no more buffers will be placed onto the
     * queue by the producer and that the consumer should therefore
     * terminate.
     */
    protected final int capacity;

    /**
     * The capacity of the bufferQueue queue.
     */
    final int queueCapacity = 10;
    
    /**
     * A queue of {@link StatementBuffer} objects that have been populated by the RIO
     * parser and are awaiting further processing.
     */
    final BlockingQueue<StatementBuffer> bufferQueue = new ArrayBlockingQueue<StatementBuffer>(
            queueCapacity); 
    
    ConsumerThread consumer;
    
    /**
     * Setup key builder that handles unicode and primitive data types.
     * <p>
     * Note: this is used when the main parser thread needs to encode term keys
     * at the same time as the {@link ConsumerThread} needs to encode statement
     * keys.
     */
    final RdfKeyBuilder keyBuilder;

    Vector<RioLoaderListener> listeners;

    /**
     * Used to bufferQueue RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser.
     */
    StatementBuffer buffer;
    
    public MultiThreadedPresortRioLoader( AbstractTripleStore store, int capacity) {

        assert store != null;
        
        assert capacity > 0;

        this.store = store;
        
        this.capacity = capacity;
        
        this.buffer = new StatementBuffer(store, capacity);
       
        this.keyBuilder = store.getKeyBuilder();
        
    }

    protected void before() {
        
        // Start thread to consume buffered data.
        consumer = new ConsumerThread(bufferQueue);
        
        consumer.start();
        
    }
    
    protected void success() {
        
        /*
         * Typically the last buffer will be non-empty so we put it on the
         * queue.
         */
        if(buffer != null) {

            putBufferOnQueue();
            
        }

        /*
         * Put a poison object on the queue to cause the ConsumerThread to
         * shutdown once it has processed everything on the queue. 
         */

        buffer = new StatementBuffer(store,-1);
        
        putBufferOnQueue();
        
        /* wait until the queue is empty.
         * 
         * @todo alternatively, wait until the ConsumerThread quits.
         */
        while(!bufferQueue.isEmpty()) {
            try {
                Thread.sleep(100); // yeild for a bit.
                checkConsumer(); // look for an error condition.
            } catch(InterruptedException ex) {
                continue;
            }
        }
        log.info("data loaded: elapsed="
                + (System.currentTimeMillis() - insertStart) + "ms");

        // check for an error condition.
        checkConsumer();

    }
    
    protected void cleanUp() {

        // shutdown the consumer.
        consumer.shutdown();

        // clear the queue.
        bufferQueue.clear();
        
        // discard the buffer.
        buffer = null;
        
    }
    
    /**
     * Checks the consumer for an error condition and if there is one then
     * throws an exception in the main thread to halt processing.
     */
    protected void checkConsumer() {

        if(consumer.error!=null) {

            throw new RuntimeException("Consumer reports error: "
                    + consumer.error);
            
        }

    }
    
    /**
     * Put the buffer onto the queue, blocking if necessary.
     * 
     * @param buffer The buffer.
     */
    protected void putBufferOnQueue() {

        assert buffer != null;

        // check for an error condition.
        checkConsumer();
        
        try {

            if (buffer.capacity > 0) {

                // pre-generate the sort keys.
                buffer.generateTermSortKeys(keyBuilder);

                // pre-sort the terms.
                buffer.sortTermsBySortKeys();
                
            }
            
            log.info("putting "
                    + (buffer.capacity == -1 ? "poison object"
                            : "buffer(haveKeys=" + buffer.haveKeys + ",sorted="
                                    + buffer.sorted + ")")
                    + " on the queue; queue length=" + bufferQueue.size());

            /*
             * Put the buffer onto the queue, blocking if necessary.
             * 
             * If the ConsumerThread stops running while we are blocked here
             * then checkConsumer() will throw an exception on the next
             * iteration.
             */
            
            bufferQueue.put(buffer);
        
            this.buffer = null;
            
        } catch (InterruptedException ex) {
            
            /*
             * @todo interpret this as an error so that there is a means
             * available to stop the parser while it is running?
             * 
             * @todo If the queue is at capacity and the {@link ConsumerThread}
             * dies should it interrupt this thread so that the parser will
             * quit?
             */
            
            throw new RuntimeException(
                    "Interrupted while waiting to put data on the queue.",
                    ex);

        }

    }
    
    /**
     * Consumers populated {@link StatementBuffer}s placed on a {@link BlockingQueue}
     * and causes their data to be inserted into the store.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ConsumerThread extends Thread {
        
        final BlockingQueue<StatementBuffer> bufferQueue;
        
        boolean shutdown = false;
        
        volatile Throwable error = null;
        
        public ConsumerThread(BlockingQueue<StatementBuffer> bufferQueue) {
            
            assert bufferQueue != null;
            
            this.bufferQueue = bufferQueue;
            
        }
        
        public void run() {
         
            log.info("Starting consumer.");
            
            StatementBuffer buffer = null;
            
            while(!shutdown) {

                try {

                    /* 
                     * obtain a buffer from the queue.
                     */
                    buffer = bufferQueue.take();

                    log.info("Consumer accepting buffer: queue length="
                            + bufferQueue.size());

                } catch (InterruptedException ex) {
                    
                    /*
                     * If we are interrupted then continue from the top of the
                     * loop. This gives a caller a means to force shutdown by
                     * causing [shutdown] to be true and interrupting this
                     * thread.
                     */
                    
                    continue;
                    
                }
                
                if(buffer.capacity == -1 ) {
                    
                    /*
                     * This indicates that no more buffers will be placed onto
                     * the queue and that this thread should quit immediately.
                     */ 
                    
                    log.info("Consumer accepted poison object - quitting now.");

                    return;
                    
                }
                
                try {

                    buffer.flush();
                    
                } catch(RuntimeException ex) {
                    
                    /*
                     * Set [error] to inform the producer that this thread has
                     * quit.
                     * 
                     * FIXME We may need to have a reference to the producer
                     * thread so that we can interrupt it.
                     */
                    
                    log.error("While loading data: "+ex, ex);
                 
                    error = ex;
                    
                    return;
                    
                }
                
                buffer = null;
                
            }
            
        }
        
        /**
         * Shutdown the thread (abnormal termination).
         */
        public void shutdown() {

            log.info("will shutdown.");
            
            // mark thread for shutdown.
            this.shutdown = true;

            while( isAlive() ) {
                
                yield(); // yield so that the ConsumerThread can run().
                
//                System.err.print('.');
                
            }
            
            log.info("did shutdown.");
            
        }

    }
    
    public void handleStatement( Resource s, URI p, Value o ) {

        /* 
         * When the bufferQueue could not accept three of any type of value plus 
         * one more statement then it is considered to be near capacity and
         * is flushed to prevent overflow. 
         */
        if(buffer.nearCapacity()) {

            putBufferOnQueue();
            
            // allocate a new buffer.
            buffer = new StatementBuffer(store,capacity);
            
            // fall through.
            
        }
        
        // add the terms and statement to the bufferQueue.
        buffer.handleStatement(s,p,o,StatementEnum.Explicit);
        
        stmtsAdded++;
        
        if ( stmtsAdded % 100000 == 0 ) {
            
            notifyListeners();
            
        }
        
    }
    
}
