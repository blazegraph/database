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
 * Created on Aug 27, 2008
 */

package com.bigdata.service.proxy;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Remote;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import cern.colt.Arrays;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Wraps an {@link RemoteAsynchronousIterator} so that it looks like an
 * {@link IAsynchronousIterator} again.
 * <p>
 * This implementation uses the caller's {@link ExecutorService} to run a task
 * that migrates data from the remote {@link IAsynchronousIterator} into a local
 * {@link BlockingBuffer}. The client implementation uses purely local
 * operations to test the contents of the local {@link BlockingBuffer}. This
 * means that operations like {@link #hasNext(long, TimeUnit)} may be used with
 * short timeouts without possibility of blocking IO.
 * <p>
 * Note: This class is NOT thread-safe. There should be only a single process
 * draining the iterator.
 * <p>
 * Note: In order to conserve resources, the caller is advised to use
 * {@link #start(ExecutorService)} before making any other requests. This will
 * allow the caller to specify an {@link ExecutorService} that will be tasked
 * with buffering elements from the {@link RemoteAsynchronousIterator}. When
 * {@link #start(ExecutorService)} is not invoked explicitly, a new
 * {@link Thread} will be allocated. Typically, an {@link ExecutorService} can
 * be much more efficient than creating a new {@link Thread}.
 * 
 * @todo Note that we only use three methods to implement
 *       {@link ClientAsynchronousIterator} - hasNext(timeout,unit),
 *       isExhausted(), and next(). The rest of the
 *       {@link RemoteAsynchronousIterator} API does not need to be implemented.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientAsynchronousIterator<E> implements IAsynchronousIterator<E>,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -6809878299983373766L;

    protected transient static final Logger log = Logger
            .getLogger(ClientAsynchronousIterator.class);
    
    protected transient static final boolean INFO = log.isInfoEnabled();

    protected transient static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * The source - the reference is cleared when the iterator is closed.
     * <p>
     * Note: This is a {@link Remote} interface. All methods on this interface
     * are RMI calls and both declare and can throw {@link IOException}.
     */
    private final RemoteAsynchronousIterator<E> remoteIterator;
    
    /**
     * The capacity that will be used when we allocate the {@link #localBuffer}.
     */
    private final int capacity;
    
    /**
     * Elements are transferred asynchronously from the {@link #remoteIterator}
     * into the {@link #localBuffer} by a {@link ReaderTask}. This will be
     * <code>null</code> until the iterator is {@link #start(ExecutorService)}ed.
     */
    private transient BlockingBuffer<E> localBuffer;

    /**
     * The {@link Future} for the {@link ReaderTask} that fills the
     * {@link #localBuffer} from the {@link #remoteIterator}. This will be
     * <code>null</code> until the iterator is {@link #start(ExecutorService)}ed.
     */
    private transient Future<Void> future;

    /**
     * Iterator draining the {@link #localBuffer}. This will be
     * <code>null</code> until the iterator is {@link #start(ExecutorService)}ed.
     */
    private transient IAsynchronousIterator<E> localIterator;
    
    /**
     * 
     * @param remoteIterator
     *            A proxy for the remote iterator from which the elements are
     *            being read.
     * @param capacity
     *            The capacity of the internal {@link IBlockingBuffer}.
     */
    public ClientAsynchronousIterator(
            final RemoteAsynchronousIterator<E> remoteIterator,
            final int capacity) {

        if (remoteIterator == null)
            throw new IllegalArgumentException();

        if (capacity <= 0)
            throw new IllegalArgumentException();
        
        // save reference.
        this.remoteIterator = remoteIterator;

        this.capacity = capacity;
        
        /*
         * Note: The [localBuffer] is not defined until someone calls start()!
         */
        
    }
    
    /**
     * Start the {@link ReaderTask} that will populate the local buffer with
     * elements from the remote iterator.
     * <p>
     * When {@link #start(ExecutorService)} is not invoked explicitly, a new
     * {@link Thread} will be allocated and the {@link ReaderTask} will be run
     * on that {@link Thread}. Typically, an {@link ExecutorService} can be
     * much more efficient than creating a new {@link Thread}. Therefore the
     * caller is encourged to break encapsulation and specify the
     * {@link ExecutorService} on which the {@link ReaderTask} will run
     * directly.
     * 
     * @throws IllegalStateException
     *             if the {@link ReaderTask} is already running.
     */
    public void start(final ExecutorService executorService) {
        
        if (executorService == null)
            throw new IllegalArgumentException();

        if (future != null)
            throw new IllegalStateException();
        
        // allocate local buffer.
        this.localBuffer = new BlockingBuffer<E>(capacity);
    
        // start reader.
        this.future = executorService.submit(new ReaderTask());
        
        /*
         * Set future on the local buffer so that we can interrupt it when the
         * client side of the iterator is closed.
         */
        this.localBuffer.setFuture(future);
        
        // save reference to iterator draining the [localBuffer].
        this.localIterator = localBuffer.iterator();

    }

    /**
     * Start the {@link ReaderTask} iff it is not already running.
     */
    final protected void start() {

        if (future != null)
            return;

        /*
         * Note: This is resource intensive if you have a lot of these
         * asynchronous iterators. That is why the caller should use
         * start(ExecutorService) and save themselves some pain.
         */

        this.executorService = Executors
                .newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());

        start(executorService);

        log.warn("Running reader on private executor service");

    }

    /**
     * Assigned IFF we need to create our own {@link ExecutorService} on which
     * to run the {@link ReaderTask}.
     * <p>
     * Note: The whole reason for {@link #start(ExecutorService)} is that the
     * {@link ExecutorService} is NOT serializable. Typically this task will be
     * run on the {@link ExecutorService} associated with the local
     * {@link IBigdataFederation} object for a {@link DataService}.
     */
    private transient ExecutorService executorService;
    
    /**
     * Timeout for the {@link ReaderTask} when it invokes
     * {@link RemoteAsynchronousIterator#hasNext(long, TimeUnit)}.
     * <p>
     * Note: A timeout is used instead of
     * {@link RemoteAsynchronousIterator#hasNext()} in order to avoid blocking
     * during an RMI call on a remote thread.
     */
    static protected transient final long timeout = 1L;

    /**
     * The units for {@link #timeout}.
     */
    static protected transient final TimeUnit unit = TimeUnit.MILLISECONDS;
    
    /**
     * Enables low-level trace of the {@link ReaderTask}.
     */
    static protected transient final boolean trace = false;
    
    /**
     * Task polls the remote {@link IAsynchronousIterator} copying elements into
     * the #localBuffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ReaderTask implements Callable<Void> {

        public Void call() throws Exception {

            final Thread t = Thread.currentThread();

            try {

                /*
                 * Note: remoteIterator#hasNext() is an RMI call. Therefore it
                 * is NOT interruptable.
                 * 
                 * In order to make this loop terminate in a timely fashion if
                 * the local Thread is interrupted, we poll the remoteIterator
                 * with a timeout and use remoteIterator#isExhausted() to
                 * determine whether a [false] return really represents an
                 * exhausted source.
                 * 
                 * That helps as long as the RMI itself does not hang since
                 * hasNext( timeout, unit) will terminate quickly.
                 */
                
                while (true) {

                    if (trace)
                        System.err.println("Polling remote iterator: ");
                    
                    // poll remote iterator with timeout.
                    if (!remoteIterator.hasNext(timeout, unit)) {

                        if (trace)
                            System.err.print('~');
                        
                        if (t.isInterrupted()) {

                            // thread interrupted, so we are done.
                            break;
                            
                        }

                        if (remoteIterator.isExhausted()) {

                            if (trace)
                                System.err.println("Remote iterator is exhausted.");
                            
                            // remote iterator is exhausted, so we are done.
                            break;

                        }

                        if (!localBuffer.isOpen()) {

                            // local buffer is closed, so we are done.
                            break;

                        }

                        /*
                         * continue polling until source proven to be exhausted,
                         * this thread is interrupted, or the source is known to
                         * have an element available for transfer to the local
                         * buffer.
                         */

                        continue;
                        
                    }

                    /*
                     * read an element from the remote iterator. while this is a
                     * blocking operation we have already proven that there is
                     * an element waiting for us so the operation should have
                     * the minimum possible latency, which depends of course on
                     * how much data there is in the serialized element that we
                     * are requesting.
                     */

                    // don't call blocking method next() if we were interrupted.
                    if (t.isInterrupted()) {

                        // thread interrupted, so we are done.
                        break;
                        
                    }

                    if(trace)
                        System.err.print(">>");                        
                    
//                    final E e = remoteIterator.next();
                    final E e = remoteIterator.nextElement().get();

                    if (trace)
                        System.err
                                .println("read "
                                        + (e.getClass().getComponentType() != null ? Arrays
                                                .toString((Object[]) e)
                                                : e.toString()));
                    
                    try {

                        /*
                         * Add the element to the local buffer (blocking, but it
                         * monitors the caller's thread and will notice if it is
                         * interrupted).
                         */

                        localBuffer.add(e);

                    } catch (BufferClosedException ex) {

                        /*
                         * The local buffer has been closed so we ignore the
                         * exception and break out of the loop.
                         */

                        if (INFO)
                            log.info(ex.getLocalizedMessage());

                        // done.
                        break;

                    }

                }

                if (INFO)
                    log.info("Reader is done.");

                return null;
                
            } finally {
                
                /*
                 * Close the local buffer. It will no longer accept new elements
                 * and none will be placed into the buffer by this reader. Once
                 * the buffer is drained via the localIterator the localIterator
                 * will report that it is exhausted.
                 */
                
                localBuffer.close();
                
                /*
                 * Make sure that the remote iterator is closed.
                 * 
                 * Note: There are several reasons why the reader may be done.
                 * It may be that the remote iterator has been exhausted.
                 * However, it may also be that this task was interrupted or
                 * that the local buffer was closed.
                 */

                try {

                    if(trace) System.err.println("Closing remote iterator");
                    
                    remoteIterator.close();
                    
                } catch (Throwable ex) {
                    
                    /*
                     * Note: try-catch is here in case there is an IOException.
                     */
                    
                    log.warn(ex.getLocalizedMessage());
                    
                }

                if (executorService != null) {

                    /*
                     * We created our own service on which to the ReaderTask. Therefore
                     * we also need to make sure that it gets shutdown!
                     */

                    executorService.shutdown();
                    
                }
                
            }

        }

    }
    
    public void close() {

        if (future == null) {
            
            /*
             * The ReaderTask was never started.
             */
            
            try {

                /*
                 * Directly close the remote iterator (RMI).
                 */

                remoteIterator.close();
                
            } catch (IOException ex) {
                
                log.warn(ex);
                
            }
            
            // done.
            return;
            
        }
        
        /*
         * Close the local buffer. This will cause the ReaderTask to abort as
         * soon as it notices that the buffer has been closed since it will no
         * longer be able to add new elements into the local buffer.
         */

        localBuffer.close();

        if (!future.isDone()) {

            /*
             * Cancel the ReaderTask if it is not done.
             */
            
            future.cancel(true/* mayInterruptIfRunning */);

        }

        /*
         * Close the local iterator - the caller will no longer read from it.
         * 
         * Note: The reader task will close the remote iterator.
         */
        
        localIterator.close();

        if (executorService != null) {

            /*
             * We created our own service on which to the ReaderTask. Therefore
             * we also need to make sure that it gets shutdown!
             */

            executorService.shutdown();
            
        }

    }

    public boolean hasNext(long timeout, TimeUnit unit) {

        start();
        
        return localIterator.hasNext(timeout, unit);
        
    }

    public boolean isExhausted() {

        start();
        
        return localIterator.isExhausted();
        
    }

    public E next(long timeout, TimeUnit unit) {

        start();
        
        return localIterator.next(timeout, unit);
        
    }

    public boolean hasNext() {
        
        start();
        
        return localIterator.hasNext();
        
    }

    public E next() {

        start();
        
        return localIterator.next();
        
    }

    /**
     * Operation is not supported.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public void remove() {

        throw new UnsupportedOperationException();
        
    }
    
}
