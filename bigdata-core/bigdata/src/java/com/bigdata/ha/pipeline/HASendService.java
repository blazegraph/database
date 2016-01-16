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

package com.bigdata.ha.pipeline;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.HASendState;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.Haltable;

/**
 * A service for sending raw {@link ByteBuffer}s across a socket. This service
 * supports the HA write pipeline. This service is designed to be paired with an
 * {@link HAReceiveService}, which typically is running on a different host. The
 * {@link HASendService} provides only an efficient raw data transfer. The HA
 * write pipeline coordinates the transfer of data using RMI messages which tell
 * the receiver how much data to expect, the checksum of the data, etc.
 * 
 * <h2>Implementation</h2>
 * 
 * This class has a private single-threaded Executor to which it submits a
 * {@link SendTask}. The {@link SendTask} will open a blocking-mode
 * {@link SocketChannel} to the service at the configured
 * {@link InetSocketAddress} and send the bytes remaining in a
 * {@link ByteBuffer} to that service on that {@link SocketChannel}. The data
 * will be sent on the socket using
 * {@link WritableByteChannel#write(ByteBuffer)}, which is optimized for the NIO
 * transfer of direct {@link ByteBuffer}s. Since this is a blocking-mode
 * connection, the write operation will block until all bytes have been sent or
 * the current thread is interrupted, e.g., by cancelling its Future.
 * <p>
 * The {@link SendTask} verifies that all bytes were sent as a post-condition
 * (position() == limit). If there is any problem, then the
 * {@link SocketChannel} is closed and the original exception is thrown out of
 * {@link SendTask#call()}. If the socket is closed from the other end while we
 * are still writing data, then that exception gets thrown out as well. The
 * socket connection is closed as a post-condition (it does not survive across
 * multiple sends). Closing the socket connection after each success or
 * unsuccessful send gives us a strong indication of success or failure for the
 * data transfer which is independent of the RMI message and makes it trivial to
 * re-synchronize the {@link HASendService} since it is basically stateless.
 * <p>
 * Note: This class exposes its synchronization mechanism to
 * {@link HAReceiveService}.
 * 
 * @see HAReceiveService
 * 
 * @author Martyn Cutcher
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HASendService {
	
    private static final Logger log = Logger.getLogger(HASendService.class);

    /**
     * The Internet socket address of the receiving service.
     */
    private final AtomicReference<InetSocketAddress> addrNext = new AtomicReference<InetSocketAddress>();

    /**
     * A single threaded executor on which {@link SendTask}s will be executed.
     */
    private final AtomicReference<ExecutorService> executorRef = new AtomicReference<ExecutorService>();

    /**
     * The {@link SocketChannel} for communicating with the downstream
     * {@link HAReceiveService}.
     */
	final private AtomicReference<SocketChannel> socketChannel = new AtomicReference<SocketChannel>();

	/*
	 * Note: toString() must be thread-safe.
	 */
	@Override
    public String toString() {

        return super.toString() + "{addrNext=" + addrNext + "}";
        
    }

    /**
     * Return the current address to which this service will send data.
     * 
     * @return The current address -or- <code>null</code> if no address is set
     *         at this time.
     * 
     * @see #start(InetSocketAddress)
     */
    public InetSocketAddress getAddrNext() {
        
        return addrNext.get();
        
    }
    
    
    /**
     * Designated constructor (flyweight initialization).
     * 
     * @see #start(InetSocketAddress)
     */
    public HASendService() {

        this(true/* blocking */);

    }

    /**
     * Note: This constructor is not exposed yet. We need to figure out whether
     * to allow the configuration of the socket options and how to support that.
     * 
     * @param blocking
     */
    private HASendService(final boolean blocking) {

        this.blocking = blocking;
        
    }

    /**
     * <code>true</code> iff the client socket will be setup in a blocking mode.
     * This is the historical behavior until at least Dec 10, 2013.
     */
    private final boolean blocking;
    
    /**
     * Extended to ensure that the private executor service is always
     * terminated.
     */
    @Override
    protected void finalize() throws Throwable {
        
        terminate();
        
        super.finalize();
        
    }

    /**
     * Return <code>true</code> iff running at the moment this method is
     * evaluated.
     */
    boolean isRunning() {

        return executorRef.get() != null;

    }

//    /**
//     * Return the address of the receiving service (may be <code>null</code>).
//     */
//    InetSocketAddress getAddrNext() {
//
//        return addr.get();
//
//    }
    
    /**
     * Starts a thread which will transfer data to a service listening at the
     * specified {@link InetSocketAddress}. A {@link SocketChannel} will be
     * opened to the specified the connection to the socket specified in the
     * constructor and start the thread pool on which the payloads will be send.
     * <p>
     * Note: This class exposes its synchronization mechanism to
     * {@link HAReceiveService}.
     * 
     * @param addrNext
     *            The Internet socket address of the downstream service.
     * 
     * @see #terminate()
     * 
     * @throws IllegalArgumentException
     *             if the address is <code>null</code>.
     * @throws IllegalStateException
     *             if this service is already running.
     */
    synchronized public void start(final InetSocketAddress addrNext) {

        if (log.isDebugEnabled())
            log.debug(toString() + " : starting.");

        if (addrNext == null)
            throw new IllegalArgumentException();

        if (executorRef.get() != null) {

            // already running.
            log.error("Already running.");

            throw new IllegalStateException("Already running.");

        }

        this.addrNext.set(addrNext);

        /*
         * Note: leave null until send() so we can lazily connect to the
         * downstream service.
         */
        this.socketChannel.set(null);// openChannel(addr)

        this.executorRef.set(Executors.newSingleThreadExecutor());

        if (log.isInfoEnabled())
            log.info(toString() + " : running.");

    }

    /**
     * Immediate shutdown. Any transfer in process will be interrupted. It is
     * safe to invoke this method whether or not the service is running.
     * <p>
     * Note: This class exposes its synchronization mechanism to
     * {@link HAReceiveService}.
     */
    synchronized public void terminate() {
        if (log.isInfoEnabled())
            log.info(toString() + " : stopping.");
        final ExecutorService tmp = executorRef.getAndSet(null);
        if (tmp == null) {
            // Not running.
            if (log.isInfoEnabled())
                log.info("Service was not running.");
            return;
        }
        try {
            closeSocketChannelNoBlock();
        } finally {
            // shutdown executor.
            tmp.shutdownNow();
            // clear address.
            addrNext.set(null);
            if (log.isInfoEnabled())
                log.info(toString() + " : stopped.");
        }
    }

//    /**
//     * Close the {@link SocketChannel} to the downsteam service (blocking).
//     */
//    public void closeChannel() {
//        synchronized (this.socketChannel) {
//            closeSocketChannelNoBlock();
//        }
//    }

    /**
     * Close the {@link SocketChannel} to the downstream service (non-blocking).
     */
    private void closeSocketChannelNoBlock() {
        final SocketChannel socketChannel = this.socketChannel.get();
        if (socketChannel != null) {
            try {
                socketChannel.close();
            } catch (IOException ex) {
                log.error("Ignoring exception during close: " + ex, ex);
            } finally {
                this.socketChannel.set(null);
            }
            if (log.isInfoEnabled())
                log.info("Closed socket channel");
        }
    }

    /**
     * Send the bytes {@link ByteBuffer#remaining()} in the buffer to the
     * configured {@link InetSocketAddress}.
     * <p>
     * Note: This operation DOES NOT have a side effect on the position, limit
     * or mark for the buffer.
     * <p>
     * Note: In order to use efficient NIO operations this MUST be a direct
     * {@link ByteBuffer}.
     * 
     * @param buffer
     *            The buffer.
     * @param marker
     *            A marker that will be used to prefix the payload for the
     *            message in the write replication socket stream. The marker is
     *            used to ensure synchronization when reading on the stream.
     * 
     * @return The {@link Future} which can be used to await the outcome of this
     *         operation.
     * 
     * @throws InterruptedException
     * @throws ImmediateDownstreamReplicationException
     * 
     * @throws IllegalArgumentException
     *             if the buffer is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the buffer is empty (no bytes remaining).
     * @throws RejectedExecutionException
     *             if this service has been shutdown.
     * 
     * @todo throws IOException if the {@link SocketChannel} was not open and
     *       could not be opened.
     */
    public Future<Void> send(final ByteBuffer buffer, final byte[] marker)
            throws ImmediateDownstreamReplicationException,
            InterruptedException {

	    if (buffer == null)
            throw new IllegalArgumentException();

        if (buffer.remaining() == 0)
            throw new IllegalArgumentException();
	 
        // Note: wrapped as a read-only buffer to prevent side-effects.
        final ExecutorService tmp = executorRef.get();
        
        if (tmp == null)
            throw new IllegalStateException("Service is not running.");

        if (log.isTraceEnabled())
            log.trace("Will send " + buffer.remaining() + " bytes");

//        reopenChannel();
        
        try {

            return tmp
                    .submit(newIncSendTask(buffer.asReadOnlyBuffer(), marker));

        } catch (Throwable t) {

            launderThrowable(t);
            
            // make the compiler happy.
            throw new AssertionError();
            
        }

	}
	
    /**
     * Test the {@link Throwable} for its root cause and distinguish between a
     * root cause with immediate downstream replication, normal termination
     * through {@link InterruptedException}, {@link CancellationException}, and
     * nested {@link AbstractPipelineException}s thrown by a downstream service.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/724" > HA
     *      wire pulling and sure kill testing </a>
     */
    private void launderThrowable(final Throwable t)
            throws InterruptedException,
            ImmediateDownstreamReplicationException {

        if (Haltable.isTerminationByInterrupt(t)) {

            // root cause is interrupt or cancellation exception.
            throw new RuntimeException(t);

        }

        if (InnerCause.isInnerCause(t, AbstractPipelineException.class)) {

            /*
             * The root cause is NOT the inability to replicate to our immediate
             * downstream service. Instead, some service (not us) has a problem
             * with pipline replication.
             */

            throw new NestedPipelineException(t);

        }

        /*
         * We have a problem with replication to our immediate downstream
         * service.
         */

        throw new ImmediateDownstreamReplicationException(toString(), t);

    }
    
    /**
     * A series of timeouts used when we need to re-open the
     * {@link SocketChannel}.
     */
    private final static long[] retryMillis = new long[] { 1, 5, 10, 50, 100, 250, 250, 250, 250 };

    /**
     * (Re-)open the {@link SocketChannel} if it is closed and this service is
     * still running.
     * 
     * @return The {@link SocketChannel}.
     */
	private SocketChannel reopenChannel() {

	    /*
         * Synchronize on the socketChannel object to serialize attempts to open
         * the SocketChannel.
         */
        synchronized (socketChannel) {

            int tryno = 0;

            SocketChannel sc = null;
            
            while ((((sc = socketChannel.get()) == null) || !sc.isOpen())
                    && isRunning()) {

                try {

                    /*
                     * (Re-)open the SocketChannel.
                     * 
                     * TODO we may have to retry or play with the timeout for
                     * the socket connect request since the downstream node may
                     * see its pipelineAdd() after the upstream node sees its
                     * pipelineChange() event. For example, given a pipeline
                     * [A], when service B joins the pipeline using
                     * [B.getActor().pipelineAdd()] the following are possible
                     * sequences in which the events could be delivered to A and
                     * B.
                     * 
                     * Option 1:
                     * 
                     * B.pipelineAdd(); A.pipelineChange(null,B);
                     * 
                     * Option 2:
                     * 
                     * A.pipelineChange(null,B); B.pipelineAdd();
                     * 
                     * In option (1), we should be able to connect immediately
                     * since B will have already setup its receive service.
                     * However, in option (2), we can not connect immediately
                     * since B does not setup its receive service until after A
                     * has seen the pipelineChange() event.
                     */

                    socketChannel.set(sc = openChannel(addrNext.get()));

                    if (log.isInfoEnabled())
                        log.info("Opened channel on try: " + tryno
                                + ", addrNext=" + addrNext);

                } catch (IOException e) {

                    if (log.isInfoEnabled())
                        log.info("Failed to open channel on try: " + tryno
                                + ", addrNext=" + addrNext);

                    if (tryno < retryMillis.length) {

                        try {
                            // sleep and retry.
                            Thread.sleep(retryMillis[tryno]);
                            tryno++;
                            continue;
                        } catch (InterruptedException e1) {
                            // rethrow original exception.
                            throw new RuntimeException(e);
                        }

                    }

                    // do not wrap.
                    throw new RuntimeException(e);

                } // catch

            } // while

            return socketChannel.get();
            
        } // synchronized(socketChannel)
        
	} // reopenChannel()
	
    /**
     * Factory for the {@link SendTask}.
     * 
     * @param buffer
     *            The buffer whose data are to be sent.
     * @param marker
     *            A marker that will be used to prefix the payload for the
     *            message in the write replication socket stream. The marker is
     *            used to ensure synchronization when reading on the stream.
     *            
     * @return The task which will send the data to the configured
     *         {@link InetSocketAddress}.
     */
    protected Callable<Void> newIncSendTask(final ByteBuffer buffer, final byte[] marker) {

        return new IncSendTask(buffer, marker);
         
    }

    /**
     * Open a blocking mode socket channel to the specified socket address.
     * 
     * @param addr
     *            The socket address.
     * 
     * @return The socket channel.
     * 
     * @throws IOException
     */
    private SocketChannel openChannel(final InetSocketAddress addr)
            throws IOException {

        final SocketChannel socketChannel = SocketChannel.open();

        try {

            socketChannel.configureBlocking(blocking);

            if (log.isTraceEnabled())
                log.trace("Connecting to " + addr);

            if (!socketChannel.connect(addr)) {
                while (!socketChannel.finishConnect()) {
                    try {
                        Thread.sleep(10/* millis */);
                    } catch (InterruptedException e) {
                        // Propagate interrupt.
                        Thread.currentThread().interrupt();
                    }
                }
            }

        } catch (IOException ex) {

            log.error(ex);
            
            throw ex;
            
        }

        return socketChannel;
        
    }
    
    /**
     * This task implements the raw data transfer. Each instance of this task
     * sends the {@link ByteBuffer#remaining()} bytes in a single
     * {@link ByteBuffer} to the receiving service on a specified
     * {@link InetSocketAddress}.
     */
    protected /*static*/ class IncSendTask implements Callable<Void> {

        private final ByteBuffer data;
        private final byte[] marker;

        public IncSendTask(final ByteBuffer data, final byte[] marker) {

            if (data == null)
                throw new IllegalArgumentException();

            this.data = data;
            this.marker = marker;
        }

        @Override
        public Void call() throws Exception {

            try {

                return doInnerCall();

            } catch (Throwable t) {

                /*
                 * Log anything thrown out of this task. We check the Future of
                 * this task, but that does not tell us what exception is thrown
                 * in the Thread executing the task when the Future is cancelled
                 * and that thread is interrupted. In particular, we are looking
                 * for the InterruptedException, ClosedByInterruptException,
                 * etc.
                 */

                final SocketChannel sc = socketChannel.get();

                log.error("socketChannel="
                        + sc
                        + (sc == null ? "" : ", sc.isOpen()=" + sc.isOpen()
                                + ", sc.isConnected()=" + sc.isConnected())
                        + ", marker=" + HASendState.decode(marker) + ", cause="
                        + t, t);

                if (t instanceof Exception)
                    throw (Exception) t;

                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;

                throw new RuntimeException(t);

            }

        }

      private Void doInnerCall() throws Exception {
          
            // defer until we actually run.
            final SocketChannel socketChannel = reopenChannel();

            if (!isRunning())
                throw new RuntimeException("Not Running.");

            if (socketChannel == null)
                throw new AssertionError();
            
            // The #of bytes to transfer.
            final int remaining = data.remaining();

            if (log.isTraceEnabled())
                log.trace("Will send " + remaining + " bytes");

            try {

                int nmarker = 0; // #of marker bytes written.
                int nwritten = 0; // #of payload bytes written.

                final ByteBuffer markerBB = marker != null ? ByteBuffer
                        .wrap(marker) : null;
                
                while (nwritten < remaining) {
                	
                    if (marker != null && nmarker < marker.length) {
                    
                        nmarker += socketChannel.write(markerBB);

                        continue;
                        
                    }

                    /*
                     * Write the data. Depending on the channel, will either
                     * block or write as many bytes as can be written
                     * immediately (this latter is true for socket channels in a
                     * non-blocking mode). IF it blocks, should block until
                     * finished or until this thread is interrupted, e.g., by
                     * shutting down the thread pool on which it is running.
                     * 
                     * Note: If the SocketChannel is closed by an interrupt,
                     * then the send request for the [data] payload will fail.
                     * However, the SocketChannel will be automatically reopened
                     * for the next request (unless the HASendService has been
                     * terminated).
                     * 
                     * Note: socketChannel.write() returns as soon as the socket
                     * on the remote end point has locally buffered the data.
                     * This is *before* the Selector.select() method returns
                     * control to the application. Thus, the write() method here
                     * can succeed if the payload is transmitted in a single
                     * socket buffer exchange and the send() Future will report
                     * success even through the application code on the receiver
                     * could fail once it gets control back from select(). This
                     * twist can be a bit surprising. Therefore it is useful to
                     * write tests with both small payloads (the data transfer
                     * will succeed at the socket level even if the application
                     * logic then fails the transfer) and for large payloads.
                     * The concept of "large" depends on the size of the socket
                     * buffer.
                     */

                    final int nbytes;
                    if (false || log.isDebugEnabled()) {
                        /*
                         * Debug only code. This breaks down the payload into
                         * small packets and adds some latency between them as
                         * well. This models what is otherwise a less common,
                         * but more stressful, pattern.
                         */
                        final int limit = data.limit();
                        if (data.position() < (limit - 50000)) {
                            data.limit(data.position() + 50000);
                        }
                        nbytes = socketChannel.write(data);
                        data.limit(limit);

                        nwritten += nbytes;
                        log.debug("Written " + nwritten + " of total "
                                + data.limit());

                        if (nwritten < limit) {
                            Thread.sleep(1);
                        }
                    } else {

                        nbytes = socketChannel.write(data);
                        nwritten += nbytes;
                    }

                    if (log.isTraceEnabled())
                        log.trace("Sent " + nbytes + " bytes with " + nwritten
                                + " of out " + remaining + " written so far");

                }

            } finally {

                // do not close the socket, leave to explicit closeIncSend
                // socketChannel.close();

            }

            if (log.isTraceEnabled())
                log.trace("Sent total of " + remaining + " bytes");

            // check all data written
            if (data.remaining() != 0)
                throw new IOException("Did not write all data: expected="
                        + remaining + ", actual=" + data.remaining());

            return null;

        }

    }

}
