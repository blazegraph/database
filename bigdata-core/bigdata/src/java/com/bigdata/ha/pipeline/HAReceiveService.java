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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Adler32;

import org.apache.log4j.Logger;

import com.bigdata.ha.QuorumPipelineImpl;
import com.bigdata.ha.msg.HAMessageWrapper;
import com.bigdata.ha.msg.HASendState;
import com.bigdata.ha.msg.IHAMessage;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteMessageBase;
import com.bigdata.ha.pipeline.HASendService.IncSendTask;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.ChecksumError;

/**
 * Receives data from an {@link HASendService}.
 * <p>
 * The non-blocking processing of the data cannot proceed until the message
 * parameters and an output buffer have been set. So an accept results in a task
 * to be run. The Future from this task is returned to the method called from
 * the RMI control invocation, thus allowing that method to wait for the
 * completion of the data transfer.
 * 
 * @author Martyn Cutcher
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAReceiveService<M extends HAMessageWrapper> extends Thread {

    private static final Logger log = Logger
            .getLogger(HAReceiveService.class);

    /**
     * The timeout (milliseconds) on the client {@link Selector}.
     * This provides a tradeoff for liveness when responding to 
     * a pipeline change exception (firstCause) versus spinning
     * while awaiting some bytes to read.
     */
    static private final long selectorTimeout = 500;
    
    /**
     * The timeout (milliseconds) before logging @ WARN that we are
     * blocking awaiting data on the socket from the upstream service.
     */
    static private final long logTimeout = 10000;
    
    /** The Internet socket address at which this service will listen. */
    private final InetSocketAddress addrSelf;

//    /**
//     * The Internet socket address of a downstream service to which each data
//     * transfer will be relayed as it is received (optional and may be
//     * <code>null</code>).
//     */
//    private final InetSocketAddress addrNext;

    /**
     * Optional callback hook.
     */
    private final IHAReceiveCallback<M> callback;

    /**
     * Used to relay data to a downstream service as it is received. This is
     * always allocated, but it will be running iff this service will relay the
     * data to a downstream service.
     */
    private final HASendService sendService;

    public HASendService getSendService() {
        return sendService;
    }

    private final ExecutorService executor = Executors
            .newSingleThreadExecutor();

//    private ServerSocketChannel server;
//    private FutureTask<Void> readFuture;

    /**
     * Service run state enumeration.

     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static enum RunState {

        Start(0), Running(1), ShuttingDown(2), Shutdown(3);
        
        private RunState(final int level) {
        
            this.level = level;
            
        }

        @SuppressWarnings("unused")
        private final int level;
        
    }

    /*
     * The lock and the things which it guards.
     */
    
    /**
     * The {@link Lock}.
     */
    private final Lock lock = new ReentrantLock();
    
    /**
     * {@link Condition} signaled when the {@link #waitFuture} is ready.
     * {@link #receiveData(IHAWriteMessageBase, ByteBuffer)} awaits this
     * {@link Condition}. Once signaled, it returns the {@link #waitFuture} to
     * the caller and clears {@link #waitFuture} to <code>null</code>.
     * <p>
     * The {@link Condition}s {@link #messageReady} and {@link #futureRead}
     * respectively manage the hand off of the message (to the {@link ReadTask})
     * and the {@link #waitFuture} (to the thread calling
     * {@link #receiveData(IHAWriteMessageBase, ByteBuffer)}.
     */
    private final Condition futureReady = lock.newCondition();
    
    /**
     * {@link Condition} signaled when a new {@link IHAWriteMessage} has been
     * set on {@link #message} by
     * {@link #receiveData(IHAWriteMessageBase, ByteBuffer)}.
     */
    private final Condition messageReady = lock.newCondition();
    
    /**
     * {@link RunState} for the {@link HAReceiveService}. This is used to manage
     * startup and termination state transitions.
     */
    private RunState runState = RunState.Start;
    
    /**
     * The current {@link IHAWriteMessageBase}. This message provides metadata
     * about the expected buffer transfer. This field is set by
     * {@link #receiveData(IHAWriteMessageBase, ByteBuffer)}.
     */
    private M message;
    
    /**
     * The current receive buffer. This buffer is populated with data based on
     * the expected {@link IHAWriteMessage#getSize()}. The data is verified by
     * comparing the checksum of the buffer to the expected checksum as
     * specified by {@link IHAWriteMessage#getChk()}.
     */
    private ByteBuffer localBuffer;
    
    /**
     * {@link Future} for the current buffer transfer used to await the
     * termination of that transfer by the {@link ReadTask}.
     * <p>
     * Note: The {@link #readFuture} is cleared to <code>null</code> as soon as
     * the buffer transfer is complete.
     */
    private FutureTask<Void> readFuture;
    
    /**
     * {@link Future} for the current buffer transfer used to await the
     * termination of that transfer by the thread that calls
     * {@link #receiveData(IHAWriteMessageBase, ByteBuffer)}.
     * <p>
     * Note: The {@link #waitFuture} is cleared to <code>null</code> as soon as
     * it is returned to the caller. This can occur before the buffer transfer
     * is complete. Therefore, {@link ReadTask} MUST NOT wait on the
     * {@link #waitFuture}.
     */
    private FutureTask<Void> waitFuture;

    /**
     * The Internet socket address of a downstream service to which each data
     * transfer will be relayed as it is received (optional and may be
     * <code>null</code>).
     * <p>
     * Note: This an {@link AtomicReference} for visibility in
     * {@link #toString()}, which does not obtain the {@link #lock}. The
     * {@link AtomicReference} also make changes in the downstream service
     * address visible inside of {@link ReadTask}.
     */
    private final AtomicReference<InetSocketAddress> addrNextRef;

    /**
     * Private buffer used to incrementally compute the checksum of the data as
     * it is received. The purpose of this buffer is to take advantage of more
     * efficient bulk copy operations from the NIO buffer into a local byte[] on
     * the Java heap against which we then track the evolving checksum of the
     * data.
     */
    private final byte[] heapBuffer = new byte[512];

    /*
     * Note: toString() implementation is non-blocking.
     */
    @Override
    public String toString() {

        return super.toString() + "{addrSelf=" + addrSelf + ", addrNext="
                + addrNextRef.get() + "}";

    }

    /** The Internet socket address at which this service will listen (immutable) */
    public InetSocketAddress getAddrSelf() {

        return addrSelf;
        
    }
    
    /**
     * The Internet socket address to which this service will relay messages
     * (dynamic and MAY be <code>null</code>).
     * 
     * @see #changeDownStream(InetSocketAddress)
     */
    public InetSocketAddress getAddrNext() {

        return addrNextRef.get();
        
    }
    
    /**
     * Create a new service instance - you MUST {@link Thread#start()} the
     * service.
     * <p>
     * Note: <i>addrNext</i> can be changed dynamically.
     * 
     * @param addrSelf
     *            The Internet socket address at which this service will listen.
     * @param addrNext
     *            The Internet socket address of a downstream service to which
     *            each data transfer will be relayed as it is received
     *            (optional).
     * 
     * @see #changeDownStream(InetSocketAddress)
     */
    public HAReceiveService(final InetSocketAddress addrSelf,
            final InetSocketAddress addrNext) {

        this(addrSelf, addrNext, null/* callback */);
        
    }

    /**
     * Create a new service instance - you MUST {@link Thread#start()} the
     * service.
     * 
     * @param addrSelf
     *            The Internet socket address at which this service will listen.
     * @param addrNext
     *            The Internet socket address of a downstream service to which
     *            each data transfer will be relayed as it is received
     *            (optional).
     * @param callback
     *            An object which will be notified as each payload arrives.
     */
    public HAReceiveService(final InetSocketAddress addrSelf,
            final InetSocketAddress addrNext,
            final IHAReceiveCallback<M> callback) {
        
        if (addrSelf == null)
            throw new IllegalArgumentException();

        this.addrSelf = addrSelf;

        this.addrNextRef = new AtomicReference<InetSocketAddress>(addrNext);
        
        this.callback = callback;

        // Note: Always allocate since the addrNext can change.
        this.sendService = new HASendService();

        // Thread will not prevent JVM exit.
        setDaemon(true);

        // Give the thread a useful name.
        setName(HAReceiveService.class.getName() + "@" + hashCode()
                + "{addrSelf=" + addrSelf + "}");

        if (log.isInfoEnabled())
            log.info("Created: " + this);

    }

    /**
     * Extended to {@link #terminate()} processing in order to ensure that
     * the service is eventually shutdown.
     */
    @Override
    protected void finalize() throws Throwable {
       
        terminate();
       
        super.finalize();
       
    }
   
    /**
     * Immediate shutdown.
     */
    public void terminate() {

        lock.lock();
        try {
            switch (runState) {
            case ShuttingDown: // already shutting down.
            case Shutdown: // already shutdown.
                return;
            default:
                runState = RunState.ShuttingDown;
                this.interrupt();
            }
        } finally {
            lock.unlock();
        }

        if (sendService != null)
            sendService.terminate();

        executor.shutdownNow();

    }
    
    /**
     * Block until the service is shutdown.
     */
    public void awaitShutdown() throws InterruptedException {
        /*
         * Wait until we observe that the service is no longer running while
         * holding the lock.
         * 
         * Note: When run() exits it MUST signalAll() on both [futureReady] and
         * [messageReady] so that all threads watching those Conditions notice
         * that the service is no longer running.
         */
        lock.lockInterruptibly();
        try {
            while (true) {
                switch (runState) {
                case Start:
                case Running:
                case ShuttingDown:
                    futureReady.await();
                    continue;
                case Shutdown:
                    // Exit terminate().
                    return;
                default:
                    throw new AssertionError();
                }
            }
        } finally {
            lock.unlock();
        }
    }
   
    public void start() {
        super.start();
        lock.lock();
        try {
            // Wait for state change from Start
            while (runState == RunState.Start) {
                try {
                    futureReady.await();
                } catch (InterruptedException e) {
                    // let's go around again
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void run() {
        lock.lock();
        try {
            // Change the run state and signal anyone who might be watching.
            runState = RunState.Running;
            futureReady.signalAll();
            messageReady.signalAll();
        } finally {
            lock.unlock();
        }
        ServerSocketChannel server = null;
        try {
            /*
             * Open a non-blocking server socket channel and start listening.
             */
            server = ServerSocketChannel.open();
            {
                /*
                 * Robustly attempt to bind the address and port where this
                 * service will listen.
                 * 
                 * Note: The retry is here because the port is not freed up
                 * immediately when we close the existing socket connection
                 */
                boolean didBind = false;
                for (int i = 0; i < 3; i++) {
                    try {
                        server.socket().bind(addrSelf);
                        didBind = true;
                        break;
                    } catch (BindException ex) {
                        log.warn("Sleeping to retry: " + ex);
                        Thread.sleep(100/* ms */);
                        continue;
                    }
                }
                if (!didBind) {
                    server.socket().bind(addrSelf);
                }
            }
            server.configureBlocking(false);
            if(log.isInfoEnabled())
                log.info("Listening on: " + addrSelf);
            runNoBlock(server);
        } catch (InterruptedException e) {
            /*
             * @todo what is the normal shutdown exception?
             */
            log.info("Shutdown");
        } catch (Throwable t) {
            log.error(t, t);
            throw new RuntimeException(t);
        } finally {
            if (server != null) {
                try {
                    server.close();
                } catch (IOException e) {
                    log.error(e, e);
                }
            }
            lock.lock();
            try {
                runState = RunState.Shutdown;
                messageReady.signalAll();
                futureReady.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * The client socket connection that was obtained when we accepted the
     * upstream reader.
     * <p>
     * Note: The {@link Client} connection is reused across {@link ReadTask}s.
     * <p>
     * Note: Exposed to {@link #changeUpStream()}.
     */
    private final AtomicReference<Client> clientRef = new AtomicReference<Client>(null);
    
    /**
     * Loops accepting requests and scheduling {@link ReadTask}s. Note that a
     * local caller must hand us a buffer and {@link IHAWriteMessageBase} using
     * {@link #receiveData(IHAWriteMessageBase, ByteBuffer)} before we will
     * accept data on the {@link SocketChannel}.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void runNoBlock(final ServerSocketChannel server) throws IOException,
            InterruptedException, ExecutionException {

        try {

            while (true) {

                // wait for the message to be set (actually, msg + buffer).
                lock.lockInterruptibly();
                try {
                    
                    // wait for the message.
                    while (message == null) {
                        switch (runState) {
                        case Running:
                            break;
                        case ShuttingDown:
                            // Service is terminating.
                            return;
                        case Start:
                        case Shutdown:
                        default:
                            throw new AssertionError(runState.toString());
                        }
                        messageReady.await();
                    }
                    
                    // Note the message.
                    final M msg = message; 
                    
                    // Message cleared.
                    message = null;

                    // Setup task to read buffer for that message.
                    readFuture = waitFuture = new FutureTask<Void>(
                            new ReadTask<M>(server, clientRef, msg,
                                    localBuffer, heapBuffer, sendService,
                                    addrNextRef, callback));

                    // [waitFuture] is available for receiveData().
                    futureReady.signalAll();

                } finally {

                    lock.unlock();
                    
                }

                /*
                 * The ReadTask now listens for the accept, ensuring that a
                 * future is available as soon as a message is present.
                 */
                try {
                    executor.execute(readFuture);
                } catch (RejectedExecutionException ex) {
                    readFuture.cancel(true/* mayInterruptIfRunning */);
                    log.error(ex);
                }

                /*
                 * Note: We might have to wait for the Future to avoid having
                 * more than one ReadTask at a time, but we should log and
                 * ignore any exception and restart the loop.
                 * 
                 * The loop needs to keep running. The thread that called
                 * receiveData() will return the [waitFuture] and will notice
                 * any exception through that Future.
                 */
                try {
                    readFuture.get();
                } catch (Exception e) {
                    log.error(e, e);
                }

                lock.lockInterruptibly();
                try {
                    readFuture = null;
                } finally {
                    lock.unlock();
                }

            } // while(true)

        } finally {
            final Client client = clientRef.get();
            if (client != null) {
                client.close();
            }
        }

    }

    /**
     * Class encapsulates the connection state for the socket channel used to
     * receive from on the upstream {@link HASendService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static private class Client {

        private final SocketChannel client;
        private final Selector clientSelector;
        private final SelectionKey clientKey;
        
        /**
         * When a pipeline change event is handled, we need to throw out an
         * exception rather than just cancelling the
         * {@link HAReceiveService#readFuture}. Cancelling the
         * {@link HAReceiveService#readFuture} causes a
         * {@link CancellationException} to be propoagated back to the remote
         * service that invoked
         * {@link IPipelineGlue#receiveAndReplicate(IHASyncRequest, IHAWriteMessage)}
         * . That {@link CancellationException} gets interpreted as a normal
         * termination in {@link QuorumPipelineImpl} and results in the
         * retrySend() logic NOT retrying and resending and thus breaks the
         * robustness of write pipeline replication.
         * <p>
         * Instead, the pipeline change events are used to set a
         * {@link Throwable} that is then thrown out of
         * {@link ReadTask#doReceiveAndReplicate(Client)} and thus appears as a
         * non-normal termination of the read future in the upstream service.
         * This allows retrySend() to do the right thing - namely it sends an
         * RMI message to the new downstream service and retransmits the payload
         * along the write pipeline.
         */
        private final AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>();

//        /** Used to replicate the message to the downstream service (if any). */
//        private final HASendService downstream;
        
        /**
         * Gets the client connection and open the channel in a non-blocking
         * mode so we will read whatever is available and loop until all data
         * has been read.
         */
        public Client(//
                final ServerSocketChannel server //
//                , final HASendService downstream //
//                , final InetSocketAddress addrNext//
        ) throws IOException {

            try {
                
                /*
                 * Note: This binds a port for a specific upstream HASendService
                 * that will be talking to this HAReceiveService.
                 */
                client = server.accept();
                client.configureBlocking(false);
                if (!client.finishConnect())
                    throw new IOException("Upstream client not connected");
                
                clientSelector = Selector.open();

                // must register OP_READ selector on the new client
                clientKey = client.register(clientSelector,
                        SelectionKey.OP_READ);
                
                if (log.isInfoEnabled())
                    log.info("Accepted new connection");
                
//                this.downstream = downstream;
//                
//                // Prepare downstream (if any) for incremental transfers
//                if (addrNext != null) {
//
//                    downstream.start(addrNext);
//                    
//                }

            } catch (IOException ex) {
                
                close();

                throw ex;
                
            }
            
        }

        @Override
        public String toString() {

            final Socket s = client.socket();
            
            return super.toString() //
                    + "{client.isOpen()=" + client.isOpen()//
                    + ",client.isConnected()=" + client.isConnected()//
                    + ",socket.isInputShutdown()="
                    + (s == null ? "N/A" : s.isInputShutdown())//
                    + ",clientSelector.isOpen=" + clientSelector.isOpen()//
                    + "}";

        }

        private void close() throws IOException {

            if (log.isInfoEnabled())
                log.info("Closing client connection: " + this);
            
            clientKey.cancel();
            
            try {

                client.close();
            
            } finally {
//                try {
                    clientSelector.close();
//                } finally {
//                    if (downstream != null) {
//                        downstream.terminate();
//                    }
//                }
            }
            
        }

        /**
         * Wraps {@link SocketChannel#read(ByteBuffer)} to test for an EOF and
         * calls {@link #close()} if an EOF is reached.
         * 
         * @param dst
         *            The destination buffer.
         * 
         * @return The #of bytes read.
         * 
         * @throws IOException
         */
        private int read(final ByteBuffer dst) throws IOException {

            final int rdlen = client.read(dst);

            if (rdlen == -1) {

                close();

            }

            return rdlen;

        }

        /**
         * Termination path used to signal a pipeline change through exception
         * control back to the leader. The leader will then handle this in
         * {@link QuorumPipelineImpl}'s retrySend() method.
         */
        private void checkFirstCause() throws RuntimeException {

            final Throwable t = firstCause.getAndSet(null);

            if (t != null) {
                try {
                    close();
                } catch (IOException ex) {
                    log.warn(ex, ex);
                }
                throw new RuntimeException(t);

            }

        }

    }

    /**
     * Read task is called with a {@link ServerSocketChannel}, a message
     * describing the data to be received, and a buffer into which the data will
     * be copied. It waits for the client connection and then copies the data
     * into the buffer, computing the checksum as it does, and optionally
     * transfer the data onto the downstream {@link SocketChannel}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     *          
     * @todo report counters
     *       <p>
     *       report the #of chunks per payload so we can decide if the private
     *       byte[] for computing the checksum is a good size.
     *       <p>
     *       report the #of payloads.
     */
    static private class ReadTask<M extends HAMessageWrapper> implements
            Callable<Void> {

        private final ServerSocketChannel server;

        private final AtomicReference<Client> clientRef;

        private final M message;

        private final ByteBuffer localBuffer;

        /**
         * Used to transfer received data to the downstream service (if any).
         */
        private final HASendService sendService;

        /**
         * The address of the downstream service -or- <code>null</code> iff
         * there is no downstream service.
         */
        private final AtomicReference<InetSocketAddress> addrNextRef;
        
        /**
         * Optional callback.
         */
        private final IHAReceiveCallback<M> callback;
        
        private final Adler32 chk = new Adler32();

        private final byte[] heapBuffer;
        
        /**
         * 
         * @param server
         * @param clientRef
         *            The client socket, selector, etc.
         * @param message
         *            The message carrying metadata about the data to be
         *            received (especially its byte length and its
         *            {@link Adler32} checksum).
         * @param localBuffer
         *            The buffer into which the data will be transferred.
         * @param downstream
         *            The {@link HASendService} used to relay data to the
         *            downstream node.
         * @param addrNext
         *            An {@link AtomicReference} for address of the downstream
         *            node. The value within that {@link AtomicReference} may be
         *            updated by
         *            {@link HAReceiveService#changeDownStream(InetSocketAddress)}
         *            . That value will be <code>null</code> if this is the last
         *            node in the write pipeline at the time the value is
         *            observed.
         * @param callback
         *            An optional callback.
         */
        public ReadTask(final ServerSocketChannel server,
                final AtomicReference<Client> clientRef, final M message,
                final ByteBuffer localBuffer, final byte[] heapBuffer,
                final HASendService downstream,
                final AtomicReference<InetSocketAddress> addrNextRef,
                final IHAReceiveCallback<M> callback) {

            if (server == null)
                throw new IllegalArgumentException();
            if (clientRef == null)
                throw new IllegalArgumentException();
            if (message == null)
                throw new IllegalArgumentException();
            if (heapBuffer == null)
                throw new IllegalArgumentException();
            if (localBuffer == null)
                throw new IllegalArgumentException();
            if (downstream == null)
                throw new IllegalArgumentException();
            
            this.server = server;
            this.clientRef = clientRef;
            this.message = message;
            this.localBuffer = localBuffer;
            this.heapBuffer = heapBuffer;
            this.sendService = downstream;
            this.addrNextRef = addrNextRef;
            this.callback = callback;
        }

        /**
         * Blocking wait for a client connection.
         * 
         * @throws IOException
         *             if something goes wrong.
         */
        protected void awaitAccept() throws IOException {

            // blocking wait for a client connection.
            final Selector serverSelector = Selector.open();
            try {

                final SelectionKey serverKey = server.register(serverSelector,
                        SelectionKey.OP_ACCEPT);

                try {

                    serverSelector.select(); // blocks

                    final Set<SelectionKey> keys = serverSelector
                            .selectedKeys();

                    final Iterator<SelectionKey> iter = keys.iterator();
                    
                    while (iter.hasNext()) {

                        final SelectionKey key = (SelectionKey) iter.next();

                        iter.remove();

                        if (key != serverKey)
                            throw new AssertionError();

                        break;
                    }

                } finally {
                    serverKey.cancel();
                }

            } finally {
                serverSelector.close();
            }

        }

        /**
         * Update the running checksum. This uses the {@link #heapBuffer} to
         * amoritize the cost of the transfers for the incremental checksum
         * maintenance.
         * 
         * @param rdlen
         *            The #of bytes read in the last read from the socket into
         *            the {@link #localBuffer}.
         */
        private void updateChk(final int rdlen) {

            // isolate changes to (pos,limit).
            final ByteBuffer b = localBuffer.asReadOnlyBuffer();

            // current position (and limit of how much data we need to chksum).
            final int mark = b.position();

            // rewind to the first byte to be read.
            b.position(mark - rdlen);
            
            for (int pos = mark - rdlen; pos < mark; pos += heapBuffer.length) {

                // #of bytes to copy into the local byte[]. 
                final int len = Math.min(mark - pos, heapBuffer.length);

                // copy into Java heap byte[], advancing b.position().
                b.get(heapBuffer, 0/* off */, len);

                // update the running checksum.
                chk.update(heapBuffer, 0/* off */, len);

            }
            
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
                log.error(
                        "client="
                                + clientRef.get()
                                + ", msg="
                                + message
                                + ", marker="
                                + HASendState.decode(message.getHASendState()
                                        .getMarker()) + ", cause=" + t, t);

                if (t instanceof Exception)
                    throw (Exception) t;
                
                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;
                
                throw new RuntimeException(t);
                
            }
            
        }
        
        private Void doInnerCall() throws Exception {
        
//          awaitAccept();
//          
//          /*
//           * Get the client connection and open the channel in a non-blocking
//           * mode so we will read whatever is available and loop until all
//           * data has been read.
//           */
//          final SocketChannel client = server.accept();
//          client.configureBlocking(false);
//          
//          final Selector clientSelector = Selector.open();
//
//          // must register OP_READ selector on the new client
//          final SelectionKey clientKey = client.register(clientSelector,
//                  SelectionKey.OP_READ);

          Client client = clientRef.get();

//          if (client != null) {
//
//              /*
//               * Note: We need to know when the client connection is no longer
//               * valid. The code here does not appear to do the trick.
//               * changeUpStream() is handling this instead.
//               * 
//               * We need to decide whether the client is no longer valid
//               * (either because the upstream HASendService has changed (our
//               * predecessor in the pipeline might have died) or because it
//               * has closed is socket connection to this HAReceiveService).
//               * 
//               * Either way, we need to establish a client connection using
//               * awaitAccept().
//               */
//              if (!client.client.isConnected()) {
//                  log.warn("Closing old client connection.");
//                  clientRef.set(client = null);
//              }
//
//          }
          
            if (client == null || !client.client.isOpen()
                    || !client.clientSelector.isOpen()) {

                log.warn("Re-opening upstream client connection");
                
                final Client tmp = clientRef.getAndSet(null);
                if (tmp != null) {
                    // Close existing connection if not open.
                    tmp.close();
                }

              /*
               * Accept and the initialize a connection from the upstream
               * HASendService.
               */
              
              // Accept a client connection (blocks)
              awaitAccept();

              // New client connection.
              client = new Client(server);//, sendService, addrNext);

              // save off reference.
              clientRef.set(client);

            }

//            boolean success = false;
//            try {
                doReceiveAndReplicate(client);
//                success = true;
                // success.
                return null;
//            } finally {
//                try {
//                    if(success) {
//                        ack(client);
//                    } else {
//                        nack(client);
//                    }
//                } catch (IOException ex) {
//                    // log and ignore.
//                    log.error(ex, ex);
//                }
//            }
            
        } // call.
        
        private void doReceiveAndReplicate(final Client client)
                throws Exception {

//            /**
//             * The first cause if downstream replication fails. We make a note
//             * of this first cause, continue to drain the payload, and then
//             * rethrow the first cause once the payload has been fully drained.
//             * This is necessary to ensure that the socket channel does not have
//             * partial data remaining from an undrained payload.
//             * 
//             * @see <a
//             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/724"
//             *      > HA wire pulling and sure kill testing </a>
//             * 
//             *      Note: It appears that attempting to drain the
//             *      payload is risky since there are a variety of ways in which
//             *      the process might be terminated. It seems to be safer to
//             *      drain the socket channel until we reach a marker that gives
//             *      us confidence that we are at the payload for the message
//             *      that is being processed.
//             */
//            Throwable downstreamFirstCause = null;
            
            /*
             * We should now have parameters ready in the WriteMessage and can
             * begin transferring data from the stream to the writeCache.
             */
            final long begin = System.currentTimeMillis();
            long mark = begin;

            // #of bytes remaining (to be received).
            int rem = message.getSize();

            // End of stream flag.
            boolean EOS = false;
            
            // for debug retain number of low level reads
            int reads = 0;
            
            final DrainToMarkerUtil drainUtil = message.getHASendState() != null ? new DrainToMarkerUtil(
                    message.getHASendState().getMarker(), client) : null;

            while (rem > 0 && !EOS) {

                // block up to the timeout.
                final int nkeys = client.clientSelector
                        .select(selectorTimeout/* ms */);

                // Check for termination (first cause exception).
                client.checkFirstCause();
                
                if (nkeys == 0) {
                
                    /*
                     * Nothing available.
                     */
                    
                    // time since last mark.
                    final long now = System.currentTimeMillis();
                    final long elapsed = now - mark;
                    
                    if (elapsed > logTimeout) {
                        // Issue warning if we have been blocked for a while.
                        log.warn("Blocked: awaiting " + rem + " out of "
                                + message.getSize() + " bytes.");
                        mark = now;// reset mark.
                    }

                    if (!client.client.isOpen()
                            || !client.clientSelector.isOpen()) {
                        
                        /*
                         * The channel has been closed. The request must be
                         * failed. TODO Or set EOF:=true? 
                         * 
                         * Note: The [callback] is NOT notified. The service
                         * that issued the RMI request to this service to
                         * receive the payload over the HAReceivedService will
                         * see this exception thrown back across the RMI
                         * request.
                         * 
                         * @see HAReceiveService.receiveData().
                         */
                        
                        throw new AsynchronousCloseException();
                        
                    }
                    
                    // no keys. nothing to read.
                    continue;
                    
                }

                final Set<SelectionKey> keys = client.clientSelector
                        .selectedKeys();
                
                final Iterator<SelectionKey> iter = keys.iterator();
                
                while (iter.hasNext()) {
                
                    // Check for termination.
                    client.checkFirstCause();

                    iter.next();
                    iter.remove();

                    if (!drainUtil.findMarker()) {

                        // continue to drain until the marker.
                        continue;
                        
                    }

                    final int rdlen = client.read(localBuffer);

                    if (log.isTraceEnabled())
                        log.trace("Read " + rdlen + " bytes with "
                                + (rdlen > 0 ? rem - rdlen : rem)
                                + " bytes remaining.");

                    if (rdlen > 0) {
                        reads++;
                        updateChk(rdlen);
                    }

                    if (rdlen == -1) {
                        // The stream is closed?
                        EOS = true;
                        break;
                    }

                    rem -= rdlen;

                    if (callback != null) {
                        // notify of incremental read.
                        callback.incReceive(message, reads, rdlen, rem);
                    }

                    forwardReceivedBytes(client, rdlen);

                } // while(itr.hasNext())

            } // while( rem > 0 && !EOS )

            if (localBuffer.position() != message.getSize())
                throw new IOException("Receive length error: rem=" + rem
                        + ", EOS=" + EOS + ", localBuffer.pos="
                        + localBuffer.position() + ", message.size="
                        + message.getSize());

            // prepare for reading.
            localBuffer.flip();

			if (log.isTraceEnabled())
				log.trace("Prior check checksum: " + chk.getValue()
						+ " for position: " + localBuffer.position()
						+ ", limit: " + localBuffer.limit()
						+ ", number of reads: " + reads + ", buffer: "
						+ localBuffer);

			if (message.getChk() != (int) chk.getValue()) {
                throw new ChecksumError("msg=" + message.toString()
                        + ", actual=" + (int) chk.getValue());
            }

            // Check for termination.
            client.checkFirstCause();

            if (callback != null) {

                /*
                 * The message was received and (if there is a downstream
                 * service) successfully replicated to the downstream service.
                 * We now invoke the callback to given this service an
                 * opportunity to handle the message and the fully received
                 * payload.
                 */
                
                callback.callback(message, localBuffer);
                
            }

        } // call()

        /**
         * Forward the most recent transfer bytes downstream.
         * <p>
         * 
         * Note: [addrNext] is final. If the downstream address is changed, then
         * the {@link ReadTask} is interrupted using its {@link Future} and the
         * WriteCacheService on the leader will handle the error by
         * retransmitting the current cache block.
         * 
         * The rdlen is checked for non zero to avoid an
         * IllegalArgumentException.
         * 
         * Note: loop since addrNext might change asynchronously.
         * 
         * @throws ExecutionException
         * @throws InterruptedException
         * @throws ImmediateDownstreamReplicationException
         * 
         * @todo Since the downstream writes are against a blocking mode
         *       channel, the receiver on this node runs in sync with the
         *       receiver on the downstream node. In fact, those processes could
         *       be decoupled with a bit more effort and are only required to
         *       synchronize by the end of each received payload.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/724" >
         *      HA wire pulling and sure kill testing </a>
         */
        private void forwardReceivedBytes(final Client client, final int rdlen)
                throws InterruptedException, ExecutionException,
                ImmediateDownstreamReplicationException {

            while (true) {

                if (rdlen != 0 && addrNextRef.get() != null) {
                    if (log.isTraceEnabled())
                        log.trace("Incremental send of " + rdlen + " bytes");
                    final ByteBuffer out = localBuffer.asReadOnlyBuffer();
                    out.position(localBuffer.position() - rdlen);
                    out.limit(localBuffer.position());
                    synchronized (sendService) {
                        /*
                         * Note: Code block is synchronized on [downstream] to
                         * make the decision to start the HASendService that
                         * relays to [addrNext] atomic. The HASendService uses
                         * [synchronized] for its public methods so we can
                         * coordinate this lock with its synchronization API.
                         */
                        if (!sendService.isRunning()) {
                            /*
                             * Prepare send service for incremental transfers to
                             * the specified address.
                             */
                            // Check for termination.
                            client.checkFirstCause();
                            // Note: use then current addrNext!
                            sendService.start(addrNextRef.get());
                            continue;
                        }
                    }
                    // Check for termination.
                    client.checkFirstCause();
                    /*
                     * Send and await Future. If this is the first chunk of a
                     * payload and a marker exists, then send the marker as
                     * well.
                     */
                    sendService
                            .send(out,
                                    out.position() == 0
                                            && message.getHASendState() != null ? message
                                            .getHASendState().getMarker()
                                            : null).get();
                }
                break; // break out of the inner while loop.
            
            } // while(true)

        }
           
    } // class ReadTask

    /**
     * Helper class to drain bytes from the upstream socket until we encounter a
     * marker in the stream that immediately proceeds the desired payload.
     * 
     * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn
     *         Cutcher</a>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/724" > HA
     *      wire pulling and sure kill testing </a>
     */
    static private class DrainToMarkerUtil {

        final private byte[] marker;
        final private byte[] markerBuffer;
        final private ByteBuffer markerBB;
        final private Client client;

        private boolean foundMarkerInInitialPosition = true;
        private int markerIndex = 0;
        private int nreads = 0;
        private int nmarkerbytematches = 0;
        private long bytesRead = 0L;

        DrainToMarkerUtil(final byte[] marker, final Client client) {

            this.marker = marker;
            this.markerBuffer = marker == null ? null : new byte[marker.length];
            this.markerBB = marker == null ? null : ByteBuffer
                    .wrap(markerBuffer);
            this.client = client;

            if (log.isDebugEnabled())
                log.debug("Receive token: " + BytesUtil.toHexString(marker));

        }

        /**
         * Note that the logic for finding the token bytes depends on the first
         * byte in the token being unique!
         * <p>
         * We have to be a bit clever to be sure we do not read beyond the token
         * and therefore complicate the reading into the localBuffer.
         * <p>
         * This is optimized for the normal case where the marker is read as
         * from the next bytes from the stream. In the worst case scenario this
         * could read large amounts of data only a few bytes at a time, however
         * this is not in reality a significant overhead.
         */
        boolean findMarker() throws IOException {

            if (markerIndex == marker.length) {
                
                // Marker already found for this payload.
                return true;

            }
            
            if (log.isDebugEnabled())
                log.debug("Looking for token, " + BytesUtil.toHexString(marker)
                        + ", reads: " + nreads);

            while (markerIndex < marker.length) {

                final int remtok = marker.length - markerIndex;
                markerBB.limit(remtok);
                markerBB.position(0);

                final int rdLen = client.read(markerBB);
                if (rdLen == -1) {
                    throw new IOException("EOF: nreads=" + nreads
                            + ", bytesRead=" + bytesRead);
                }
                nreads++;
                bytesRead += rdLen;
                for (int i = 0; i < rdLen; i++) {
                    if (markerBuffer[i] != marker[markerIndex]) {
                        if (foundMarkerInInitialPosition) {
                            /*
                             * The marker was not found in the initial position
                             * in the stream. We are going to drain data until
                             * we can match the marker.
                             */
                            foundMarkerInInitialPosition = false;
                            log.error("Marker not found: skipping");
                        }
                        markerIndex = 0;
                        if (markerBuffer[i] == marker[markerIndex]) {
                            markerIndex++;
                        }
                    } else {
                        markerIndex++;
                        nmarkerbytematches++;
                    }
                }

                if (nreads % 10000 == 0) {
                    if (log.isDebugEnabled())
                        log.debug("...still looking: reads=" + nreads
                                + ", bytesRead=" + bytesRead);
                }

            }

            if (markerIndex != marker.length) { 
                /*
                 * Partial marker has been read, but we do not have enough data
                 * for a full match yet.
                 */
                if (log.isDebugEnabled())
                    log.debug("Not found token yet!");
                return false;
            } else {
                if (log.isDebugEnabled())
                    log.debug("Found token after " + nreads
                            + " token reads and " + nmarkerbytematches
                            + " byte matches");

                return true;
            }

        }

    }

    /**
     * Receive data into the caller's buffer as described by the caller's
     * message.
     * 
     * @param msg
     *            The metadata about the data to be transferred.
     * @param buffer
     *            The buffer in which this service will receive the data. The
     *            buffer MUST be large enough for the data to be received. The
     *            buffer SHOULD be a direct {@link ByteBuffer} in order to
     *            benefit from NIO efficiencies. This method will own the buffer
     *            until the returned {@link Future} is done.
     * 
     * @return A {@link Future} which you can await. The {@link Future} will
     *         become available when the data has been transferred into the
     *         buffer, at which point the position will be ZERO (0) and the
     *         limit will be the #of bytes received into the buffer. If the data
     *         transfer fails or is interrupted, the {@link Future} will report
     *         the exception.
     * 
     * @throws InterruptedException
     */
    public Future<Void> receiveData(final M msg, final ByteBuffer buffer)
            throws InterruptedException {

        if (msg == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        lock.lockInterruptibly();
        try {
        	    assert message == null;
        	    message = msg;
            localBuffer = buffer;// DO NOT duplicate()! (side-effects required)
            localBuffer.limit(message.getSize());
            localBuffer.position(0);
            messageReady.signalAll();

            if (log.isTraceEnabled())
                log.trace("Will accept data for message: msg=" + msg);

            while (waitFuture == null) {
                switch (runState) {
                case Start:
                case Running:
                    // fall through and await signal.
                    break;
                case ShuttingDown:
                case Shutdown:
                    throw new RuntimeException("Service closed.");
                default:
                    throw new AssertionError();
                }
                // await signal.
                futureReady.await();
            }
            assert waitFuture != null;
            return waitFuture;
//            return readFuture; // Note: readFuture observed as null (!)

        } finally {
            waitFuture = null;
            lock.unlock();
        }

    }

    /**
     * Hook to notice receive events.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * 
     * @param <M>
     */
    public interface IHAReceiveCallback<M extends IHAWriteMessageBase> {

        /**
         * Notify that some payload bytes have been incrementally received for
         * an {@link IHAMessage}. This is invoked each time some data has been
         * read from the upstream socket.
         * 
         * @param msg
         *            The message.
         * @param nreads
         *            The number of reads performed against the upstream socket
         *            for this message.
         * @param rdlen
         *            The number of bytes read from the socket in this read.
         * @param rem
         *            The number of bytes remaining before the payload has been
         *            fully read.
         * 
         * @throws Exception
         */
        void incReceive(M msg, int nreads, int rdlen, int rem) throws Exception;
        
        /**
         * Hook invoked once a buffer has been received.
         * 
         * @param msg
         *            The message.
         * @param data
         *            The buffer containing the data. The position() will be
         *            ZERO (0). The limit() will be the #of bytes available. The
         *            implementation MAY have side effects on the buffer state
         *            (position, limit, etc).
         * 
         * @throws Exception
         */
        void callback(M msg, ByteBuffer data) throws Exception;

    }

    /**
     * Change the address to which the payloads are being relayed. This
     * terminates the embedded {@link HASendService}. The {@link HASendService}
     * will be restarted with the new {@link InetSocketAddress} (if any) by the
     * {@link ReadTask}.
     * <p>
     * Note: The {@link ReadTask} will throw out an exception when if there was
     * a downstream target when the {@link IncSendTask} is interrupted. Since
     * the {@link ReadTask} lacks the context to issue the appropriate RMI to
     * the downstream task, the exception must be caught and handled by the
     * {@link WriteCacheService}. It can simply rediscover the new downstream
     * service and then re-submit both the RMI and the {@link WriteCache} block.
     * 
     * @param addrNext
     *            The new address -or- <code>null</code> if payloads should not
     *            be relayed at this time.
     */
    public void changeDownStream(final InetSocketAddress addrNext) {

        lock.lock();
        try {

            if (log.isInfoEnabled())
                log.info("addrNext(old)=" + this.addrNextRef.get()
                        + ", addrNext(new)=" + addrNext + ", readFuture="
                        + readFuture);

            final Client c = clientRef.get();

            if (c != null && readFuture != null) {

                /*
                 * Set firstCause. doReceiveAndReplicate() will notice this and
                 * throw the (wrapped) exception back to the caller. This allows
                 * retrySend() on the leader to differentiate between normal
                 * termination of a downstream service and a pipeline change
                 * event.
                 * 
                 * Note: We do this *instead* of interrupting the [readFuture].
                 * The cause will be thrown out after a timeout on the client
                 * Selector or the next time any bytes are received at that
                 * Selector.
                 * 
                 * Note: The code path that interrupted the [readFuture] would
                 * only do so if the [readFuture] was non-null. The same
                 * behavior is preserved here. This subtlty means that a
                 * pipeline change event that occurs *before* the next attempt
                 * to receive a payload will succeed while a change that occurs
                 * once we have started to read data will fail.
                 */

                c.firstCause.set(new PipelineDownstreamChange());
                
            }
   
//            if (readFuture != null) {
//
//                // Interrupt the current receive operation.
//                readFuture.cancel(true/* mayInterruptIfRunning */);
//
//            }

            synchronized (sendService) {

                if (sendService.isRunning()) {
                
                    // Terminate HASendService (iff running).
                    sendService.terminate();
                    
                }
                
                /*
                 * Save the new addr.
                 * 
                 * Note: We need to do this while holding the monitor for the
                 * [sendService] since the update must be visible if we restart
                 * the sendService.
                 */
                this.addrNextRef.set(addrNext);

            }

            /*
             * Note: Do not start the service here. It will be started by the
             * next ReadTask, which will have the then current value of addrNext.
             */
//            if (addrNext != null) {
//
//                // Start send service w/ a new connection.
//                downstream.start(addrNext);
//
//            }
            
        } finally {

            lock.unlock();
            
        }

    }

    /**
     * Method must be invoked when the upstream service is changed. The method
     * is responsible for interrupting the current {@link RunTask} (if any) and
     * closing the client socket connection that was used to receive data from
     * the upstream service. A new connection will be accepted by the next
     * {@link RunTask}.
     */
    public void changeUpStream() {

        lock.lock();
        try {

            if (log.isInfoEnabled())
                log.info("");

            final Client oldClient = clientRef.getAndSet(null);

            if (oldClient != null) {

                log.warn("Cleared Client reference.");

            }
            
            if (oldClient != null && readFuture != null) {

                /*
                 * Set firstCause. doReceiveAndReplicate() will notice this and
                 * throw the (wrapped) exception back to the caller. This allows
                 * retrySend() on the leader to differentiate between normal
                 * termination of a downstream service and a pipeline change
                 * event.
                 * 
                 * Note: We do this *instead* of interrupting the [readFuture].
                 * The cause will be thrown out after a timeout on the client
                 * Selector or the next time any bytes are received at that
                 * Selector.
                 * 
                 * Note: The code path that interrupted the [readFuture] would
                 * only do so if the [readFuture] was non-null. The same
                 * behavior is preserved here. This subtlty means that a
                 * pipeline change event that occurs *before* the next attempt
                 * to receive a payload will succeed while a change that occurs
                 * once we have started to read data will fail.
                 */

                oldClient.firstCause.set(new PipelineUpstreamChange());

            }

//            if (readFuture != null) {
//
//                // Interrupt the current receive operation.
//                readFuture.cancel(true/* mayInterruptIfRunning */);
//
//            }

            /*
             * Explicitly close the client socket channel.
             */
            {
            
//                final Client oldClient = clientRef.getAndSet(null);

                if (oldClient != null) {

                    if (log.isInfoEnabled())
                        log.info("Closing client connection");
                    
                    try {
                    
                        oldClient.client.close();
                        
                    } catch (IOException e) {
                        
                        log.warn(e, e);
                        
                    }

                }
                
            }
            
        } finally {

            lock.unlock();

        }

    }

}
