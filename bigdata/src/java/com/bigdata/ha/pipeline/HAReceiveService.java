/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA  */

package com.bigdata.ha.pipeline;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
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

import com.bigdata.ha.pipeline.HASendService.IncSendTask;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.io.writecache.WriteCacheService;
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
public class HAReceiveService<M extends HAWriteMessageBase> extends Thread {

    protected static final Logger log = Logger
            .getLogger(HAReceiveService.class);

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
    private final HASendService downstream;
   
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
   
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
    private final Lock lock = new ReentrantLock();
    private final Condition futureReady  = lock.newCondition();
    private final Condition messageReady = lock.newCondition();
    private RunState runState = RunState.Start;
    private HAWriteMessageBase message;
    private ByteBuffer localBuffer;
    private FutureTask<Void> readFuture;
    private FutureTask<Void> waitFuture;

    /**
     * The Internet socket address of a downstream service to which each data
     * transfer will be relayed as it is received (optional and may be
     * <code>null</code>).
     * <p>
     * Note: This is volatile for visibility in {@link #toString()}, which does
     * not obtain the {@link #lock}.
     */
    private volatile InetSocketAddress addrNext;

    public String toString() {

        return super.toString() + "{addrSelf=" + addrSelf + ", addrNext="
                + addrNext + "}";

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

        this.addrNext = addrNext;
        
        this.callback = callback;

        // Note: Always allocate since the addrNext can change.
        this.downstream = new HASendService();

        setDaemon(true);

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

        if (downstream != null)
            downstream.terminate();

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
            server.socket().bind(addrSelf);
            server.configureBlocking(false);
            if(log.isInfoEnabled())
                log.info("Listening on" + addrSelf);
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
     * Loops accepting requests and scheduling readTasks. Note that a local
     * caller must hand us a buffer and {@link HAWriteMessageBase} using
     * {@link #receiveData(HAWriteMessageBase, ByteBuffer)} before we will accept
     * data on the {@link SocketChannel}.
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void runNoBlock(final ServerSocketChannel server) throws IOException,
            InterruptedException, ExecutionException {

        final AtomicReference<Client> clientRef = new AtomicReference<Client>();
        
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
                    
                    // setup task.
                    waitFuture = new FutureTask<Void>(new ReadTask(server, clientRef,
                            message, localBuffer, downstream, addrNext, callback));
                    readFuture = waitFuture;
                    message = null;
                    
                    futureReady.signal();

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
                 */
                try {
                	readFuture.get();
                } catch (Exception e) {
                	log.warn(e);
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
     * read on the upstream client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static private class Client {

        final SocketChannel client;
        final Selector clientSelector;
        final SelectionKey clientKey;

        final HASendService downstream;
        
        /**
         * Gets the client connection and open the channel in a non-blocking
         * mode so we will read whatever is available and loop until all data
         * has been read.
         */
        public Client(final ServerSocketChannel server,
                final HASendService downstream, final InetSocketAddress addrNext)
                throws IOException {

            try {
                
                client = server.accept();
                client.configureBlocking(false);

                clientSelector = Selector.open();

                // must register OP_READ selector on the new client
                clientKey = client.register(clientSelector,
                        SelectionKey.OP_READ);

                this.downstream = downstream;
                
                // Prepare downstream (if any) for incremental transfers
                if (addrNext != null) {

                    downstream.start(addrNext);
                    
                }

            } catch (IOException ex) {
                
                close();

                throw ex;
                
            }
            
        }

        public void close() throws IOException {
            clientKey.cancel();
            try {
                client.close();
            } finally {
                try {
                    clientSelector.close();
                } finally {
                    if (downstream != null) {
                        downstream.terminate();
                    }
                }
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
     * @version $Id: HAReceiveService.java 2826 2010-05-17 11:46:23Z
     *          martyncutcher $
     * 
     * @todo report counters
     *       <p>
     *       report the #of chunks per payload so we can decide if the private
     *       byte[] for computing the checksum is a good size.
     *       <p>
     *       report the #of payloads.
     */
    static private class ReadTask<M extends HAWriteMessageBase> implements
            Callable<Void> {

        private final ServerSocketChannel server;

        private final AtomicReference<Client> clientRef;

        private final M message;

        private final ByteBuffer localBuffer;

        /**
         * Used to transfer received data to the downstream service (if any).
         */
        private final HASendService downstream;

        /**
         * The address of the downstream service -or- <code>null</code> iff
         * there is no downstream service.
         */
        private final InetSocketAddress addrNext;
        
        /**
         * Optional callback.
         */
        private final IHAReceiveCallback<M> callback;
        
        private final Adler32 chk = new Adler32();

        /**
         * Private buffer used to incrementally compute the checksum of the data
         * as it is received. The purpose of this buffer is to take advantage of
         * more efficient bulk copy operations from the NIO buffer into a local
         * byte[] on the Java heap against which we then track the evolving
         * checksum of the data.
         */
        private final byte[] a = new byte[512];

        /**
         * 
         * @param client
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
         *            The address of the downstream node (optional and
         *            <code>null</code> if this is the last node in the relay
         *            chain).
         * @param callback
         *            An optional callback.
         */
        public ReadTask(final ServerSocketChannel server,
                final AtomicReference<Client> clientRef, final M message,
                final ByteBuffer localBuffer, final HASendService downstream,
                final InetSocketAddress addrNext,
                final IHAReceiveCallback<M> callback) {

            if (server == null)
                throw new IllegalArgumentException();
            if (clientRef == null)
                throw new IllegalArgumentException();
            if (message == null)
                throw new IllegalArgumentException();
            if (localBuffer == null)
                throw new IllegalArgumentException();
            if (downstream == null)
                throw new IllegalArgumentException();
            
            this.server = server;
            this.clientRef = clientRef;
            this.message = message;
            this.localBuffer = localBuffer;
            this.downstream = downstream;
            this.addrNext = addrNext;
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
         * Update the running checksum.
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
            
            for (int pos = mark - rdlen; pos < mark; pos += a.length) {

                // #of bytes to copy into the local byte[]. 
                final int len = Math.min(mark - pos, a.length);

                // copy into Java heap byte[], advancing b.position().
                b.get(a, 0/* off */, len);

                // update the running checksum.
                chk.update(a, 0/* off */, len);

            }
            
        }
        
        public Void call() throws Exception {

//            awaitAccept();
//            
//            /*
//             * Get the client connection and open the channel in a non-blocking
//             * mode so we will read whatever is available and loop until all
//             * data has been read.
//             */
//            final SocketChannel client = server.accept();
//            client.configureBlocking(false);
//            
//            final Selector clientSelector = Selector.open();
//
//            // must register OP_READ selector on the new client
//            final SelectionKey clientKey = client.register(clientSelector,
//                    SelectionKey.OP_READ);

            Client client = clientRef.get();
            if (client == null) {

                // Accept a client connection (blocks)
                awaitAccept();

                // New client connection.
                client = new Client(server, downstream, addrNext);

                // save off reference.
                clientRef.set(client);

            }

            /*
             * We should now have parameters ready in the WriteMessage and can
             * begin transferring data from the stream to the writeCache.
             */
            int rem = message.getSize();
            while (rem > 0) {

                client.clientSelector.select();
                final Set<SelectionKey> keys = client.clientSelector
                        .selectedKeys();
                final Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext()) {
                    iter.next();
                    iter.remove();

                    final int rdlen = client.client.read(localBuffer);
                    if (log.isTraceEnabled())
                        log.trace("Read " + rdlen + " bytes");

                    if (rdlen > 0)
                        updateChk(rdlen);

                    if (rdlen == -1)
                        break;

                    rem -= rdlen;

                    /*
                     * Now forward the most recent transfer bytes downstream
                     * 
                     * @todo Since the downstream writes are against a blocking
                     * mode channel, the receiver on this node runs in sync with
                     * the receiver on the downstream node. In fact, those
                     * processes could be decoupled with a bit more effort and
                     * are only required to synchronize by the end of each
                     * received payload.
                     * 
                     * Note: [addrNext] is final. If the downstream address is
                     * changed, then the ReadTask is interrupted using its
                     * Future and the WriteCacheService will handle the error by
                     * retransmitting the current cache block.
                     */
                    if (addrNext != null) {
                        if (log.isTraceEnabled())
                            log
                                    .trace("Incremental send of " + rdlen
                                            + " bytes");
                        final ByteBuffer out = localBuffer.asReadOnlyBuffer();
                        out.position(localBuffer.position() - rdlen);
                        out.limit(localBuffer.position());
                        // Send and await Future.
                        downstream.send(out).get();
                    }
                }

            } // while( rem > 0 )

            assert localBuffer.position() == message.getSize() : "localBuffer.pos="
                    + localBuffer.position()
                    + ", message.size="
                    + message.getSize();

            // prepare for reading.
            localBuffer.flip();

            if (message.getChk() != (int) chk.getValue()) {
                throw new ChecksumError("msg=" + message.toString()
                        + ", actual=" + chk.getValue());
            }

            if (callback != null) {
                callback.callback(message, localBuffer);
            }

            // success.
            return null;

        } // call()
            
    } // class ReadTask

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
     * @return A future which you can await. The future will become available
     *         when the data has been transferred into the buffer, at which
     *         point the position will be ZERO (0) and the limit will be the #of
     *         bytes received into the buffer. If the data transfer fails or is
     *         interrupted, the future will report the exception.
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
            message = msg;
            localBuffer = buffer;// DO NOT duplicate()! (side-effects required)
            localBuffer.limit(message.getSize());
            localBuffer.position(0);
            messageReady.signal();

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
     * @version $Id$
     * @param <M>
     */
    public interface IHAReceiveCallback<M extends HAWriteMessageBase> {

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
     * terminates the embedded {@link HASendService} and then
     * {@link HASendService#start(InetSocketAddress)}s it with the new address
     * (if any).
     * <p>
     * The {@link ReadTask} will throw out an exception when if there was a
     * downstream target when the {@link IncSendTask} is interrupted. Since the
     * {@link ReadTask} lacks the context to issue the appropriate RMI to the
     * downstream task, the exception must be caught hand handled by the
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

            if (readFuture != null) {

                // Interrupt the current receive operation.
                readFuture.cancel(true/* mayInterruptIfRunning */);

            }

            // Terminate existing HASendService (if any).
            downstream.terminate();

            // Save the new addr.
            this.addrNext = addrNext;

            /*
             * Note: Do not start the service here. It will be started by the
             * next ReadTask, which will have the new value of addrNext.
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
    
}

