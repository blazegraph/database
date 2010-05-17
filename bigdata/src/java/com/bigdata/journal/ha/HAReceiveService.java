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

package com.bigdata.journal.ha;

import java.io.EOFException;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.util.ChecksumUtility;

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
public class HAReceiveService<M extends HAWriteMessage> extends Thread {

    protected static final Logger log = Logger
            .getLogger(HAReceiveService.class);

    /** The Internet socket address at which this service will listen. */
    private final InetSocketAddress addrSelf;

    /**
     * The Internet socket address of a downstream service to which each data
     * transfer will be relayed as it is received (optional and may be
     * <code>null</code>).
     *
     * FIXME Implement the relay semantics.
     */
    private final InetSocketAddress addrNext;
   
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
   
    private ServerSocketChannel server;
    private FutureTask<Void> readFuture;
    private FutureTask<Void> waitFuture;

    /**
     * Service run state enumeration.

     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static enum RunState {

        Start(0), Running(1), ShuttingDown(2), Shutdown(3);
        
        private RunState(final int level) {
        
            this.level = level;
            
        }

        private final int level;
    }

    /*
     * The lock and the things which it guards.
     */
    private final Lock lock = new ReentrantLock();
    private final Condition futureReady  = lock.newCondition();
    private final Condition messageReady = lock.newCondition();
    private RunState runState = RunState.Start;
    private HAWriteMessage message;
    private ByteBuffer localBuffer;

	private HASendService downstream;

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

        if (addrSelf == null)
         throw new IllegalArgumentException();

        this.addrSelf = addrSelf;

        this.addrNext = addrNext;

        if (log.isInfoEnabled())
            log
                    .info("Created: addrSelf=" + addrSelf + ", addrNext="
                            + addrNext);

        if (addrNext != null) {
        	this.downstream = new HASendService(addrNext);
        }
        setDaemon(true);
       
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
     * @throws InterruptedException 
     */
    public void terminate() throws InterruptedException {
       
        lock.lockInterruptibly();
        try {
            runState = RunState.ShuttingDown;
            this.interrupt();
        } finally {
            lock.unlock();
        }

        if (downstream != null)
            downstream.terminate();
        
        executor.shutdownNow();

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
        try {
            /*
             * Open a non-blocking server socket channel and start listening.
             */
            server = ServerSocketChannel.open();
            server.socket().bind(addrSelf);
            server.configureBlocking(false);
            if(log.isInfoEnabled())
                log.info("Listening on" + addrSelf);
            /*
             * Accept requests.
             */
            runNoBlock();
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
     * caller must hand us a buffer and {@link HAWriteMessage} using
     * {@link #receiveData(HAWriteMessage, ByteBuffer)} before we will accept
     * data on the {@link SocketChannel}.
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void runNoBlock() throws IOException, InterruptedException,
            ExecutionException {

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
                    waitFuture = new FutureTask<Void>(new ReadTask(server,
                            message, localBuffer, downstream));
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
        	// Now stateless since serverSocket is passed to ReadTask
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
     * @todo compute checksum and verify.
     */
    static private class ReadTask implements Callable<Void> {

        private final ServerSocketChannel server;

        private final HAWriteMessage message;

        private final ByteBuffer localBuffer;

        private final HASendService downstream;

		private ChecksumUtility chk = new ChecksumUtility();

        public ReadTask(final ServerSocketChannel server,
                final HAWriteMessage message, final ByteBuffer localBuffer,
                final HASendService downstream) {

            this.server = server;
            this.message = message;
            this.localBuffer = localBuffer;
            this.downstream = downstream;
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
        
        public Void call() throws Exception {

            awaitAccept();
            
            /*
             * Get the client connection and open the channel in a non-blocking
             * mode so we will read whatever is available and loop until all
             * data has been read.
             */
            final SocketChannel client = server.accept();
            client.configureBlocking(false);
            
            final Selector clientSelector = Selector.open();

            // must register OP_READ selector on the new client
            final SelectionKey clientKey = client.register(clientSelector,
                    SelectionKey.OP_READ);

            try {
                /*
                 * We should now have parameters ready in the WriteMessage and
                 * can begin transferring data from the stream to the
                 * writeCache.
                 */

                // Do the transfer, prepare downstream (if any) for incremental transfers
                if (downstream != null) {
                	downstream.initiateIncSend();
                }
                int rem = message.getSize();
                while (rem > 0) {

                    clientSelector.select();
                    final Set<SelectionKey> keys = clientSelector.selectedKeys();
                    final Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext()) {
                        iter.next();
                        iter.remove();

                        final int rdlen = client.read(localBuffer);
                        if (log.isTraceEnabled())
                            log.trace("Read " + rdlen + " bytes");
                        
                        if (rdlen == -1) 
                        	break;
                        
                        rem -= rdlen;

                        /*
                         * Now forward the most recent transfer bytes downstream
                         * 
                         * @todo inline open of downstream socket channel since
                         * we may have to transfer smaller chunks.
                         * 
                         * @todo Since the downstream writes are against a
                         * blocking mode channel, the receiver on this node runs
                         * in sync with the receiver on the downstream node. In
                         * fact, those processes could be decoupled with a bit
                         * more effort.
                         */
                        if (downstream != null) {
                        	if (log.isTraceEnabled())
                                log.trace("Incremental send of " + rdlen + " bytes");
                        	ByteBuffer out = localBuffer.asReadOnlyBuffer();
                            out.position(localBuffer.position() - rdlen);
                            out.limit(localBuffer.position());
                            downstream.incSend(out);
                        }
                    }

                } // while( rem > 0 )
                
                // wait for downstream send
                if (downstream != null) {
                	downstream.closeIncSend();
                }
                
                // prepare for reading.
                assert localBuffer.position() == message.getSize() : "localBuffer.pos="+localBuffer.position()+", message.size="+message.getSize();
                localBuffer.flip();
                
                // TODO extent utility to compute incrementally to take advantage of wait time for transfers
                if (chk.checksum(localBuffer) != message.getChk()) {
                	throw new RuntimeException("Checksum Error");
                }
                // success.
                return null;

            } finally {
                clientKey.cancel();
                try {
                    client.close();
                } finally {
                    clientSelector.close();
                }
            } // finally {}
            
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
    public Future<Void> receiveData(final HAWriteMessage msg,
            final ByteBuffer buffer) throws InterruptedException {

        lock.lockInterruptibly();
        try {
            message = msg;
            localBuffer = buffer;//buffer.duplicate();
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
            return readFuture;

        } finally {
            waitFuture = null;
            lock.unlock();
        }

    }

}
