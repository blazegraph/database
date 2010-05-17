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
   
    final Lock lock = new ReentrantLock();
    final Condition futureReady  = lock.newCondition();
    final Condition messageReady = lock.newCondition();
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
     */
    public void terminate() {
       
        this.interrupt();
       
        executor.shutdownNow();
       
    }
   
    public void run() {
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
                    while (message == null)
                        messageReady.await();

                    /*
                     * @todo pass in serverSocket, message, and buffer and make
                     * this static.
                     */
                    waitFuture = new FutureTask<Void>(new ReadTask(server, message, downstream)); // , message, buffer));
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
                 * @todo We might have to wait for the Future to avoid having
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
     * @version $Id$
     * 
     * @todo make this static.
     * 
     * @todo compute checksum and verify.
     * 
     * @todo transfer data onto the downstream socket and unit tests.
     */
    private class ReadTask implements Callable<Void> {

 		private HAWriteMessage message;
		private ServerSocketChannel server;
		private HASendService downstream;

        public ReadTask(final ServerSocketChannel server, final HAWriteMessage message, final HASendService downstream) {

            this.server = server;
            this.message = message;
            this.downstream = downstream;

        }

        public Void call() throws Exception {

            // blocking wait for a client connection.
            final Selector serverSelector = Selector.open();
            final SelectionKey serverKey = server.register(serverSelector,
                     SelectionKey.OP_ACCEPT);
            
            serverSelector.select();

            {
                final Set<SelectionKey> keys = serverSelector.selectedKeys();
                final Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext()) {

                    final SelectionKey key = (SelectionKey) iter.next();

                    iter.remove();

                    if (key != serverKey)
                        throw new AssertionError();

                    break;
                }
            }

            // @todo client should use its own selector.
            
            // get the client connection.
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

                // Do the transfer
                int rem = message.getSize();
                while (rem > 0) {

                	clientSelector.select();
                    final Set<SelectionKey> keys = clientSelector.selectedKeys();
                    final Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext()) {
                        final SelectionKey key = (SelectionKey) iter.next();
                        if (key.isReadable()) {
                            assert key == clientKey;

                            iter.remove();

                            if (key.channel() != client)
                                throw new IllegalStateException(
                                        "Unexpected socket channel");

                            final int rdlen = client.read(localBuffer);
                            if (log.isTraceEnabled())
                                log.trace("Read " + rdlen + " into buffer");
                            rem -= rdlen;
                            
                            // now forward the most recent transfer bytes downstream
                            if (downstream != null) {
                            	ByteBuffer out = localBuffer.asReadOnlyBuffer();
                            	out.position(localBuffer.position() - rdlen);
                            	out.limit(localBuffer.position());
                            	downstream.send(out);
                            }
                        } else {
                            throw new IllegalStateException(
                                    "Unexpected Selection Key: " + key);
                        }
                    }
                }
                
                // success.
                return null;
            } finally {
                clientKey.cancel();
                client.close();
                clientSelector.close();
                serverKey.cancel();
                serverSelector.close();
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
     *            benefit from NIO efficiencies.
     * 
     * @return A future which you can await. The future will become available
     *         when the data has been transferred into the buffer. If the data
     *         transfer fails or is interrupted, the future will report the
     *         exception.
     * 
     * @throws InterruptedException
     */
    public Future<Void> receiveData(final HAWriteMessage msg,
            final ByteBuffer buffer) throws InterruptedException {
        {
            lock.lockInterruptibly();
            try {
                message = msg;
                localBuffer = buffer.duplicate();
                localBuffer.limit(message.getSize());
                localBuffer.position(0);
                messageReady.signal();

                if (log.isTraceEnabled())
                    log.trace("Will accept data for message: msg=" + msg);

                while (waitFuture == null)
                    futureReady.await();
                waitFuture = null;
                
            } finally {
                lock.unlock();
            }

        }

        return readFuture;
    }

}
