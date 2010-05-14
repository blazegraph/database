/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.journal.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
	
	private Selector selector;
	private ServerSocketChannel server;
	private SelectionKey serverKey;
	
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	
	private Future<Void> readFuture;
	
	final Lock lock = new ReentrantLock();
	final Condition futureReady  = lock.newCondition(); 
	final Condition messageReady = lock.newCondition(); 
	private HAWriteMessage message;
	private ByteBuffer localBuffer;

    /**
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
            log.info("Created for " + addrSelf + ", addrNext" + addrNext);

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
			selector = Selector.open();
			server = ServerSocketChannel.open();
			server.socket().bind(addrSelf);
			if(log.isInfoEnabled())
			    log.info("Listening on" + addrSelf);
			server.configureBlocking(false);
			serverKey = server.register(selector, SelectionKey.OP_ACCEPT);
			runNoBlock();
		} catch (ClosedByInterruptException cie) {
			/*
			 * Normal shutdown.
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
	 * Loops accepting requests and scheduling readTasks
	 */
	private void runNoBlock() {
		while (true) {
			try {
				selector.select();

				final Set<SelectionKey> keys = selector.selectedKeys();
				final Iterator<SelectionKey> iter = keys.iterator();
				while (iter.hasNext()) {
					final SelectionKey key = (SelectionKey) iter.next();
					iter.remove();

					if (key == serverKey) {
						if (key.isAcceptable()) {
							final SocketChannel client = server.accept();
							if (log.isTraceEnabled())
								log.trace("Accepted connection");
							client.configureBlocking(false);
							SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
							clientKey.attach(Integer.valueOf(0));
							
							this.readFuture = executor.submit(new Callable<Void>() {

								public Void call() throws Exception {
									lock.lockInterruptibly();
									try {
										while (message == null) messageReady.await(); 
									} finally {
										lock.unlock();
									}
									
									// We should now have parametesr ready in the WriteMessage and 
									//	can begin transferring data from the stream to the writeCache
									
									// Do the transfer
									int rem = message.getSize();
									while (rem > 0) {
										selector.select();

										final Set<SelectionKey> keys = selector.selectedKeys();
										final Iterator<SelectionKey> iter = keys.iterator();
										while (iter.hasNext())  {
											final SelectionKey key = (SelectionKey) iter.next();
											if (key.isReadable()) {
												iter.remove();
												
												if (key.channel() != client)
													throw new IllegalStateException("Unexpected socket channel");

												int rdlen = client.read(localBuffer);
												log.trace("Read " + rdlen + " into buffer");
												rem -= rdlen;
											} else {
												throw new IllegalStateException("Unexpected Selection Key: " + key);
											}
										}
									}
									
									// success.
									return null;
								}});
							
							// We must signal the future availability separately because the RMI
							// message may be receieved before we have data on the input stream
							{
								lock.lockInterruptibly();
								try {
									futureReady.signal(); 
								} finally {
									lock.unlock();
								}								
							}
						}
						readFuture.get(); // wait synchronously for read!!
					} else {
						// we don't expect to see readable selectors here, the readTask should consume them, OTOH
						//	a read selector may indicate the socket has been closed by the client
						if (key.isReadable()) {
							if (log.isTraceEnabled()) log.trace("Maybe socket close " + key);
							
							SocketChannel client = (SocketChannel) key.channel();
							
							ByteBuffer tmp = ByteBuffer.allocate(512);
							int rdlen = client.read(tmp);
							if (rdlen == -1) {
								if (log.isTraceEnabled()) log.trace("Socket closed");
					            key.cancel();
					            client.close();					    
							} else {
								log.warn("Read " + rdlen + " bytes from unexpected read");
							}
						}


					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
		}
	}

    /**
     * 
     * @param msg
     *            The metadata about the data to be transferred.
     * @param buffer
     *            The buffer in which this service will receive the data. The
     *            buffer MUST be large enough for the data to be received. The
     *            buffer SHOULD be a direct {@link ByteBuffer} in order to
     *            benefit from NIO efficiencies.
     * @return
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

                while (readFuture == null)
                    futureReady.await();
            } finally {
                lock.unlock();
            }

        }

        return readFuture;
    }
}
