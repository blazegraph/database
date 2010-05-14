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
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.io.WriteCache;

/**
 * Receives data from an HASendService.
 * 
 * The non-blocking processing of the data cannot proceed until the message parameters and an output
 * buffer have been set.  So an accept results in a task to be run.  The Future from this task is
 * returned to the method called from the RMI control invocation, thus allowing that method to wait
 * for the completion of the data transfer.
 * 
 * @author Martyn Cutcher
 *
 */
public class HAReceiveService extends Thread {
	protected static final Logger log = Logger.getLogger(HAReceiveService.class);
	private InetAddress addr;
	private int port;
	private Selector selector;
	private ServerSocketChannel server;
	private SelectionKey serverKey;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private Future<Integer> readFuture; // future task ready to process data transfer
	final Lock lock = new ReentrantLock();
	final Condition futureReady  = lock.newCondition(); 
	final Condition messageReady = lock.newCondition(); 
	private WriteMessage message;

	public HAReceiveService(InetAddress addr, int port, IHAClient client, boolean messageDrive) {
		this.addr = addr;
		this.port = port;
		this.setDaemon(true);
		
		if (log.isInfoEnabled())
			log.info("Created for " + addr + ":" + port);
	}
	
	public void run() {
		try {
			selector = Selector.open();
			server = ServerSocketChannel.open();
			server.socket().bind(new InetSocketAddress(addr, port));
			if(log.isInfoEnabled())
			    log.info("Listening on" + addr + ":" + port);
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
							SocketChannel client = server.accept();
							if (log.isTraceEnabled())
								log.trace("Accepted connection");
							client.configureBlocking(false);
							SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
							clientKey.attach(Integer.valueOf(0));
							
							this.readFuture = executor.submit(new Callable<Integer>() {

								public Integer call() throws Exception {
									lock.lockInterruptibly();
									try {
										if (message == null) messageReady.await(); 
									} finally {
										lock.unlock();
									}
									
									// We should now have parametesr ready in the WriteMessage and 
									//	can begin transferring data from the stream to the writeCache
									
									// TODO: Do the transfer
									
									return -1; // returns length of data read
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
					} else {
						// we don't expect to see readable selectors here, the readTask should consume them

						log.warn("Unexpected selection " + key);


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
	 * @param sze
	 * @param chk
	 * @param cache
	 * @return
	 * @throws InterruptedException 
	 */
	public Future<Integer> receiveData(int sze, int chk, boolean prefixWrites, WriteCache cache) throws InterruptedException {
		{
			lock.lockInterruptibly();
			try {
				this.message = new WriteMessage(sze, chk, prefixWrites, cache);
				messageReady.signal();
				
				if (readFuture == null) futureReady.await(); 
			} finally {
				lock.unlock();
			}
			
		}
		
		return readFuture;
	}
	
	static class WriteMessage {

		int sze;
		int chk;
		boolean prefixWrites;
		WriteCache writeCache;

		public WriteMessage(int sze, int chk, boolean prefixWrites, WriteCache cache) {
			this.sze = sze;
			this.chk = chk;
			this.prefixWrites = prefixWrites;
			this.writeCache = writeCache;
		}
		
	}
}
