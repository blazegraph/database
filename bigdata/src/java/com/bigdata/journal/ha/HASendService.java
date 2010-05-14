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
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;

/**
 * This class has a private single-threaded Executor to which it submits a SendTask (implements Callable<Void>).  
 * The SendTask will open a blocking-mode socket connection to the specified service and send the bytes remaining 
 * in the buffer to that service.  The data will be sent on the socket using WritableByteChannel#write(buffer).  
 * Since this is a blocking-mode connection, the call should block until all bytes have been sent or the current 
 * thread is interrupted, e.g., by cancelling its Future.  Regardless, verify that all bytes were sent as a 
 * post-condition (position() == limit).  If there is any problem, then the socket is closed and the original 
 * exception is thrown out of call().  If the socket is closed from the other end while we are still writing data, 
 * then that exception gets thrown out as well.  The socket connection is closed as a post-condition (it does 
 * not survive across multiple sends).  Closing the socket connection after each success or unsuccessful send 
 * gives us a strong indication of success or failure for the data transfer which is independent of the RMI 
 * message and makes it trivial to re-synchronize the HASendService since it is basically stateless.
 * 
 * See HAReceiveService for downstream process
 * 
 * @author Martyn Cutcher
 *
 */
public class HASendService {
	
    protected static final Logger log = Logger.getLogger(HASendService.class);

    private InetSocketAddress inetSocketAddress;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	
	public HASendService(final InetSocketAddress inetSocketAddress) {
		this.inetSocketAddress = inetSocketAddress;
	}

	SocketChannel openChannel() throws IOException {
		SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);

        if (log.isTraceEnabled())
        	log.trace("Connecting to " + inetSocketAddress);
        
        socketChannel.connect(inetSocketAddress);
        socketChannel.finishConnect();
        return socketChannel;
	}
	
	public Future<Void> send(final ByteBuffer data) {
		return executor.submit(new Callable<Void>() {

			public Void call() throws Exception {
				SocketChannel socketChannel = openChannel();
				try {
					socketChannel.write(data);
					
					// check all data written
					if (data.position() != data.limit()) {
						throw new RuntimeException("Not all data has been sent");
					}
				} finally {
					socketChannel.close();
				}
				
				return null;
			}});
	}
}
