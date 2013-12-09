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
package com.bigdata.ha.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.AssertionFailedError;

import com.bigdata.btree.BytesUtil;
import com.bigdata.io.TestCase3;

/**
 * Test suite for basic socket behaviors.
 * 
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn Cutcher</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestSocketsDirect extends TestCase3 {

    public TestSocketsDirect() {
    }

    public TestSocketsDirect(String name) {
        super(name);
    }

    /**
     * Simple test of connecting to a server socket and the failure to connect
     * to a port not associated with a server socket.
     * 
     * @throws IOException
     */
    public void testDirectSockets_exceptionIfPortNotOpen() throws IOException {

        // Get two socket addressses. We will open a service on one and try to
        // connect to the unused one on the other port.
        final InetSocketAddress serverAddr1 = new InetSocketAddress(getPort(0));
        final InetSocketAddress serverAddr2 = new InetSocketAddress(getPort(0));

        // First our ServerSocket
        final ServerSocket ss1 = new ServerSocket();
        try {

            ss1.bind(serverAddr1);

            assertTrue(ss1.getChannel() == null);

            // Now the first Client SocketChannel
            final SocketChannel cs1 = SocketChannel.open();
            try {
                /*
                 * Note: true if connection made. false if connection in
                 * progress. 
                 */
                final boolean immediate1 = cs1.connect(serverAddr1);
                if (!immediate1) {
                    // Did not connect immediately, so finish connect now.
                    if (!cs1.finishConnect()) {
                        fail("Did not connect.");
                    }
                }
            } finally {
                cs1.close();
            }

            // Now the first Client SocketChannel
            final SocketChannel cs2 = SocketChannel.open();
            try {
                cs1.connect(serverAddr2);
                fail("Expecting " + IOException.class);
            } catch (IOException ex) {
                if(log.isInfoEnabled())
                    log.info("Ignoring expected exception: "+ex);
            } finally {
                cs2.close();
            }

        } finally {

            ss1.close();
            
        }
        
    }
    
    /**
     * The use of threaded tasks in the send/receive service makes it difficult to
     * observer the socket state changes.
     * 
     * So let's begin by writing some tests over the raw sockets.
     * 
     * Note that connecting and then immediately closing the socket is perfectly okay.
     * ...with an accept followed by a read() of -1 on the returned Socket stream.
     * 
     * @throws IOException 
     * @throws InterruptedException 
     */
    public void testDirectSockets() throws IOException, InterruptedException {

        // The payload size that we will use.
        final int DATA_LEN = 200;

        final Random r = new Random();
        final byte[] data = new byte[DATA_LEN];
        r.nextBytes(data);
        final byte[] dst = new byte[DATA_LEN];
        
        // The server side receive buffer size (once we open the server socket).
        int receiveBufferSize = -1;
        
        final InetSocketAddress serverAddr = new InetSocketAddress(getPort(0));

        // First our ServerSocket
        final ServerSocket ss = new ServerSocket();
        try {

            assertTrue(ss.getChannel() == null);

            // bind the server socket to the port.
            ss.bind(serverAddr);

            assertTrue(ss.getChannel() == null);
            
            // figure out the receive buffer size on the server socket.
            receiveBufferSize = ss.getReceiveBufferSize();

            if (log.isInfoEnabled())
                log.info("receiveBufferSize=" + receiveBufferSize
                        + ", payloadSize=" + DATA_LEN);
            
            if (receiveBufferSize < DATA_LEN) {

                fail("Service socket receive buffer is smaller than test payload size: receiveBufferSize="
                        + receiveBufferSize + ", payloadSize=" + DATA_LEN);

            }
            
            /*
             * InputStream for server side of socket connection - set below and
             * then reused outside of the try/finally block.
             */
            InputStream instr = null;

            // Now the first Client SocketChannel
            final SocketChannel cs1 = SocketChannel.open();
            try {

                /*
                 * Note: true if connection made. false if connection in
                 * progress.
                 */
                final boolean immediate1 = cs1.connect(serverAddr);
                if (!immediate1) {
                    if (!cs1.finishConnect()) {
                        fail("Did not connect?");
                    }
                }

                assertTrue(ss.getChannel() == null);

                /*
                 * We are connected.
                 */

                final ByteBuffer src = ByteBuffer.wrap(data);

                // Write some data on the client socket.
                cs1.write(src);

                /*
                 * Accept client's connection on server (after connect and
                 * write).
                 */
                final Socket readSckt1 = accept(ss);

                // Stream to read the data from the socket on the server side.
                instr = readSckt1.getInputStream();

                // and read the data
                instr.read(dst);

                // confirming the read is correct
                assertTrue(BytesUtil.bytesEqual(data, dst));

                assertTrue(ss.getChannel() == null);

                /*
                 * Attempting to read more returns ZERO because there is nothing
                 * in the buffer and the connection is still open on the client
                 * side.
                 * 
                 * Note: instr.read(buf) will BLOCK until the data is available,
                 * the EOF is detected, or an exception is thrown.
                 */
                assertEquals(0,instr.available());
//                assertEquals(0, instr.read(dst));

                /*
                 * Now write some more data into the channel and *then* close
                 * it.
                 */
                cs1.write(ByteBuffer.wrap(data));

                // close the client side of the socket
                cs1.close();

                // The server side of client connection is still open.
                assertTrue(readSckt1.isConnected());
                assertFalse(readSckt1.isClosed());

                /*
                 * Now try writing some more data. This should be disallowed
                 * since we closed the client side of the socket.
                 */
                try {
                    cs1.write(ByteBuffer.wrap(data));
                    fail("Expected closed channel exception");
                } catch (ClosedChannelException e) {
                    // expected
                }

                /*
                 * Since we closed the client side of the socket, when we try to
                 * read more data on the server side of the connection. The data
                 * that we already buffered is still available on the server
                 * side of the socket.
                 */
                {
                    // the already buffered data should be available.
                    final int rdlen = instr.read(dst);
                    assertEquals(DATA_LEN, rdlen);
                    assertTrue(BytesUtil.bytesEqual(data, dst));
                }

                /*
                 * We have drained the buffered data. There is no more buffered
                 * data and client side is closed, so an attempt to read more
                 * data on the server side of the socket will return EOF (-1).
                 */
                assertEquals(-1, instr.read(dst)); // read EOF

                // if so then should we explicitly close its socket?
                readSckt1.close();
                assertTrue(readSckt1.isClosed());

                assertFalse(ss.isClosed());
                assertTrue(ss.getChannel() == null);

            } finally {
                cs1.close();
            }

            /*
             * Now open a new client Socket and connect to the server.
             */
            final SocketChannel cs2 = SocketChannel.open();
            try {

                // connect to the server socket again.
                final boolean immediate2 = cs2.connect(serverAddr);
                if (!immediate2) {
                    if (!cs2.finishConnect()) {
                        fail("Did not connect?");
                    }
                }

                // Now server should accept the new client connection
                final Socket s2 = accept(ss);

                // Client writes to the SocketChannel
                final int wlen = cs2.write(ByteBuffer.wrap(data));
                assertEquals(DATA_LEN, wlen); // verify data written.

                // failing to read from original stream
                final int nrlen = instr.read(dst);
                assertEquals(-1, nrlen);

                // but succeeding to read from the new Socket
                final InputStream instr2 = s2.getInputStream();
                instr2.read(dst);
                assertTrue(BytesUtil.bytesEqual(data, dst));

                /*
                 * Question: Can a downstream close be detected upstream?
                 * 
                 * Answer: No. Closing the server socket does not tell the
                 * client that the socket was closed.
                 */
                {
                    // close server side input stream.
                    instr2.close();

                    // but the client still thinks its connected.
                    assertTrue(cs2.isOpen()); 
                    
                    // Does the client believe it is still open after a brief
                    // sleep?
                    Thread.sleep(1000);
                    assertTrue(cs2.isOpen()); // yes.

                    // close server stocket.
                    s2.close();

                    // client still thinks it is connected after closing server
                    // socket.
                    assertTrue(cs2.isOpen()); 

                    // Does the client believe it is still open after a brief
                    // sleep?
                    Thread.sleep(1000);
                    assertTrue(cs2.isOpen()); // yes.

                }

                /*
                 * Now write some more to the socket. We have closed the
                 * accepted connection on the server socket. Our observations
                 * show that the 1st write succeeds. The second write then fails
                 * with 'IOException: "Broken pipe"'
                 * 
                 * The server socket is large (256k). We are not filling it up,
                 * but the 2nd write always fails. Further, the client never
                 * believes that the connection is closed until the 2nd write,
                 */
                {
                    final int writeSize = 1;
                    int nwritesOk = 0;
                    long nbytesReceived = 0L;
                    while (true) {
                        try {
                            // write a payload.
                            final int wlen2 = cs2.write(ByteBuffer.wrap(data,
                                    0, writeSize));
                            // if write succeeds, should have written all bytes.
                            assertEquals(writeSize, wlen2);
                            nwritesOk++;
                            nbytesReceived += wlen2;
                            // does the client think the connection is still open?
                            assertTrue(cs2.isOpen()); // yes.
                            Thread.sleep(1000);
                            assertTrue(cs2.isOpen()); // yes.
                        } catch (IOException ex) {
                            if (log.isInfoEnabled())
                                log.info("Expected exception: nwritesOk="
                                        + nwritesOk + ", nbytesReceived="
                                        + nbytesReceived + ", ex=" + ex);
                            break;
                        }
                    }
                }

                /*
                 * Having closed the input, without a new connect request we
                 * should not be able to accept the new write.
                 */
                try {
                    final Socket s3 = accept(ss);
                    fail("Expected timeout failure");
                } catch (AssertionFailedError afe) {
                    // expected
                }

            } finally {
                cs2.close();
            }

        } finally {
            ss.close();
        }
       
    }
    
    /**
     * Confirms that multiple clients can communicate with same Server
     * 
     * @throws IOException
     */
    public void testMultipleClients() throws IOException {
        final InetSocketAddress serverAddr = new InetSocketAddress(getPort(0));

        final ServerSocket ss = new ServerSocket();
        ss.bind(serverAddr);
        
        assertTrue(ss.getChannel() == null);
        
        final int nclients = 10;
        
        final ArrayList<SocketChannel> clients = new ArrayList<SocketChannel>();
        final ArrayList<Socket> sockets = new ArrayList<Socket>();
        
        final Random r = new Random();
        final byte[] data = new byte[200];
        r.nextBytes(data);
        assertNoTimeout(10, TimeUnit.SECONDS, new Callable<Void>() {

			@Override
			public Void call() throws Exception {
		        for (int c = 0; c < nclients; c++) {
		            final SocketChannel cs = SocketChannel.open();           
		            cs.connect(serverAddr);
		            
		            clients.add(cs);
		            sockets.add(ss.accept());
		            
		            // write to each SocketChannel (after connect/accept)
		            cs.write(ByteBuffer.wrap(data));
		        }
				return null;
			}
        	
        });     
        
        // Now read from all Sockets accepted on the server
        final byte[] dst = new byte[200];
        for (Socket s : sockets) {
        	assertFalse(s.isClosed());
        	
            final InputStream instr = s.getInputStream();
            
            assertFalse(-1 == instr.read(dst)); // doesn't return -1
            
            assertTrue(BytesUtil.bytesEqual(data, dst));
            
            // Close each Socket to ensure it is different
            s.close();
            
        	assertTrue(s.isClosed());
        }

    }
    
    /** wrap the ServerSocket accept with a timeout check. */
    private Socket accept(final ServerSocket ss) {

        final AtomicReference<Socket> av = new AtomicReference<Socket>();

        assertNoTimeout(1, TimeUnit.SECONDS, new Callable<Void>() {

            @Override
            public Void call() throws Exception {

                av.set(ss.accept());

                return null;
            }
        });

        return av.get();
    }

    private void assertTimeout(long timeout, TimeUnit unit, Callable<Void> callable) {
        final ExecutorService es = Executors.newSingleThreadExecutor();
        final Future<Void> ret = es.submit(callable);
        try {
            ret.get(timeout, unit);
            fail("Expected timeout");
        } catch (TimeoutException e) {
            // that is expected
            return;
        } catch (Exception e) {
            fail("Expected timeout");
        } finally {
        	log.warn("Cancelling task - should interrupt accept()");
        	ret.cancel(true);
            es.shutdown();
        }
    }

    /**
     * Throws {@link AssertionFailedError} if the {@link Callable} does not
     * succeed within the timeout.
     * 
     * @param timeout
     * @param unit
     * @param callable
     * 
     * @throws AssertionFailedError
     *             if the {@link Callable} does not succeed within the timeout.
     * @throws AssertionFailedError
     *             if the {@link Callable} fails.
     */
    private void assertNoTimeout(final long timeout, final TimeUnit unit,
            final Callable<Void> callable) {
        final ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            final Future<Void> ret = es.submit(callable);
            ret.get(timeout, unit);
        } catch (TimeoutException e) {
            fail("Unexpected timeout");
        } catch (Exception e) {
            fail("Unexpected Exception", e);
        } finally {
            es.shutdown();
        }
    }

}
