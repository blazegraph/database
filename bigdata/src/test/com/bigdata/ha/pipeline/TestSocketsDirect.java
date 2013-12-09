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

public class TestSocketsDirect extends TestCase3 {

    public TestSocketsDirect() {
    }

    public TestSocketsDirect(String name) {
        super(name);
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
     */
    public void testDirectSockets() throws IOException {

        final InetSocketAddress serverAddr = new InetSocketAddress(getPort(0));

        // First our ServerSocket
        final ServerSocket ss = new ServerSocket();
        ss.bind(serverAddr);
        
        assertTrue(ss.getChannel() == null);
        
        // Now the first Client SocketChannel
        final SocketChannel cs1 = SocketChannel.open();
        
        final boolean immediate1 = cs1.connect(serverAddr);
        assertTrue("Expected immediate local connection", immediate1);
        
        final Random r = new Random();
        final byte[] data = new byte[200];
        r.nextBytes(data);
        
        final ByteBuffer src = ByteBuffer.wrap(data);
        
        // Write some data
        cs1.write(src);
        
        final byte[] dst = new byte[200];

        // Accept the client connection (after connect and write)
        final Socket readSckt1 = accept(ss);
        
        InputStream instr = readSckt1.getInputStream();
        
        // and read the data
        instr.read(dst);
        
        // confirming the read is correct
        assertTrue(BytesUtil.bytesEqual(data, dst));
        
        assertTrue(ss.getChannel() == null);

        // now write some more data into the channel and then close it
        cs1.write(ByteBuffer.wrap(data));
        
        // close the client socket
        cs1.close();
        
        assertTrue(readSckt1.isConnected());
        assertFalse(readSckt1.isClosed());
                
        // Now try writing some more data
        try {
            cs1.write(ByteBuffer.wrap(data));
            fail("Expected closed channel exception");
        } catch (ClosedChannelException e) {
            // expected
        }
                
        // the old stream should be closed
        try {
            final int rdlen = instr.read(dst); // should be closed
            assertTrue(rdlen == 200);
            assertTrue(BytesUtil.bytesEqual(data, dst));
            
            assertTrue(instr.read(dst) == -1); // read EOF
        } catch (Exception e) {
        	fail("not expected");
        }
        
        // if so then should we explicitly close its socket?
        readSckt1.close();
        assertTrue(readSckt1.isClosed());
        
        assertFalse(ss.isClosed());
        assertTrue(ss.getChannel() == null);
        
        // Now open a new client Socket and connect to the server
        
        final SocketChannel cs2 = SocketChannel.open();       
        final boolean immediate2 = cs2.connect(serverAddr);
        
        assertTrue("Expected immediate local connection", immediate2);
        
        // Now we should be able to accept the new connection
        final Socket s2 = accept(ss);
        
        // ... write to the SocketChannel
        final int wlen = cs2.write(ByteBuffer.wrap(data));
        
        assertTrue(wlen == data.length);

        // failing to read from original stream
        final int nrlen = instr.read(dst);
        assertTrue(nrlen == -1);

        // but succeeding to read from the new Socket
        final InputStream instr2 = s2.getInputStream();
        instr2.read(dst);
        
        assertTrue(BytesUtil.bytesEqual(data, dst));     
        
        // Can a downstream close be detected upstream?
        instr2.close();
        
        assertTrue(cs2.isOpen()); // Not after closing input stream
        
        s2.close();
       
        assertTrue(cs2.isOpen()); // Nor after closing the socket
        
        // now write some more to the socket
        final int wlen2 = cs2.write(ByteBuffer.wrap(data));
        assertTrue(wlen2 == data.length);
        
        // having closed the input, without a new connect request
        // we should not be able to accept the new write
        try {
        	final Socket s3 = accept(ss);
        	fail("Expected timeout failure");
        } catch (AssertionFailedError afe) {
        	// expected
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
    
        // wrap the ServerSocket accept with a timeout check
    Socket accept(final ServerSocket ss) {
        final AtomicReference<Socket> av = new AtomicReference<Socket>();
        assertNoTimeout(1, TimeUnit.SECONDS, new Callable<Void>() {

            @Override
            public Void call() throws Exception {
            	
                av.set(ss.accept());
                
                return null;
            }});
        
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

    private void assertNoTimeout(long timeout, TimeUnit unit, Callable<Void> callable) {
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
