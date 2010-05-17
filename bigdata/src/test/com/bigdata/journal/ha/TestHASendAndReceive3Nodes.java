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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Random;

import com.bigdata.io.TestCase3;

/**
 * Test the raw socket protocol implemented by {@link HASendService} and
 * {@link HAReceiveService} against a pipeline of 3 nodes.
 * 
 * @author martyn Cutcher
 */
public class TestHASendAndReceive3Nodes extends TestCase3 {

	/**
	 * A random number generated - the seed is NOT fixed.
	 */
	protected final Random r = new Random();

	/**
	 * Returns random data that will fit in N bytes. N is chosen randomly in
	 * 1:256.
	 * 
	 * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code> of
	 *         random length and having random contents.
	 */
	public ByteBuffer getRandomData() {

		final int nbytes = r.nextInt(256) + 1;

		return getRandomData(nbytes);

	}

    /**
     * Returns random data that will fit in <i>nbytes</i>.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
     *         having random contents.
     */
    public ByteBuffer getRandomData(final int nbytes) {

        final byte[] bytes = new byte[nbytes];

        r.nextBytes(bytes);

        return ByteBuffer.wrap(bytes);

    }

    /**
     * Returns random data that will fit in <i>nbytes</i>.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
     *         having random contents.
     */
    public ByteBuffer getRandomData(final ByteBuffer b, final int nbytes) {

        final byte[] a = new byte[nbytes];

        r.nextBytes(a);
        
        b.limit(nbytes);
        b.position(0);
        b.put(a);
        
        b.flip();
        
        return b;

    }
    
    public TestHASendAndReceive3Nodes() {

    }
    
    public TestHASendAndReceive3Nodes(String name) {
        
        super(name);

    }
    
	private HASendService sendService;
	private HAReceiveService<HAWriteMessage> receiveService1;
	private HAReceiveService<HAWriteMessage> receiveService2;
    
	protected void setUp() throws Exception {

        final InetSocketAddress receiveAddr2 = new InetSocketAddress(getPort(0));

        receiveService2 = new HAReceiveService<HAWriteMessage>(receiveAddr2, null/* downstream */);
        receiveService2.start();

        final InetSocketAddress receiveAddr1 = new InetSocketAddress(getPort(0));
        
        receiveService1 = new HAReceiveService<HAWriteMessage>(receiveAddr1, receiveAddr2);
        receiveService1.start();

        sendService = new HASendService(receiveAddr1);

        if (log.isInfoEnabled()) {
            log.info("receiveService1: addr=" + receiveAddr1);
            log.info("receiveService2: addr=" + receiveAddr2);
        }

    }

    protected void tearDown() throws Exception {

        if (receiveService1 != null)
            receiveService1.terminate();

        if (receiveService2 != null)
            receiveService2.terminate();

        if (sendService != null)
            sendService.terminate();
	    
	}

    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    private static int getPort(int suggestedPort) throws IOException {
        
        ServerSocket openSocket;
        try {
            openSocket = new ServerSocket(suggestedPort);
        } catch (BindException ex) {
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        }

        final int port = openSocket.getLocalPort();
        
        openSocket.close();

        if (suggestedPort != 0 && port != suggestedPort) {

            log.warn("suggestedPort is busy: suggestedPort=" + suggestedPort
                    + ", using port=" + port + " instead");
            
        }

        return port;

    }

}
