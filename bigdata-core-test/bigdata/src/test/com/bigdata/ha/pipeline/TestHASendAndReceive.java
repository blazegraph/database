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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.msg.HAMessageWrapper;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;

/**
 * Test the raw socket protocol implemented by {@link HASendService} and
 * {@link HAReceiveService}.
 * 
 * @author martyn Cutcher
 */
public class TestHASendAndReceive extends AbstractHASendAndReceiveTestCase {

    public TestHASendAndReceive() {

    }
    
    public TestHASendAndReceive(String name) {
        
        super(name);

    }
    
	private HASendService sendService;
	private HAReceiveService<HAMessageWrapper> receiveService;
	
	@Override
	protected void setUp() throws Exception {

	    super.setUp();
	    r = new Random();
	    
        /*
         * Note: ZERO (0) indicates that a random free port will be selected. If
         * you use a fixed port then there is a danger that the port will not be
         * able to be reopened immediately after it has been closed, in which
         * case you will see something like: "bind address already in use".
         */
	    final int port = getPort(0);// 3000
	    
        if (log.isInfoEnabled())
            log.info("Using port=" + port);

	    final InetSocketAddress addr = new InetSocketAddress(port);
		
		receiveService = new HAReceiveService<HAMessageWrapper>(addr, null);
		receiveService.start();

        sendService = new HASendService();
        sendService.start(addr);

    }

	@Override
    protected void tearDown() throws Exception {

	    super.tearDown();
	    
        if (receiveService != null) {
            receiveService.terminate();
            receiveService = null;
        }

        if (sendService != null) {
//            sendService.closeIncSend();
            sendService.terminate();
            sendService = null;
        }
	 
	}

    /**
     * Should we expect concurrency of the Socket send and RMI? It seems that we
     * should be able to handle it whatever the logical argument. The only
     * constraint should be on the processing of each pair of socket/RMI
     * interactions. OTOH, if we are intending to process the OP_ACCEPT and
     * OP_READ within the ReadTask that can only be processed AFTER the RMI is
     * received, then we should not sen the socket until we have a returned
     * FutureTask.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException 
     * @throws ImmediateDownstreamReplicationException 
     */
    public void testSimpleExchange() throws InterruptedException, ExecutionException, TimeoutException, ImmediateDownstreamReplicationException {

        final long timeout = 5000;// ms
        {
            final ByteBuffer tst1 = getRandomData(50);
            final HAMessageWrapper msg1 = newHAWriteMessage(50, tst1);
            final ByteBuffer rcv = ByteBuffer.allocate(2000);
            final Future<Void> futRec = receiveService.receiveData(msg1, rcv);
            final Future<Void> futSnd = sendService.send(tst1, msg1.getMarker());
            futSnd.get(timeout,TimeUnit.MILLISECONDS);
            futRec.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst1, rcv);
        }

        {
            final ByteBuffer tst2 = getRandomData(100);
            final HAMessageWrapper msg2 = newHAWriteMessage(100, tst2);
            final ByteBuffer rcv2 = ByteBuffer.allocate(2000);
            final Future<Void> futSnd = sendService.send(tst2, msg2.getMarker());
            final Future<Void> futRec = receiveService.receiveData(msg2, rcv2);
            futSnd.get(timeout,TimeUnit.MILLISECONDS);
            futRec.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst2, rcv2);
        }

    }

    /**
     * Sends a large number of random buffers, confirming successful
     * transmission.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws ImmediateDownstreamReplicationException 
     */
    public void testStress() throws TimeoutException, InterruptedException,
            ExecutionException, ImmediateDownstreamReplicationException {

        final long timeout = 5000; // ms
        for (int i = 0; i < 100; i++) {
            final int sze = 10000 + r.nextInt(300000);
            final ByteBuffer tst = getRandomData(sze);
            final HAMessageWrapper msg = newHAWriteMessage(sze,  tst);
            final ByteBuffer rcv = ByteBuffer.allocate(sze + r.nextInt(1024));
            // FutureTask return ensures remote ready for Socket data
            final Future<Void> futRec = receiveService.receiveData(msg, rcv);
            final Future<Void> futSnd = sendService.send(tst, msg.getMarker());
            futSnd.get(timeout,TimeUnit.MILLISECONDS);
            futRec.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst, rcv); // make sure buffer has been transmitted
        }
    }

    /**
     * Sends a large number of random buffers, confirming successful
     * transmission.
     * 
     * @throws InterruptedException
     */
    public void testStressDirectBuffers() throws InterruptedException {

        final long timeout = 5000; // ms
        IBufferAccess tstdb = null, rcvdb = null;
        int i = -1, sze = -1;
        try {
        	tstdb = DirectBufferPool.INSTANCE.acquire();
        	rcvdb = DirectBufferPool.INSTANCE.acquire();
        	final ByteBuffer tst = tstdb.buffer();
        	final ByteBuffer rcv = rcvdb.buffer();
            for (i = 0; i < 1000; i++) {
                sze = 1 + r.nextInt(tst.capacity());
                getRandomData(tst, sze);
                final HAMessageWrapper msg = newHAWriteMessage(sze, tst);
                assertEquals(0,tst.position());
                assertEquals(sze,tst.limit());
                // FutureTask return ensures remote ready for Socket data
                final Future<Void> futRec = receiveService.receiveData(msg, rcv);
                final Future<Void> futSnd = sendService.send(tst, msg.getMarker());
                futSnd.get(timeout,TimeUnit.MILLISECONDS);
                futRec.get(timeout,TimeUnit.MILLISECONDS);
                // make sure buffer has been transmitted
                assertEquals(tst, rcv);
                if (log.isInfoEnabled() && (i<10 || i % 10 == 0))
                    log.info("Looks good for #" + i);
           }
        } catch (Throwable t) {
            throw new RuntimeException("i=" + i + ", sze=" + sze + " : " + t, t);
        } finally {
            try {
                if (tstdb != null) {
                    tstdb.release();
                }
            } finally {
                if (rcvdb != null) {
                    rcvdb.release();
                }
            }
        }
    }
}
