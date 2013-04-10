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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.bigdata.ha.msg.HAWriteMessageBase;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteMessageBase;
import com.bigdata.ha.pipeline.HAReceiveService.IHAReceiveCallback;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.TestCase3;
import com.bigdata.util.ChecksumUtility;

/**
 * Test the raw socket protocol implemented by {@link HASendService} and
 * {@link HAReceiveService}. This test suite looks at the interrupt handling
 * behavior. Interrupts during an IO cause the {@link SocketChannel} to be
 * closed. If the service is still open, then we need to transparently re-open
 * the socket channel.
 * <p>
 * Interrupts wind up closing the {@link HASendService} side of the channel when
 * a remote service makes a request to the quorum leader to send some data along
 * the write pipeline, and then cancels that operation. For example, when a
 * service needs to resynchronize with the quorum it will request either HALogs
 * or a snapshot from the leader. Those data are sent over the write pipeline.
 * There may be concurrent requests and data transfers in progress. Those
 * requests will be serialized (one raw buffer of at a time). The
 * {@link IHAWriteMessage}s for different requests (and their payloads) can be
 * interleaved, but each {@link IHAWriteMessage} and payload is transmitted
 * atomically (the packets sent along the socket pertain to only a single
 * payload / {@link IHAWriteMessage}). When the service has caught up, if it
 * enters an error state, or if it shuts down, then the service will cancel the
 * remote future on the leader for the request (e.g., a "sendHALog()" request).
 * This can cause an interrupt of a blocked {@link SocketChannel} IO and that
 * will cause the {@link SocketChannel} to be closed asynchronously.
 * <p>
 * Both the {@link HASendService} and the {@link HAReceiveService} need to
 * notice the asynchronous {@link SocketChannel} close and cure it (unless they
 * are terminated or terminating). The {@link HAReceiveService} needs to
 * propagate the {@link AsynchronousCloseException} through the
 * {@link IHAReceiveCallback} and drop the paylaod. The {@link HASendService}
 * needs to drop the payload and re-open the {@link SocketChannel}. The next
 * {@link IHAWriteMessage} and payload should be traversed normally once the
 * {@link SocketChannel} has been re-opened.
 * 
 * @author martyn Cutcher
 */
public class TestHASendAndReceiveInterrupts extends TestCase3 {

    public TestHASendAndReceiveInterrupts() {

    }
    
    public TestHASendAndReceiveInterrupts(String name) {
        
        super(name);

    }
    
    private class MyCallback implements IHAReceiveCallback<IHAWriteMessageBase> {

        @Override
        public void callback(IHAWriteMessageBase msg, ByteBuffer data)
                throws Exception {
            // TODO Auto-generated method stub
            
        }
        
    }
    
	private HASendService sendService;
	private HAReceiveService<IHAWriteMessageBase> receiveService;
	private ChecksumUtility chk;
	private IHAReceiveCallback<IHAWriteMessageBase> callback;

	@Override
	protected void setUp() throws Exception {

	    super.setUp();
	    
	    chk = new ChecksumUtility();
	    
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

	    callback = new MyCallback();
	    
        receiveService = new HAReceiveService<IHAWriteMessageBase>(addr,
                null/* nextAddr */, callback);
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
	 
        chk = null;
        
	}

    public void testRecoverFromInterrupts() throws InterruptedException,
            ExecutionException, TimeoutException {

        // replace receiveService with one with callback
        final InetSocketAddress addrSelf = receiveService.getAddrSelf();
        final InetSocketAddress addrNext = null;

        receiveService.terminate();

        // Callback will check fail value to determine if task should be
        // interrupted
        final AtomicReference<Future<Void>> receiveFail = new AtomicReference<Future<Void>>(
                null);

        receiveService = new HAReceiveService<IHAWriteMessageBase>(addrSelf,
                addrNext, new IHAReceiveCallback<IHAWriteMessageBase>() {
                    public void callback(final IHAWriteMessageBase msg,
                            final ByteBuffer data) throws Exception {
                        final Future<Void> task = receiveFail.get();
                        if (task != null) {
                            task.cancel(true);
                        }
                    }
                });

        receiveService.start();

        /*
         * Note: Must be larger than the socket buffer / packet size or the 2nd
         * message will get buffered before we can interrupt it.
         * 
         * FIXME The problem here is that we are queuing the 2nd message
         * immediately on the HASendService's Executor. The actual IncSendTask
         * does not wait once it has finished deliverying data to the socket
         * channel. It immediately exits, and the data transfer for the next
         * queued request begins. In order to avoid messing up the payload for
         * the next request, we might need to wait on the Future of the remote
         * ReceiveService.receiveData() task before terminating the IncSendTask
         * (or before scheduling the next one). Otherwise we may not be in a
         * position where we can figure out whether or not to restart a given
         * payload. If the Send Future (of the IncSendTask) was cancelled, then
         * we want to drop the payload associated with that specific Future.
         */
        final int msgSize = 256 + r.nextInt(1024);
        final int receiveBufSize = msgSize + r.nextInt(128);

        final long timeout = 5000;// ms
        {
            // 1st xfer.
            final ByteBuffer tst1 = getRandomData(msgSize);
            final IHAWriteMessageBase msg1 = new HAWriteMessageBase(msgSize,
                    chk.checksum(tst1));
            final ByteBuffer rcv1 = ByteBuffer.allocate(receiveBufSize);
            final Future<Void> futSnd1 = sendService.send(tst1);
            
            // 2nd xfer.
            final ByteBuffer tst2 = getRandomData(msgSize);
            final IHAWriteMessageBase msg2 = new HAWriteMessageBase(msgSize,
                    chk.checksum(tst2));
            final ByteBuffer rcv2 = ByteBuffer.allocate(receiveBufSize);
            final Future<Void> futSnd2 = sendService.send(tst2);

            // We will interrupt the 2nd send.
            receiveFail.set(futSnd2);

            // 3rd xfer.
            final ByteBuffer tst3 = getRandomData(msgSize);
            final IHAWriteMessageBase msg3 = new HAWriteMessageBase(msgSize,
                    chk.checksum(tst3));
            final ByteBuffer rcv3 = ByteBuffer.allocate(receiveBufSize);
            final Future<Void> futSnd3 = sendService.send(tst3);

            final Future<Void> futRec1 = receiveService.receiveData(msg1, rcv1);

            futSnd2.get(); // should throw exception UNLESS IO done before
            final Future<Void> futRec2 = receiveService.receiveData(msg2, rcv2);

            final Future<Void> futRec3 = receiveService.receiveData(msg3, rcv2);

            // first send. should be good.
            futSnd1.get(timeout,TimeUnit.MILLISECONDS);
            futRec1.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst1, rcv1);

            // seecond 2nd. should be cancelled.
            futSnd2.get(timeout,TimeUnit.MILLISECONDS);
            futRec2.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst1, rcv2);

            // 3rd send. should be good. TODO Longer sequences w/ variable message sizes to hit various timings.
            futSnd3.get(timeout,TimeUnit.MILLISECONDS);
            futRec3.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst3, rcv3);

        }

    }
    
    public void testStressRecoverFromInterrupts() throws Exception {
        tearDown();
        for (int i = 0; i < 100; i++) {
            setUp();
            try {
                testRecoverFromInterrupts();
            } catch (Throwable t) {
                fail("Fail on pass " + i + " : " + t, t);
            } finally {
                tearDown();
            }
        }
    }

}
