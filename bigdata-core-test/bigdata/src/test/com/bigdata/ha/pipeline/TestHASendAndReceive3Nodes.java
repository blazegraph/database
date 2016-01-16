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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.BigdataStatics;
import com.bigdata.ha.msg.HAMessageWrapper;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.util.Bytes;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.InnerCause;

/**
 * Test the raw socket protocol implemented by {@link HASendService} and
 * {@link HAReceiveService} against a pipeline of 3 nodes.
 * 
 * @author martyn Cutcher
 */
public class TestHASendAndReceive3Nodes extends
        AbstractHASendAndReceiveTestCase {

	public TestHASendAndReceive3Nodes() {

	}

	public TestHASendAndReceive3Nodes(String name) {

		super(name);

	}

	/** The leader. */
	private HASendService sendServiceA;
	
	/** The first follower. */
	private HAReceiveService<HAMessageWrapper> receiveServiceB;

	/** The second follower (and the end of the pipeline). */
	private HAReceiveService<HAMessageWrapper> receiveServiceC;

    /**
     * {@inheritDoc}
     * <p>
     * Sets up an HA3 pipeline [A,B,C].
     */
    @Override
	protected void setUp() throws Exception {

        super.setUp();

        /*
         * Setup C at the end of the pipeline.
         */
        {
            
            final InetSocketAddress receiveAddrC = new InetSocketAddress(
                    getPort(0));

            receiveServiceC = new HAReceiveService<HAMessageWrapper>(
                    receiveAddrC, null/* downstream */);
            
            receiveServiceC.start();
            
        }

        /*
         * Setup B. B is in the middle of the pipeline. It will receive from A
         * and replicate to C.
         */
		{
		    
		    final InetSocketAddress receiveAddrB = new InetSocketAddress(getPort(0));
            
		    receiveServiceB = new HAReceiveService<HAMessageWrapper>(
                    receiveAddrB, receiveServiceC.getAddrSelf());
            
		    receiveServiceB.start();
		    
		}

        /*
         * Setup A. A is the leader. It will send messages to B, which will then
         * replicate them to C.
         */
        {

            sendServiceA = new HASendService();
            
            sendServiceA.start(receiveServiceB.getAddrSelf());
            
        }

		if (log.isInfoEnabled()) {
            
		    log.info("sendService: addrNext=" + sendServiceA.getAddrNext());
            
		    log.info("receiveService1: addrSelf="
                    + receiveServiceB.getAddrSelf() + ", addrNext="
                    + receiveServiceB.getAddrNext());
            
            log.info("receiveService2: addrSelf="
                    + receiveServiceC.getAddrSelf() + ", addrNext="
                    + receiveServiceC.getAddrNext());
        }

	}

	@Override
	protected void tearDown() throws Exception {

	    super.tearDown();
	    
		if (receiveServiceB != null) {
			receiveServiceB.terminate();
			receiveServiceB = null;
		}

		if (receiveServiceC != null) {
			receiveServiceC.terminate();
			receiveServiceC = null;
		}

		if (sendServiceA != null) {
//            sendService.closeIncSend();
            sendServiceA.terminate();
            sendServiceA = null;
		}
		
	}

    public void testSimpleExchange() throws InterruptedException,
            ExecutionException, TimeoutException, ImmediateDownstreamReplicationException {

        final long timeout = 5000; // ms
		final ByteBuffer tst1 = getRandomData(50);
		final HAMessageWrapper msg1 = newHAWriteMessage(50, tst1);
		final ByteBuffer rcv1 = ByteBuffer.allocate(2000);
		final ByteBuffer rcv2 = ByteBuffer.allocate(2000);
		// rcv.limit(50);
		final Future<Void> futRec1 = receiveServiceB.receiveData(msg1, rcv1);
		final Future<Void> futRec2 = receiveServiceC.receiveData(msg1, rcv2);
		final Future<Void> futSnd = sendServiceA.send(tst1, msg1.getMarker());
		futSnd.get(timeout,TimeUnit.MILLISECONDS);
		futRec1.get(timeout,TimeUnit.MILLISECONDS);
		futRec2.get(timeout,TimeUnit.MILLISECONDS);
		assertEquals(tst1, rcv1);
		assertEquals(rcv1, rcv2);
	}

	public void testChecksumError() throws InterruptedException, ExecutionException, ImmediateDownstreamReplicationException

	{
		final ByteBuffer tst1 = getRandomData(50);
		final HAMessageWrapper msg1 = newHAWriteMessage(50, chk.checksum(tst1) + 1);
		final ByteBuffer rcv1 = ByteBuffer.allocate(2000);
		final ByteBuffer rcv2 = ByteBuffer.allocate(2000);
		// rcv.limit(50);
		final Future<Void> futRec1 = receiveServiceB.receiveData(msg1, rcv1);
		final Future<Void> futRec2 = receiveServiceC.receiveData(msg1, rcv2);
		final Future<Void> futSnd = sendServiceA.send(tst1, msg1.getMarker());
		while (!futSnd.isDone() && !futRec2.isDone()) {
			try {
				futSnd.get(10L, TimeUnit.MILLISECONDS);
			} catch (TimeoutException ignore) {
			} catch (ExecutionException e) {
                if (!InnerCause.isInnerCause(e, ChecksumError.class)) {
                    fail("Expecting " + ChecksumError.class + ", not " + e, e);
                }
			}
			try {
				futRec2.get(10L, TimeUnit.MILLISECONDS);
			} catch (TimeoutException ignore) {
			} catch (ExecutionException e) {
			    assertTrue(InnerCause.isInnerCause(e, ChecksumError.class));
			}
		}
		futSnd.get();
		try {
			futRec1.get();
			futRec2.get();
		} catch (ExecutionException e) {
            assertTrue(InnerCause.isInnerCause(e, ChecksumError.class));
		}
		assertEquals(tst1, rcv1);
		assertEquals(rcv1, rcv2);
	}

    /**
     * Unit test verifies that we can reconfigure the downstream target in an
     * HA3 setting.
     * <p>
     * The test begins with 3 services [A,B,C] in the pipeline. Service A writes
     * a message to ensure that the communications channels have been setup and
     * we verify that the message is received by B and C.
     * <p>
     * We then remove (C) from the pipeline, leaving [A,B]. Another message is
     * written on A and we verify that it is received by B.
     * <p>
     * We then add C to the pipeline, which gives us [A,B,C]. Another message is
     * written on A and we verify that the message is received by both B and C.
     * <p>
     * Finally, we remove B from the pipeline, leaving [A,C]. Another message is
     * written on A and we verify that the message is received by C.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     * @throws TimeoutException 
     */
    public void testPipelineChange_smallMessage() throws InterruptedException,
            ExecutionException, IOException, TimeoutException {

        if (!BigdataStatics.runKnownBadTests) {
            // Conditionally disabled to clean up CI. See BLZG-1279
            return;
        }
        
        doTestPipelineChange(50/* msgSize */, true/* smallMessage */);

    }

    /**
     * Variant test with a message size that we expect to be larger than will be
     * received by the OS before it hands control back to our code through the
     * {@link Selector}.
     */
    public void testPipelineChange_largeMessage() throws InterruptedException,
            ExecutionException, IOException, TimeoutException {

        doTestPipelineChange(10 * Bytes.megabyte32/* msgSize */, false/* smallMessage */);

    }

    private void doTestPipelineChange(final int msgSize, final boolean smallMessage)
            throws InterruptedException, ExecutionException, IOException,
            TimeoutException {
        
        final long timeout = 5000; // milliseconds.
        final ByteBuffer rcv1 = ByteBuffer.allocate(msgSize + Bytes.kilobyte32);
        final ByteBuffer rcv2 = ByteBuffer.allocate(msgSize + Bytes.kilobyte32);

        /*
         * Pipeline is [A,B,C]. Write on A. Verify received by {B,C}.
         */
        if(true){
            log.info("Pipeline: [A,B,C]");
            final ByteBuffer tst1 = getRandomData(msgSize);
            final HAMessageWrapper msg1 = newHAWriteMessage(msgSize, tst1);
            final Future<Void> futRec1 = receiveServiceB
                    .receiveData(msg1, rcv1);
            final Future<Void> futRec2 = receiveServiceC
                    .receiveData(msg1, rcv2);
            final Future<Void> futSnd = sendServiceA.send(tst1, msg1.getMarker());
            futSnd.get(timeout,TimeUnit.MILLISECONDS);
            futRec1.get(timeout,TimeUnit.MILLISECONDS);
            futRec2.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst1, rcv1);
            assertEquals(tst1, rcv2);
        }
        
        /*
         * Remove C from the pipeline, leaving [A,B]. Write on A. Verify
         * received by B.
         * 
         * Note: We do NOT terminate the HAReceiveService for C. It just will
         * not receive any messages from B.
         */
        if(true){
            log.info("Pipeline: [A,B] (C removed)");
            /*
             * Note: The pipeline change events are applied *before* we start
             * the send() or receiveData() operations. This means that the
             * HAReceiveService should not transfer any data from the OS socket
             * buffer into its localBuffer.
             */
            receiveServiceB.changeDownStream(null/* addrNext */);
            receiveServiceC.changeUpStream(); // close upstream socket.
            
            final ByteBuffer tst1 = getRandomData(msgSize);
            final HAMessageWrapper msg1 = newHAWriteMessage(msgSize, tst1);
            final Future<Void> futRec1 = receiveServiceB
                    .receiveData(msg1, rcv1);
//            final Future<Void> futRec2 = receiveService2
//                    .receiveData(msg1, rcv2);
            final Future<Void> futSnd = sendServiceA.send(tst1.duplicate(), msg1.getMarker());
            // Send will always succeed.
            futSnd.get(timeout, TimeUnit.MILLISECONDS);
            /*
             * Note: the following does not occur since we are only closing the
             * close of the socketChannel if there is an active ReadTask on the
             * receiver. Since the pipeline change is applied before we send()
             * from the leader, there is no active ReadTask on the follower and
             * the send() will always succeed.
             */
//            if (smallMessage) {
//                /*
//                 * For a small message, the sendService should believe that it
//                 * has successfully transferred the data. What happens is that
//                 * the socket channel on the follower has accepted all of the
//                 * message bytes into an OS level socket buffer. When that
//                 * happens, the HASendService socketChannel.write() returns
//                 * normally and the IncSendTask reports that all bytes were
//                 * transferred. This is true. However, the HAReceiveService will
//                 * throw an exception as soon as control is transferred to our
//                 * code, we will throw the exception. That exception will be
//                 * observed thrown the RMI Future for the receiveData() message
//                 * on the follower.
//                 */
//                futSnd.get(timeout, TimeUnit.MILLISECONDS);
//            } else {
//                /*
//                 * If the payload is larger than the OS level socket buffer then
//                 * control will be transferred to the HAReceiveService before
//                 * all bytes have been send by the upstream service. In this
//                 * case, the HAReceiveService will throw out the
//                 * PipelineChangedException and close the socketChannel. The
//                 * IOException caught and tested here is caused when the
//                 * HAReceiveService closes the socket channel, thus preventing
//                 * the HASendService from completing the data transfer.
//                 */
//                try {
//                    futSnd.get(timeout, TimeUnit.MILLISECONDS);
//                    fail("Expecting: " + ExecutionException.class);
//                } catch (ExecutionException ex) {
//                    if (!InnerCause.isInnerCause(ex, IOException.class)) {
//                        fail("Expecting: " + IOException.class + ", not " + ex,
//                                ex);
//                    }
//                }
//            }
            /*
             * Note: exception not thrown since pipelineChange is applied
             * *before* we invoke send().
             */
//            try {
                futRec1.get(timeout, TimeUnit.MILLISECONDS);
//                fail("Expecting: "+PipelineDownstreamChange.class);
//            } catch (ExecutionException ex) {
//                if (!InnerCause
//                        .isInnerCause(ex, PipelineDownstreamChange.class)) {
//                    fail("Expecting: " + PipelineDownstreamChange.class
//                            + ", not " + ex, ex);
//                }
//            }
//            futRec2.get();
            assertEquals(tst1, rcv1);
//            assertEquals(rcv1, rcv2);
        }

        /*
         * Restore C to the pipeline, leaving [A,B,C]. Write on A. Verify
         * received by [B,C].
         */
        if(true){
            log.info("Pipeline: [A,B,C] (C restored).");
//            if(false) {
//                final InetSocketAddress receiveAddrC = receiveServiceC
//                        .getAddrSelf();
//                receiveServiceC.terminate();
//                receiveServiceC = new HAReceiveService<IHAWriteMessageBase>(
//                        receiveAddrC, null/* downstream */);
//
//                receiveServiceC.start();
//            }
//            if(false) {
//                final InetSocketAddress receiveAddrB = receiveServiceB
//                        .getAddrSelf();
//                receiveServiceB.terminate();
//                receiveServiceB = new HAReceiveService<IHAWriteMessageBase>(
//                        receiveAddrB, receiveServiceC.getAddrSelf());
//
//                receiveServiceB.start();
//            } else {
                receiveServiceB.changeDownStream(receiveServiceC.getAddrSelf());
//            }
//            if(true) {
//                sendServiceA.terminate();
////                sendServiceA = new HASendService();
//                sendServiceA.start(receiveServiceB.getAddrSelf());
//            }

            final ByteBuffer tst1 = getRandomData(msgSize);
            final HAMessageWrapper msg1 = newHAWriteMessage(msgSize, tst1);
            final Future<Void> futRec1 = receiveServiceB
                    .receiveData(msg1, rcv1);
            final Future<Void> futRec2 = receiveServiceC
                    .receiveData(msg1, rcv2);
            final Future<Void> futSnd = sendServiceA.send(tst1, msg1.getMarker());
            // Send will always succeed.
            futSnd.get(timeout, TimeUnit.MILLISECONDS);
            /*
             * Note: the following does not occur since we are only closing the
             * close of the socketChannel if there is an active ReadTask on the
             * receiver. Since the pipeline change is applied before we send()
             * from the leader, there is no active ReadTask on the follower and
             * the send() will always succeed.
             */
//            if (smallMessage) {
//                /*
//                 * For a small message, the sendService should believe that it
//                 * has successfully transferred the data. What happens is that
//                 * the socket channel on the follower has accepted all of the
//                 * message bytes into an OS level socket buffer. When that
//                 * happens, the HASendService socketChannel.write() returns
//                 * normally and the IncSendTask reports that all bytes were
//                 * transferred. This is true. However, the HAReceiveService will
//                 * throw an exception as soon as control is transferred to our
//                 * code, we will throw the exception. That exception will be
//                 * observed thrown the RMI Future for the receiveData() message
//                 * on the follower.
//                 */
//                futSnd.get(timeout, TimeUnit.MILLISECONDS);
//            } else {
//                /*
//                 * If the payload is larger than the OS level socket buffer then
//                 * control will be transferred to the HAReceiveService before
//                 * all bytes have been send by the upstream service. In this
//                 * case, the HAReceiveService will throw out the
//                 * PipelineChangedException and close the socketChannel. The
//                 * IOException caught and tested here is caused when the
//                 * HAReceiveService closes the socket channel, thus preventing
//                 * the HASendService from completing the data transfer.
//                 */
//                try {
//                    futSnd.get(timeout, TimeUnit.MILLISECONDS);
//                    fail("Expecting: " + ExecutionException.class);
//                } catch (ExecutionException ex) {
//                    if (!InnerCause.isInnerCause(ex, IOException.class)) {
//                        fail("Expecting: " + IOException.class + ", not " + ex,
//                                ex);
//                    }
//                }
//            }
            futRec1.get(timeout,TimeUnit.MILLISECONDS);
            futRec2.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst1, rcv1);
            assertEquals(rcv1, rcv2);
        }

        /*
         * Remove B from the pipeline, leaving [A,C]. Write on A. Verify
         * received by C.
         * 
         * Note: We do NOT terminate the HAReceiveService for B. It just will
         * not receive any messages from A.
         * 
         * Note: For this case, we need to stop the HASendService on A and then
         * re-start() it with the address for C.
         */
        if (true) {
            log.info("Pipeline: [A,C] (B removed)");
            sendServiceA.terminate();
            sendServiceA.start(receiveServiceC.getAddrSelf());
            receiveServiceB.changeUpStream();
            receiveServiceB.changeDownStream(null/* addrNext */);
            receiveServiceC.changeUpStream();
            
            final ByteBuffer tst1 = getRandomData(msgSize);
            final HAMessageWrapper msg1 = newHAWriteMessage(msgSize, tst1);
//            final Future<Void> futRec1 = receiveService1
//                    .receiveData(msg1, rcv1);
            final Future<Void> futRec2 = receiveServiceC
                    .receiveData(msg1, rcv2);
            final Future<Void> futSnd = sendServiceA.send(tst1, msg1.getMarker());
            futSnd.get(timeout,TimeUnit.MILLISECONDS);
//            futRec1.get();
            futRec2.get(timeout,TimeUnit.MILLISECONDS);
//            assertEquals(tst1, rcv1);
            assertEquals(tst1, rcv2);
        }

        /*
         * Add (B) back into the pipeline.
         */
        if (true) {
            log.info("Pipeline: [A,C,B] (B added)");
            receiveServiceC.changeDownStream(receiveServiceB.getAddrSelf());
            
            final ByteBuffer tst1 = getRandomData(msgSize);
            final HAMessageWrapper msg1 = newHAWriteMessage(msgSize, tst1);
            final Future<Void> futRec1 = receiveServiceB
                    .receiveData(msg1, rcv1);
            final Future<Void> futRec2 = receiveServiceC
                    .receiveData(msg1, rcv2);
            final Future<Void> futSnd = sendServiceA.send(tst1, msg1.getMarker());
            futSnd.get(timeout,TimeUnit.MILLISECONDS);
            futRec1.get(timeout,TimeUnit.MILLISECONDS);
            futRec2.get(timeout,TimeUnit.MILLISECONDS);
            assertEquals(tst1, rcv1);
            assertEquals(tst1, rcv2);
        }

        /*
         * Note: For these cases we need to start an HASendService for (C) and
         * an HAReceiveService for (A).
         */
        HASendService sendServiceC = null;
        HAReceiveService<HAMessageWrapper> receiveServiceA = null;
        try {
            /*
             * Fail (A).
             */
            if (true) {
                log.info("Pipeline: [C,B] (A removed - leader fails)");
                sendServiceA.terminate();
                sendServiceC = new HASendService();
                sendServiceC.start(receiveServiceB.getAddrSelf());
                receiveServiceC.terminate();
                receiveServiceB.changeUpStream();

                final ByteBuffer tst1 = getRandomData(msgSize);
                final HAMessageWrapper msg1 = newHAWriteMessage(msgSize,
                        tst1);
                final Future<Void> futRec1 = receiveServiceB.receiveData(msg1,
                        rcv1);
//                final Future<Void> futRec2 = receiveServiceC.receiveData(msg1,
//                        rcv2);
                final Future<Void> futSnd = sendServiceC.send(tst1, msg1.getMarker());
                futSnd.get(timeout, TimeUnit.MILLISECONDS);
                futRec1.get(timeout, TimeUnit.MILLISECONDS);
//                futRec2.get(timeout, TimeUnit.MILLISECONDS);
                assertEquals(tst1, rcv1);
//                assertEquals(tst1, rcv2);
            }
            if(true) {
                log.info("Pipeline: [C,B,A] (A added)");
                final InetSocketAddress receiveAddrA = new InetSocketAddress(
                        getPort(0));
                receiveServiceA = new HAReceiveService<HAMessageWrapper>(
                        receiveAddrA, null/* downstream */);
                receiveServiceA.start();
                receiveServiceB.changeDownStream(receiveServiceA.getAddrSelf());

                final ByteBuffer tst1 = getRandomData(msgSize);
                final HAMessageWrapper msg1 = newHAWriteMessage(msgSize,
                        tst1);
                final Future<Void> futRec1 = receiveServiceB.receiveData(msg1,
                        rcv1);
                final Future<Void> futRec2 = receiveServiceA.receiveData(msg1,
                        rcv2);
                final Future<Void> futSnd = sendServiceC.send(tst1, msg1.getMarker());
                futSnd.get(timeout, TimeUnit.MILLISECONDS);
                futRec1.get(timeout, TimeUnit.MILLISECONDS);
                futRec2.get(timeout, TimeUnit.MILLISECONDS);
                assertEquals(tst1, rcv1);
                assertEquals(tst1, rcv2);
            }
        } finally {
            if (sendServiceC != null) {
                sendServiceC.terminate();
                sendServiceC = null;
            }
            if (receiveServiceA != null) {
                receiveServiceA.terminate();
            }
        }
        
    }

    /**
     * <em>Note: This appears to work now.</em> This test has been observed to
     * deadlock CI and is disabled until we finish debugging the HA pipeline and
     * quorums. See <a
     * href="https://sourceforge.net/apps/trac/bigdata/ticket/280>
     * https://sourceforge.net/apps/trac/bigdata/ticket/280 </a>.
     * <p>
     * When I ramp up the stress test for three nodes to 1000 passes I get one
     * of the following exceptions repeatedly:
     * <p>
     * (a) trying to reopen the downstream client socket either in the SendTask;
     * 
     * <pre>
     * Caused by: java.net.BindException: Address already in use: connect
     *     at sun.nio.ch.Net.connect(Native Method)
     *     at sun.nio.ch.SocketChannelImpl.connect(SocketChannelImpl.java:507)
     *     at com.bigdata.journal.ha.HASendService.openChannel(HASendService.java:272)
     *     at com.bigdata.journal.ha.HASendService$SendTask.call(HASendService.java:319)
     *     at com.bigdata.journal.ha.HASendService$SendTask.call(HASendService.java:1)
     *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:138)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
     *     at java.lang.Thread.run(Thread.java:619)
     * </pre>
     * 
     * (b) trying to open a selector on the client socket in the Receive task.
     * 
     * <pre>
     * Caused by: java.io.IOException: Unable to establish loopback connection
     *     at sun.nio.ch.PipeImpl$Initializer.run(PipeImpl.java:106)
     *     at java.security.AccessController.doPrivileged(Native Method)
     *     at sun.nio.ch.PipeImpl.<init>(PipeImpl.java:122)
     *     at sun.nio.ch.SelectorProviderImpl.openPipe(SelectorProviderImpl.java:27)
     *     at java.nio.channels.Pipe.open(Pipe.java:133)
     *     at sun.nio.ch.WindowsSelectorImpl.<init>(WindowsSelectorImpl.java:105)
     *     at sun.nio.ch.WindowsSelectorProvider.openSelector(WindowsSelectorProvider.java:26)
     *     at java.nio.channels.Selector.open(Selector.java:209)
     *     at com.bigdata.journal.ha.HAReceiveService$ReadTask.call(HAReceiveService.java:508)
     * </pre>
     * 
     * It seems that we need to hold the client socket open across send and
     * receive tasks and also hold open the selectors. I think that (a) is
     * caused by the reconnect latency while (b) is caused by GC not having
     * allowed reclamation yet of the existing selectors.
     * 
     * @throws InterruptedException
     */
    public void testStressDirectBuffers() throws InterruptedException {

		IBufferAccess tstdb = null, rcv1db = null, rcv2db = null;
		int i = -1, sze = -1;
		try {
			tstdb = DirectBufferPool.INSTANCE.acquire();
			rcv1db = DirectBufferPool.INSTANCE.acquire();
			rcv2db = DirectBufferPool.INSTANCE.acquire();
			final ByteBuffer tst = tstdb.buffer(), rcv1 = rcv1db.buffer(), rcv2 = rcv2db.buffer();
			for (i = 0; i < 1000; i++) {

				if(log.isTraceEnabled())
				    log.trace("Transferring message #" + i);

				sze = 1 + r.nextInt(tst.capacity());
				getRandomData(tst, sze);
				final HAMessageWrapper msg = newHAWriteMessage(sze, tst);
				assertEquals(0, tst.position());
				assertEquals(sze, tst.limit());
				// FutureTask return ensures remote ready for Socket data
				final Future<Void> futRec1 = receiveServiceB.receiveData(msg, rcv1);
				final Future<Void> futRec2 = receiveServiceC.receiveData(msg, rcv2);
				final Future<Void> futSnd = sendServiceA.send(tst, msg.getMarker());
				while (!futSnd.isDone() && !futRec1.isDone() && !futRec2.isDone()) {
					try {
						futSnd.get(10L, TimeUnit.MILLISECONDS);
					} catch (TimeoutException ignored) {
					}
					try {
						futRec1.get(10L, TimeUnit.MILLISECONDS);
					} catch (TimeoutException ignored) {
					}
					try {
						futRec2.get(10L, TimeUnit.MILLISECONDS);
					} catch (TimeoutException ignored) {
					}
				}
				futSnd.get();
                futRec1.get();
                futRec2.get();
                // make sure buffer has been transmitted
                assertEquals(tst, rcv1);
                // make sure buffer has been transmitted
                assertEquals(rcv1, rcv2);
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
				try {
					if (rcv1db != null) {
						rcv1db.release();
					}
				} finally {
					if (rcv2db != null) {
						rcv2db.release();
					}
				}
			}
		}
	}

}
