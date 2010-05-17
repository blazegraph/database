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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.TestCase3;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;
import com.bigdata.util.InnerCause;

/**
 * Test the raw socket protocol implemented by {@link HASendService} and
 * {@link HAReceiveService} against a pipeline of 3 nodes.
 * 
 * @author martyn Cutcher
 */
public class TestHASendAndReceive3Nodes extends TestCase3 {

	private ChecksumUtility chk = new ChecksumUtility();

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

			log.warn("suggestedPort is busy: suggestedPort=" + suggestedPort + ", using port=" + port + " instead");

		}

		return port;

	}

	public void testSimpleExchange() throws InterruptedException, ExecutionException

	{
		final ByteBuffer tst1 = getRandomData(50);
		final HAWriteMessage msg1 = new HAWriteMessage(50, chk.checksum(tst1));
		final ByteBuffer rcv1 = ByteBuffer.allocate(2000);
		final ByteBuffer rcv2 = ByteBuffer.allocate(2000);
		// rcv.limit(50);
		final Future<Void> futRec1 = receiveService1.receiveData(msg1, rcv1);
		final Future<Void> futRec2 = receiveService2.receiveData(msg1, rcv2);
		final Future<Void> futSnd = sendService.send(tst1);
		while (!futSnd.isDone() && !futRec2.isDone()) {
			try {
				futSnd.get(10L, TimeUnit.MILLISECONDS);
			} catch (TimeoutException ignore) {
			}
			try {
				futRec2.get(10L, TimeUnit.MILLISECONDS);
			} catch (TimeoutException ignore) {
			}
		}
		futSnd.get();
		futRec1.get();
		futRec2.get();
		assertEquals(tst1, rcv1);
		assertEquals(rcv1, rcv2);
	}

	public void testChecksumError() throws InterruptedException, ExecutionException

	{
		final ByteBuffer tst1 = getRandomData(50);
		final HAWriteMessage msg1 = new HAWriteMessage(50, chk.checksum(tst1) + 1);
		final ByteBuffer rcv1 = ByteBuffer.allocate(2000);
		final ByteBuffer rcv2 = ByteBuffer.allocate(2000);
		// rcv.limit(50);
		final Future<Void> futRec1 = receiveService1.receiveData(msg1, rcv1);
		final Future<Void> futRec2 = receiveService2.receiveData(msg1, rcv2);
		final Future<Void> futSnd = sendService.send(tst1);
		while (!futSnd.isDone() && !futRec2.isDone()) {
			try {
				futSnd.get(10L, TimeUnit.MILLISECONDS);
			} catch (TimeoutException ignore) {
			} catch (ExecutionException e) {
				assertTrue(e.getCause().getMessage().equals("Checksum Error"));
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
     * FIXME When I ramp up the stress test for three nodes to 1000 passes I get
     * one of the following exceptions repeatedly:
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

		ByteBuffer tst = null, rcv1 = null, rcv2 = null;
		int i = -1, sze = -1;
		try {
			tst = DirectBufferPool.INSTANCE.acquire();
			rcv1 = DirectBufferPool.INSTANCE.acquire();
			rcv2 = DirectBufferPool.INSTANCE.acquire();
			for (i = 0; i < 1000; i++) {

				if(log.isTraceEnabled())
				    log.trace("Transferring message #" + i);

				sze = 1 + r.nextInt(tst.capacity());
				getRandomData(tst, sze);
				final HAWriteMessage msg = new HAWriteMessage(sze, chk.checksum(tst));
				assertEquals(0, tst.position());
				assertEquals(sze, tst.limit());
				// FutureTask return ensures remote ready for Socket data
				final Future<Void> futRec1 = receiveService1.receiveData(msg, rcv1);
				final Future<Void> futRec2 = receiveService2.receiveData(msg, rcv2);
				final Future<Void> futSnd = sendService.send(tst);
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
                if (log.isTraceEnabled())
                    log.trace("Looks good for #" + i);
            }
		} catch (Throwable t) {
			throw new RuntimeException("i=" + i + ", sze=" + sze + " : " + t, t);
		} finally {
			try {
				if (tst != null) {
					DirectBufferPool.INSTANCE.release(tst);
				}
			} finally {
				try {
					if (rcv1 != null) {
						DirectBufferPool.INSTANCE.release(rcv1);
					}
				} finally {
					if (rcv2 != null) {
						DirectBufferPool.INSTANCE.release(rcv2);
					}
				}
			}
		}
	}
}
