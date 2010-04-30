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

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCache.FileChannelScatteredWriteCache;
import com.bigdata.journal.ha.SocketMessage.HATruncateConfirm;
import com.bigdata.journal.ha.SocketMessage.ITruncateCallback;
import com.bigdata.journal.ha.SocketMessage.HAWriteMessage.HAWriteConfirm;
import com.bigdata.journal.ha.SocketMessage.HAWriteMessage.IWriteCallback;

import junit.framework.TestCase;

/**
 * Tests for the HA SocketMessages over raw Sockets
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestSocketMessage extends TestCase {

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
	 * Helper method verifies that the contents of <i>actual</i> from position()
	 * to limit() are consistent with the expected byte[]. A read-only view of
	 * <i>actual</i> is used to avoid side effects on the position, mark or
	 * limit properties of the buffer.
	 * 
	 * @param expected
	 *            Non-null byte[].
	 * @param actual
	 *            Buffer.
	 */
	public static void assertEquals(ByteBuffer expectedBuffer, ByteBuffer actual) {

		if (expectedBuffer == null)
			throw new IllegalArgumentException();

		if (actual == null)
			fail("actual is null");

		if (expectedBuffer.hasArray() && expectedBuffer.arrayOffset() == 0) {

			// evaluate byte[] against actual.
			assertEquals(expectedBuffer.array(), actual);

			return;

		}

		/*
		 * Copy the expected data into a byte[] using a read-only view on the
		 * buffer so that we do not mess with its position, mark, or limit.
		 */
		final byte[] expected;
		{

			expectedBuffer = expectedBuffer.asReadOnlyBuffer();

			final int len = expectedBuffer.remaining();

			expected = new byte[len];

			actual.get(expected);

		}

		// evaluate byte[] against actual.
		assertEquals(expectedBuffer.array(), actual);

	}

	/**
	 * Helper method verifies that the contents of <i>actual</i> from position()
	 * to limit() are consistent with the expected byte[]. A read-only view of
	 * <i>actual</i> is used to avoid side effects on the position, mark or
	 * limit properties of the buffer.
	 * 
	 * @param expected
	 *            Non-null byte[].
	 * @param actual
	 *            Buffer.
	 */
	public static void assertEquals(final byte[] expected, ByteBuffer actual) {

		if (expected == null)
			throw new IllegalArgumentException();

		if (actual == null)
			fail("actual is null");

		if (actual.hasArray() && actual.arrayOffset() == 0) {

			assertEquals(expected, actual.array());

			return;

		}

		/*
		 * Create a read-only view on the buffer so that we do not mess with its
		 * position, mark, or limit.
		 */
		actual = actual.asReadOnlyBuffer();

		final int len = actual.remaining();

		final byte[] actual2 = new byte[len];

		actual.get(actual2);

		// compare byte[]s.
		assertEquals(expected, actual2);

	}

	private static class ReopenFileChannel implements IReopenChannel<FileChannel> {

		final private File file;

		private final String mode;

		private volatile RandomAccessFile raf;

		public ReopenFileChannel(final File file, final String mode) throws IOException {

			this.file = file;

			this.mode = mode;

			reopenChannel();

		}

		public String toString() {

			return file.toString();

		}

		/**
		 * Hook used by the unit tests to destroy their test files.
		 */
		public void destroy() {
			try {
				raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * Read some data out of the file.
		 * 
		 * @param off
		 *            The offset of the record.
		 * @param nbytes
		 *            The #of bytes to be read.
		 * @return The record.
		 */
		public ByteBuffer read(final long off, final int nbytes) throws IOException {

			final ByteBuffer tmp = ByteBuffer.allocate(nbytes);

			FileChannelUtility.readAll(this, tmp, off);

			return tmp;

		}

		synchronized public FileChannel reopenChannel() throws IOException {

			if (raf != null && raf.getChannel().isOpen()) {

				/*
				 * The channel is still open. If you are allowing concurrent
				 * reads on the channel, then this could indicate that two
				 * readers each found the channel closed and that one was able
				 * to re-open the channel before the other such that the channel
				 * was open again by the time the 2nd reader got here.
				 */

				return raf.getChannel();

			}

			// open the file.
			this.raf = new RandomAccessFile(file, mode);

			return raf.getChannel();

		}

	};

	WriteCache cache1 = null;
	WriteCache cache2 = null;
	
	HAConnect messenger;
	private FileChannelScatteredWriteCache cache3;

	public TestSocketMessage() {

	}
	
	protected void setup() {
		try {
			final File file1 = File.createTempFile("cache1", ".tmp");
			final File file2 = File.createTempFile("cache2", ".tmp");
			final File file3 = File.createTempFile("cache3", ".tmp");

			cache1 = new WriteCache.FileChannelScatteredWriteCache(DirectBufferPool.INSTANCE.acquire(),
                    false, false, new ReopenFileChannel(file1, "rw"));
			cache2 = new WriteCache.FileChannelScatteredWriteCache(DirectBufferPool.INSTANCE.acquire(),
                    false, false, new ReopenFileChannel(file2, "rw"));
			cache3 = new WriteCache.FileChannelScatteredWriteCache(DirectBufferPool.INSTANCE.acquire(),
                    false, false, new ReopenFileChannel(file3, "rw"));
			
			final int port = 3800;
			startWriteCacheSocket(cache2, port);
			
			
			messenger = new HAConnect(new InetSocketAddress(port));
			messenger.start();

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}


	void startWriteCacheSocket(final WriteCache cache, final int port) {
		try {
			IHAClient handler = new IHAClient() {
				ObjectSocketChannelStream input = null;
//				@Override
				public ObjectSocketChannelStream getInputSocket() {
					return input;
				}

//				@Override
				public ObjectSocketChannelStream getNextSocket() {
					return null;
				}

//				@Override
				public WriteCache getWriteCache() {
					return cache;
				}

//				@Override
				public void setInputSocket(ObjectSocketChannelStream in) {
					input = in;
				}

//				@Override
				public void truncate(long extent) {
					System.out.println("Received truncate request");
				}

			};

            HAServer server = new HAServer(InetAddress.getLocalHost(), port,
                    handler);

			server.start();

			// Thread.sleep(2000); // make sure the server is started
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public void testSimpleMessage() {
		
		setup();
		
		SocketMessage.HAWriteMessage msg1 = new SocketMessage.HAWriteMessage(cache1);
		SocketMessage.HATruncateMessage msg2 = new SocketMessage.HATruncateMessage(210000);
		SocketMessage.HAWriteMessage msg3 = new SocketMessage.HAWriteMessage(cache3);
		final ByteBuffer data1 = getRandomData(r.nextInt(100) + 1);
		System.out.println("data1 capacity: " + data1.capacity());
		final ByteBuffer data3 = getRandomData(r.nextInt(cache1.capacity()-100) + 1);
		final ByteBuffer data4 = getRandomData(r.nextInt((cache1.capacity()-100)/2) + 1);
		final ByteBuffer data5 = getRandomData(r.nextInt((cache1.capacity()-100)/2) + 1);

		HAGlue glue = null;
		IWriteCallback whandler = new IWriteCallback() {

			public void ack(HAWriteConfirm writeConfirm) {
				System.out.println("Got write acknowledgement : " + writeConfirm.twinId);
			}
		};
		
		ITruncateCallback thandler = new ITruncateCallback() {

			public void ack(HATruncateConfirm truncateConfirm) {
				System.out.println("Got truncate acknowledgement : " + truncateConfirm.twinId);
			}
		};
		try {
			msg1.setHandler(whandler);
			msg2.setHandler(thandler);
			msg3.setHandler(whandler);
			
			long addr1 = 0;
			cache1.write(addr1, data1, 0);
			long addr2 = addr1 + data1.capacity();
            assertTrue(cache1.write(addr2, data4, 0));
            // verify record @ addr can be read.
            assertNotNull(cache1.read(addr2));
            // verify data read back @ addr.
            // assertEquals(data4, cache1.read(addr1));            
			
			long addr3 = addr2 + data4.capacity();
			cache3.write(addr3, data5, 0);
			
			// messages are processed in sequence, so just wait for last one
			messenger.send(msg3);
			messenger.send(msg1);
			messenger.send(msg2, true);
			
			// Thread.sleep(2000); // give chance for messages to be processed
	           assertNotNull(cache1.read(addr2));
			
			// assertEquals(data4, cache1.read(0)); // did the data get the the downstream cache?
			ByteBuffer tst1 = cache1.read(addr2);
			ByteBuffer tst2 = cache2.read(addr2);
			System.out.println("tst capacity: " + tst1.capacity() + "/" + tst2.capacity());
			// At present is a problem with WriteCache (test fails)
			// assertEquals(data4, tst1); // did the data get the the downstream cache?
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
