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
	ObjectSocketChannelStream out1;

	public TestSocketMessage() {
		try {
			final File file1 = File.createTempFile("cache1", ".tmp");
			final File file2 = File.createTempFile("cache2", ".tmp");

			cache1 = new WriteCache.FileChannelWriteCache(0, DirectBufferPool.INSTANCE.acquire(), false,
					false, new ReopenFileChannel(file1, "rw"));
			cache2 = new WriteCache.FileChannelWriteCache(0, DirectBufferPool.INSTANCE.acquire(), false,
					false, new ReopenFileChannel(file2, "rw"));

			final int port = 3800;
			startWriteCacheSocket(cache2, port);

			SocketChannel socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(true);

			// Kick off connection establishment
			socketChannel.connect(new InetSocketAddress("localhost", port));
			socketChannel.finishConnect();
			// Thread.sleep(2000);

			// and set the output stream
			out1 = new ObjectSocketChannelStream(socketChannel);

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	/**
	 * Fix for deadlock bug in NIO
	 * @param channel
	 * @return
	 */
//	private static ByteChannel wrapChannel(final ByteChannel channel) {
//		return new ByteChannel() {
//			public int write(ByteBuffer src) throws IOException {
//				return channel.write(src);
//			}
//
//			public int read(ByteBuffer dst) throws IOException {
//				return channel.read(dst);
//			}
//
//			public boolean isOpen() {
//				return channel.isOpen();
//			}
//
//			public void close() throws IOException {
//				channel.close();
//			}
//		};
//	}

	void startWriteCacheSocket(final WriteCache cache, final int port) {
		try {
			IHAClient handler = new IHAClient() {
				ObjectSocketChannelStream input = null;
				@Override
				public ObjectSocketChannelStream getInputSocket() {
					return input;
				}

				@Override
				public ObjectSocketChannelStream getNextSocket() {
					return null;
				}

				@Override
				public WriteCache getWriteCache() {
					return cache;
				}

				@Override
				public void setInputSocket(ObjectSocketChannelStream in) {
					input = in;
				}

				@Override
				public void truncate(long extent) {
					System.out.println("Received truncate request");
				}

			};

			HAServer server = new HAServer(port, handler);

			server.start();

			// Thread.sleep(2000); // make sure the server is started
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public void testSimpleMessage() {
		SocketMessage.HAWriteMessage msg1 = new SocketMessage.HAWriteMessage(cache1);
		SocketMessage.HATruncateMessage msg2 = new SocketMessage.HATruncateMessage(210000);
		final ByteBuffer data4 = getRandomData(r.nextInt(cache1.capacity()-100) + 1);

		try {
			cache1.write(0, data4, 0);
			
			msg2.send(out1);

			msg1.send(out1);
			
			msg2.send(out1);
			
			msg1.send(out1);

			Thread.sleep(2000); // give chance for messages to be processed
			
			// assertEquals(data4, cache1.read(0)); // did the data get the the downstream cache?
			assertEquals(cache1.read(0), cache2.read(0)); // did the data get the the downstream cache?
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
