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
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.io.TestCase3;
import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCache.FileChannelScatteredWriteCache;
import com.bigdata.journal.ha.SocketMessage.HATruncateConfirm;
import com.bigdata.journal.ha.SocketMessage.ITruncateCallback;
import com.bigdata.journal.ha.SocketMessage.HAWriteMessage.HAWriteConfirm;
import com.bigdata.journal.ha.SocketMessage.HAWriteMessage.IWriteCallback;
import com.bigdata.util.ChecksumError;

/**
 * Tests for the HA SocketMessages over raw Sockets
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestSocketMessage extends TestCase3 {

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

//		/**
//		 * Hook used by the unit tests to destroy their test files.
//		 */
//		public void destroy() {
//			try {
//				raf.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}

//		/**
//		 * Read some data out of the file.
//		 * 
//		 * @param off
//		 *            The offset of the record.
//		 * @param nbytes
//		 *            The #of bytes to be read.
//		 * @return The record.
//		 */
//		public ByteBuffer read(final long off, final int nbytes) throws IOException {
//
//			final ByteBuffer tmp = ByteBuffer.allocate(nbytes);
//
//			FileChannelUtility.readAll(this, tmp, off);
//			
//			tmp.flip();
//
//			return tmp;
//
//		}

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
	private HAServer server;

	public TestSocketMessage() {

	}
	
	protected void setUp() throws Exception {
//		try {
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
			
			server = startWriteCacheSocket(cache2, port);
			server.start();
			
			messenger = new HAConnect(new InetSocketAddress(port));
			messenger.start();

//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (Throwable e) {
//			e.printStackTrace();
//		}
	}

	protected void tearDown() throws Exception {

	    if(messenger!=null)
	        messenger.interrupt();
	    if(server!=null)
	        server.interrupt();
        if(cache3!=null)
            cache3.close();
        if(cache2!=null)
            cache2.close();
        if(cache1!=null)
            cache1.close();
	    
	}

	HAServer startWriteCacheSocket(final WriteCache cache, final int port) throws UnknownHostException {
//		try {
			IHAClient handler = new IHAClient() {
				ObjectSocketChannelStream input = null;
//				@Override
				public ObjectSocketChannelStream getInputSocket() {
					return input;
				}

//				@Override
				public HAConnect getNextConnect() {
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

//				@Override
				public void setNextOffset(long lastOffset) {
					// not needed
				}

			};

            HAServer server = new HAServer(InetAddress.getLocalHost(), port, handler);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} // make sure the server is started

			return server;
			
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
	}

	public void testSimpleMessage() throws ChecksumError, InterruptedException, IOException {
		
//		setup();
		
		SocketMessage.HAWriteMessage msg1 = new SocketMessage.HAWriteMessage(cache1);
		SocketMessage.HATruncateMessage msg2 = new SocketMessage.HATruncateMessage(210000);
		SocketMessage.HAWriteMessage msg3 = new SocketMessage.HAWriteMessage(cache3);
		final ByteBuffer data2 = getRandomData(r.nextInt(100) + 1);
		final ByteBuffer data1 = ByteBuffer.wrap(new byte[] {1,2,3,4,5,6,7,8,9,10});
		// final ByteBuffer data2 = ByteBuffer.wrap(new byte[] {50,51,52,53,54,55});
		// final ByteBuffer data3 = ByteBuffer.wrap(new byte[] {11,12,13,14,15});
		final ByteBuffer data3 = getRandomData(r.nextInt(cache1.capacity()-100) + 1);
		// final ByteBuffer data4 = getRandomData(r.nextInt((cache1.capacity()-100)/2) + 1);
		final ByteBuffer data4 = ByteBuffer.wrap(new byte[] {16,17,18,19,20});
		final ByteBuffer data5 = ByteBuffer.wrap(new byte[] {31,32,33,34,35,36,37});

		HAGlue glue = null;
		IWriteCallback whandler = new IWriteCallback() {

			public void ack(HAWriteConfirm writeConfirm) {
				System.out.println("Got write acknowledgement : " + writeConfirm.getTwinId());
			}
		};
		
		ITruncateCallback thandler = new ITruncateCallback() {

			public void ack(HATruncateConfirm truncateConfirm) {
				System.out.println("Got truncate acknowledgement : " + truncateConfirm.getTwinId());
			}
		};

        msg1.setHandler(whandler);
        msg2.setHandler(thandler);
        msg3.setHandler(whandler);

        long addr1 = 0;
        cache1.write(addr1, data1, 0);
        long addr2 = addr1 + data1.capacity();
        assertTrue(cache1.write(addr2, data2, 0));
        data1.flip();
        // verify record @ addr can be read.
        assertNotNull(cache1.read(addr1));
        // verify data read back @ addr.
        assertEquals(data1, cache1.read(addr1));

        long addr3 = addr2 + data2.capacity();
        cache3.write(addr3, data5, 0);

        // messages are processed in sequence, so just wait for last one
        messenger.send(msg1, true);
        messenger.send(msg2, true);
        messenger.send(msg3, true);

        // Thread.sleep(2000); // give chance for messages to be processed
        assertNotNull(cache1.read(addr2));

        // assertEquals(data4, cache1.read(0)); // did the data get the the
        // downstream cache?
        ByteBuffer tst1 = cache1.read(addr1);
        ByteBuffer tst2 = cache2.read(addr1);
        System.out.println("tst capacity: " + tst1.capacity() + "/"
                + tst2.capacity());

        assertEquals(tst1, data1);
        assertEquals(tst1, tst2);
        assertEquals(cache1.read(addr2), cache2.read(addr2));
        
        /*
         * FIXME This unit test will "pass" even though the operation did not
         * succeed. It needs to do more to verify that the state change was
         * undertaken by all nodes.
         */
        fail("finish test");
        
    }

}
