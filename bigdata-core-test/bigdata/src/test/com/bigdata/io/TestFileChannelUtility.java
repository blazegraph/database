/*

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
/*
 * Created on May 16, 2008
 */

package com.bigdata.io;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.io.FileChannelUtility.AsyncTransfer;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;

import junit.framework.TestCase;

/**
 * Test suite for {@link FileChannelUtility}.
 * 
 * @todo this test suite does not test the behavior under concurrent IO
 *       requests. readAll() and writeAll() should be ok, but can not offer
 *       atomic guarentees since at least write operations have been observed to
 *       break into multiple IOs under load. transferAll() is neither atomic nor
 *       isolated since it has side-effects on the position of the source and
 *       target channels.
 * 
 * @todo test the new {@link IReopenChannel} variants.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFileChannelUtility extends TestCase {

	private static final Logger log = Logger.getLogger(TestFileChannelUtility.class);

    /**
     * 
     */
    public TestFileChannelUtility() {
    }

    /**
     * @param arg0
     */
    public TestFileChannelUtility(String arg0) {
        super(arg0);
    }

    /**
     * The file size for the tests (20M).
     */
    private final int FILE_SIZE = 20 * Bytes.megabyte32;
    
    private Random r;
    
    @Override
    protected void setUp() throws Exception {
    	
    		super.setUp();
    		
    		r = new Random();
    	
    }

    @Override
    protected void tearDown() throws Exception {
    	
    		r = null;
    	
    		super.tearDown();
    	
    }
   
    // size of a single buffer.
    static private final int bufferSize = DirectBufferPool.INSTANCE.getBufferCapacity();

    /** Start at any position in the source file (up to int32 offset). */
	protected long getRandomPosition(final RandomAccessFile raf)
			throws IOException {

        final long pos = r.nextInt((int)raf.length());
        
        return pos;

    }

    /**
	 * Choose #of bytes for the an operation which is no more bytes than exist
	 * from that position to the end of the file but up to 4 times the capacity
	 * of the direct buffers in use by the pool (and no more than
	 * Integer.MAX_VALUE bytes regardless).
	 * 
	 * @param pos
	 *            A position within that file.
	 * 
	 * @throws IOException
	 */
	protected int getRandomLength(final RandomAccessFile raf, final long pos)
			throws IOException {

        final int count = (int) Math.min(Integer.MAX_VALUE, Math.min(
            raf.length() - pos, bufferSize
                    * r.nextInt(3) + r.nextInt(bufferSize)));
       
        return count;
        
    }

	protected void assertSameData(final byte[] expected, final byte[] actual) {

        for (int i = 0; i < expected.length; i++) {

            if (expected[i] != actual[i]) {

                fail("bytes differ starting at offset=" + i);
                
            }

        }
        
    }
    
    /**
     * A single trial testing the behavior of readAll() and writeAll()
     * 
     * @throws IOException
     */
    public void test_oneTrial_readAll_writeAll() throws IOException {
        
        final File file = File.createTempFile("TestFileChannelUtility", getName());

        file.deleteOnExit();

        final RandomAccessFile raf = new RandomAccessFile(file, "rw");

        try {

            /*
             * Setup some random data (20M worth).
             * 
             * Note: This array will be our ground truth.
             */
            final byte[] expected = new byte[FILE_SIZE];
            {

                r.nextBytes(expected);

            }

            final int ioCount1 = FileChannelUtility.writeAll(raf.getChannel(),
                    ByteBuffer.wrap(expected), 0L/* pos */);

            final byte[] actual = new byte[expected.length];

            final int ioCount2 = FileChannelUtility.readAll(raf.getChannel(),
                    ByteBuffer.wrap(actual), 0L);

            // used to provoke a test failure.
//            expected[12]++;

            assertSameData(expected, actual);
            
        } finally {

            try {
                raf.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }

            file.delete();

        }

    }
    
    /**
     * A sequence of trials testing the behavior of readAll() and writeAll().
     * The ground truth data is changed periodically and updated on the source
     * file and continued testing is performed.
     * 
     * @throws IOException
     */
    public void test_readAll_writeAll() throws IOException {
        
        File file = File.createTempFile("TestFileChannelUtility", getName());

        file.deleteOnExit();

        RandomAccessFile raf = new RandomAccessFile(file, "rw");

        try {

            /*
             * Setup some random data (20M worth).
             * 
             * Note: This array will be our ground truth.
             */
            final byte[] expected = new byte[FILE_SIZE];
            {

                r.nextBytes(expected);

            }

            // write ground truth onto the file.
            FileChannelUtility.writeAll(raf.getChannel(), ByteBuffer
                    .wrap(expected), 0L/* pos */);

            /*
             * Do a number of trials.
             */
            final int ntrials = 20;
            for (int trial = 0; trial < ntrials; trial++) {

                // do a number of random test verification tests.
                doReadTest(50,expected, raf);

                if (trial + 1 < ntrials) {
                    
                    /*
                     * If we will do another trial we first purturb the ground
                     * truth and write the updated region on the file before we
                     * test again.
                     */

                    // start of purturbed region.
                    final int off = r.nextInt(expected.length);

                    // length of purturbed region.
                    final int len = r.nextInt(expected.length - off);
                    
					if (log.isInfoEnabled())
						log.info("purturbing region after trial: trial="
								+ trial + ", off=" + off + ", len=" + len);
                    
                    final byte[] a = new byte[len];
                    
                    // random data
                    r.nextBytes(a);
                    
                    // copy to ground truth array.
                    System.arraycopy(a, 0, expected, off, len);
                    
                    // seek to a random position since writeAll() should not
                    // effect the channel position.
                    final long randomPosition = getRandomPosition(raf); 
                    
                    raf.getChannel().position(randomPosition);

                    // and write on the file channel as well.
                    final int ioCount = FileChannelUtility.writeAll(raf
                            .getChannel(), ByteBuffer.wrap(a), (long) off);
                    
                    assertEquals(randomPosition,raf.getChannel().position());
                    
                    // Note: used to provoke a test failure.
//                    r.nextBytes(expected);
                    
                }
                
            }
            
        } finally {

            try {
                raf.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }

            file.delete();

        }

    }

    /**
     * Verify {@link FileChannelUtility#readAll(FileChannel, ByteBuffer, long)}
     * using a file that the caller has pre-written and a byte[] containing the
     * ground truth data for that file.
     * 
     * @param expected
     *            The ground truth data.
     * @param raf
     *            A file pre-written with that ground truth data.
     *            
     * @throws IOException
     */
    protected void doReadTest(final int ntrials, final byte[] expected, RandomAccessFile raf) throws IOException {
        
        final FileChannel channel = raf.getChannel();

        // a bunch of random reads.
        for(int trial=0; trial<ntrials; trial++) {

            final long pos = getRandomPosition(raf);
            
            assert pos <= Integer.MAX_VALUE;
            
            final int count = getRandomLength(raf, pos);
            
			if (log.isInfoEnabled())
				log.info("verifying data: pos=" + pos + ", count=" + count);

            final ByteBuffer actual = ByteBuffer.wrap(new byte[count]);

            // seek to a random position since readAll() should not effect the
            // position.
            final long randomPosition = getRandomPosition(raf); 
            
            channel.position(randomPosition);
            
            final int ioCount = FileChannelUtility.readAll(channel, actual, pos);
            
            assertEquals( randomPosition, channel.position() );
            
            assert actual.position() == actual.limit();

            assert actual.limit() == count;

            if (ioCount > 1) {

				if (log.isInfoEnabled())
					log.info("Note: read required: " + ioCount + " IOs");

            }
            
            if (0 != BytesUtil.compareBytesWithLenAndOffset((int) pos, count,
                    expected, 0, count, actual.array())) {
                
                fail("Data differ");
                
            }
            
        }
        
    }
    
    /**
     * Test of
     * {@link FileChannelUtility#transferAll(FileChannel, long, long, RandomAccessFile)}
     * on 20M of random data using a bunch of transfer of different sizes from
     * different positions in the source file.
     * 
     * @throws IOException
     */
    public void test_transferAllFrom() throws IOException {

//        if(SystemUtil.isOSX()) {
//            /*
//             * FIXME For some reason, this unit test is hanging under OS X.
//             * 
//             * @see https://sourceforge.net/apps/trac/bigdata/ticket/287
//             */
//            fail("Unit test hangs under OS X");
//        }
        
        final File sourceFile = File.createTempFile("TestFileChannelUtility", getName());

        sourceFile.deleteOnExit();

        final File targetFile = File.createTempFile("TestFileChannelUtility", getName());

        targetFile.deleteOnExit();

        final RandomAccessFile source = new RandomAccessFile(sourceFile, "rw");
        
        final RandomAccessFile target = new RandomAccessFile(targetFile, "rw");
       
        try {
            
            /*
             * Setup some random data (20M worth).
             * 
             * Note: This array will be our ground truth.
             */
            final byte[] expected = new byte[FILE_SIZE];
            {

                r.nextBytes(expected);

            }

            // write ground truth onto the file.
            FileChannelUtility.writeAll(source.getChannel(), ByteBuffer
                    .wrap(expected), 0L/* pos */);
            target.setLength(FILE_SIZE);            
            
            // do a bunch of trials of random transfers.
            for(int trial=0; trial<1000; trial++) { 
                
                final long fromPosition = getRandomPosition(source);
                
                assert fromPosition < expected.length;
                
                final int count = getRandomLength(source, fromPosition);
                
				if (log.isInfoEnabled())
					log.info("fromPosition=" + fromPosition + ", count="
							+ count);

                /*
                 * Transfer some number of bytes from the source channel to the
                 * target channel.
                 * 
                 * Note: The source channel position is modified as a side
                 * effect but the target channel position is NOT modified.
                 */
                final long randomSourcePosition = getRandomPosition(source);
                source.getChannel().position(randomSourcePosition);

                final long randomTargetPosition = getRandomPosition(target);
                target.getChannel().position(randomTargetPosition);

                // to the same offset on the target channel.
                final long toPosition = fromPosition;

                final int ioCount1 = FileChannelUtility.transferAll(source
                        .getChannel(), fromPosition, count, target, toPosition);

                // changed : new position is [fromPosition + count]
                assertEquals("sourcePosition", fromPosition + count, source
                        .getChannel().position());

                // changed : new position is [toPosition + count].
                assertEquals("targetPosition", toPosition + count, target
                        .getChannel().position());
                
                /*
                 * Read the data back from the target channel.
                 */

                final ByteBuffer actual = ByteBuffer.wrap(new byte[count]);

                final int ioCount2 = FileChannelUtility.readAll(target
                        .getChannel(), actual, fromPosition);

                assert actual.position() == actual.limit();
                
                assert actual.limit() == count;
                
                // Note: used to provoke a test failure.
//                actual.array()[0]++;
                
                /*
                 * Verify that the transferred data agrees with the ground truth.
                 */
                if (0 != BytesUtil.compareBytesWithLenAndOffset((int) fromPosition, count,
                        expected, 0, count, actual.array())) {
                    
                    fail("Data differs: trial=" + trial + ", fromPosition="
                            + fromPosition + ", count=" + count);
                    
                }

            }
            
        } finally {

            try {
                source.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }

            sourceFile.delete();

            try {
                target.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }

            targetFile.delete();
            
        }
        
    }
    
	public void testReopenerInputStream() throws IOException,
			InterruptedException {
		final Random r = new Random();

		final File sourceFile = File.createTempFile("TestFileChannelUtility",
				getName());
		try {

			final int filelen = 20 * 1024 * 1024;

			final FileOutputStream outstr = new FileOutputStream(sourceFile);
			try {
				// write 20M!
				byte[] buf = new byte[4096];
				r.nextBytes(buf);

				for (int i = 0; i < filelen; i += buf.length) {
					outstr.write(buf);
				}
				outstr.flush();
			} finally {
				outstr.close();
			}

			final RandomAccessFile raf = new RandomAccessFile(sourceFile, "rw");
			final FileChannel channel = raf.getChannel();

			try {
				final IReopenChannel<FileChannel> reopener = new IReopenChannel<FileChannel>() {

					@Override
					public FileChannel reopenChannel() throws IOException {

						if (channel == null)
							throw new IOException("Closed");

						return channel;

					}
				};

				final FileChannelUtility.ReopenerInputStream instr = new FileChannelUtility.ReopenerInputStream(
						reopener);
				try {
					int totalReads = 0;
					final byte[] buf = new byte[8192];
					while (totalReads < filelen) {
						int nxtLen = 1 + r.nextInt(buf.length - 1); // max 8192
																	// read

						final int rdlen = instr.read(buf, 0, nxtLen);
						if (rdlen == -1) {
							throw new EOFException("Unexpected, total reads: "
									+ totalReads);
						}
						totalReads += rdlen;
					}
				} finally {
					instr.close();
				}

			} finally {
				raf.close();
			}

		} finally {
			sourceFile.delete();
		}
	}
	
	/*
	 * The idea is to write a large file and then read asynchronously across a large number of small buffers.
	 * 
	 */
	public void no_testAsyncReadersCancelled() throws IOException, InterruptedException {
		final Random r = new Random();

		final File sourceFile = new File("/Volumes/NonSSD/bigdata/interrupted.jnl"); // External non-SSD drive
		

		final RandomAccessFile raf = new RandomAccessFile(sourceFile, "rw");
		
		// Now let's read 50M randomly from the file
		final byte[] buf = new byte[50*1024*1024];
		long addr = 0;
		final ArrayList<AsyncTransfer> transfers = new ArrayList<AsyncTransfer>();
		while (addr < buf.length) { // cursor is within buffer
			// final int rdlen = r.nextInt(4096);
			final int rdlen = 4096;
			
			final ByteBuffer bb = ByteBuffer.wrap(buf, (int) addr, rdlen);
			// Thread.sleep(2);
			transfers.add(new AsyncTransfer(addr, bb));
			addr += rdlen;			
		}
		
		// Create a new Thread which will race backwards attempting to cancel the AsyncTransfer
		// this will be a NOP until it hits one with a Future, ie one that has been scheduled.
		
		final Thread canceller = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				for (int i = transfers.size()-1; i >= 0; i--) {
					transfers.get(i).cancel();
				}
			}
			
		});
		canceller.start();
		
		try {		
	        final ReopenFileChannel reopener = new ReopenFileChannel(sourceFile, raf, "rw");
			FileChannelUtility.readAllAsync(reopener, transfers);
			fail("Unexpected Success");
		} catch (final CancellationException ce) {
			// Expected
		} catch (final Exception e) {
			fail("Unexpected exception");
		}
	}
    
	/*
	 * The idea is to write a large file and then read asynchronously across a large number of small buffers.
	 * 
	 */
	public void no_testAsyncReadersCloseChannel() throws IOException, InterruptedException {
		final Random r = new Random();

		final File sourceFile = new File("/Volumes/NonSSD/bigdata/interrupted.jnl"); // External non-SSD drive
		
		final int rdlen = 1 * 4096;

		final long faddr = sourceFile.length() - rdlen;

		final RandomAccessFile raf = new RandomAccessFile(sourceFile, "rw");
		
		// Now let's read 50M randomly from the file
		final byte[] buf = new byte[5*1024*1024];
		long addr = 0;
		long readAddr = 0;
		final ArrayList<AsyncTransfer> transfers = new ArrayList<AsyncTransfer>();
		while (addr < buf.length) { // cursor is within buffer
			// final int rdlen = r.nextInt(4096);
			
			final ByteBuffer bb = ByteBuffer.wrap(buf, (int) addr, rdlen);
			// Thread.sleep(2);
			transfers.add(new AsyncTransfer(readAddr, bb));
			readAddr = r.nextLong() % faddr;	
			if (readAddr < 0) {
				readAddr = -readAddr;
			}
			addr += rdlen;
		}
		
		// Create a new Thread which will race backwards attempting to cancel the AsyncTransfer
		// this will be a NOP until it hits one with a Future, ie one that has been scheduled.
		
        final ReopenFileChannel reopener = new ReopenFileChannel(sourceFile, raf, "rw");
        
        final AtomicInteger closes = new AtomicInteger(0);
 
        final Thread closer = new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
				    try {
						reopener.getAsyncChannel().close();
						System.out.println("File Close: " + closes.get());
						closes.incrementAndGet();
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						// expected
						return;
					}
				}
			}
			
		});
        closer.start();
		
		try {		
			FileChannelUtility.readAllAsync(reopener, transfers);
			// expected success
			closer.interrupt();
			
			if (closes.get() == 0) {
				fail("No closes");
			}
			
			log.info("File Closes: " + closes.get());
			System.out.println("File Closes: " + closes.get());
		} catch (final Exception e) {
			e.printStackTrace();
			fail("Unexpected exception");
		}
	}
	/*
	 * ReopenFileChannel similar to RWStore class
	 */
	private class ReopenFileChannel implements IReopenChannel<FileChannel>,
			FileChannelUtility.IAsyncOpener {

		final private File file;

		private final String mode;

		private volatile RandomAccessFile raf;

		private final Path path;

		private volatile AsynchronousFileChannel asyncChannel;

		private int asyncChannelOpenCount = 0;;

		public ReopenFileChannel(final File file, final RandomAccessFile raf,
				final String mode) throws IOException {

			this.file = file;

			this.mode = mode;

			this.raf = raf;

			this.path = Paths.get(file.getAbsolutePath());

			reopenChannel();

		}

		public AsynchronousFileChannel getAsyncChannel() {
			if (asyncChannel != null) {
				if (asyncChannel.isOpen())
					return asyncChannel;
			}

			synchronized (this) {
				if (asyncChannel != null) { // check again while synchronized
					if (asyncChannel.isOpen())
						return asyncChannel;
				}

				try {
					asyncChannel = AsynchronousFileChannel.open(path,
							StandardOpenOption.READ);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				asyncChannelOpenCount++;

				return asyncChannel;
			}
		}

		public int getAsyncChannelOpenCount() {
			return asyncChannelOpenCount;
		}

		public String toString() {

			return file.toString();

		}

		public FileChannel reopenChannel() throws IOException {

			/*
			 * Note: This is basically a double-checked locking pattern. It is
			 * used to avoid synchronizing when the backing channel is already
			 * open.
			 */
			{
				final RandomAccessFile tmp = raf;
				if (tmp != null) {
					final FileChannel channel = tmp.getChannel();
					if (channel.isOpen()) {
						// The channel is still open.
						return channel;
					}
				}
			}

			synchronized (this) {

				if (raf != null) {
					final FileChannel channel = raf.getChannel();
					if (channel.isOpen()) {
						/*
						 * The channel is still open. If you are allowing
						 * concurrent reads on the channel, then this could
						 * indicate that two readers each found the channel
						 * closed and that one was able to re-open the channel
						 * before the other such that the channel was open again
						 * by the time the 2nd reader got here.
						 */
						return channel;
					}
				}

				// open the file.
				this.raf = new RandomAccessFile(file, mode);
				return raf.getChannel();

			}

		}

	}
}
