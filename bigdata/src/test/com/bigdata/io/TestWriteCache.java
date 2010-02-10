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
/*
 * Created on Feb 10, 2010
 */

package com.bigdata.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rwstore.RWStore;

import junit.framework.TestCase2;

/**
 * Test suite for the {@link WriteCache}.
 * <p>
 * Note: This test suite uses the {@link DirectBufferPool} to allocate its
 * buffers. This reduces the likelihood that direct buffers will be leaked
 * across the unit tests. The tests are written defensively to release the
 * {@link ByteBuffer}s back to the {@link DirectBufferPool}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test concurrent readers and a single writer.
 * 
 * @todo Build out write cache service for WORM (serialize writes on the cache
 *       but the caller does not wait for the IO and readers are non-blocking).
 *       A pool of write cache instances should be used. Readers should check
 *       the current write cache and also each non-recycled write cache with
 *       writes on it in the pool. Write caches remain available to readers
 *       until they need to be recycled as the current write cache (the one
 *       servicing new writes). The write cache services needs to maintain a
 *       dirty list of write cache instances. A single thread will handle writes
 *       onto the disk. When the caller calls flush() on the write cache service
 *       it flush() the current write cache (if dirty) to the dirty list and
 *       then wait until the specific write cache instances now on the dirty
 *       list have been serviced (new writes MAY continue asynchronously).
 * 
 * @todo Build out write cache service for RW. The salient differences here is
 *       gathered writes on the store. Note that writers do not have more
 *       concurrency since that bit is still synchronized inside of
 *       {@link WriteCache#write(long, ByteBuffer)} and the {@link RWStore}
 *       serializes its allocation requests. Also, the gathering writes can
 *       combine and order records from the dirty write cache list for better
 *       efficiency. However, if it does this during flush(), then it should not
 *       combine records from write caches which are inside of the write cache
 *       set on which the flush() is waiting in order to ensure that flush() is
 *       service in a timely manner.
 * 
 * @todo The WORM (and RW) stores need to also establish a read-write lock to
 *       prevent changes in the file extent from causing corrupt data for
 *       concurrent read or write operations on the file.
 * 
 * @todo Next steps.
 *       <p>
 *       First I plan to modify IndexSegmentBuilder to use WriteCache over the
 *       output file without a leaf buffer which should reduce the IO for index
 *       segment builds by 50% and let us remove the IUpdateStore from the
 *       DiskOnlyStrategy.
 *       <p>
 *       I wonder if we can take this a step at a time without branching? The
 *       main danger point is when we allow readers (and a single write thread)
 *       to run concurrently on the store. We just need to MUTEX those
 *       conditions with file extension, and a read-write lock is exactly the
 *       tool for that job. We also need to explore whether or not (and if so,
 *       how) to queue disk reads for servicing. I would like to take a metrics
 *       based approach to that once we have concurrent readers. I expect that
 *       performance could be very good on a server grade IO bus such as the
 *       cluster machines. The SAS should already handle the reordering of
 *       concurrent reads. However, it is clear that the SATA (non-SCSI) bus is
 *       not as good at this, so maybe handling in s/w makes sense for non-SCSI
 *       disks?
 */
public class TestWriteCache extends TestCase2 {

    /**
     * 
     */
    public TestWriteCache() {
    }

    /**
     * @param name
     */
    public TestWriteCache(String name) {
        super(name);
    }

    /**
     * The file mode. We generally use 'rw' and then explicitly force the file
     * contents and metadata to the disk as necessary.
     */
    final private static String mode = "rw";

    /**
     * Exercises most of the API.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void test_writeCache01() throws IOException, InterruptedException {

        /*
         * The offset into the file of the start of the "user" data. May be GTE
         * zero.
         */
        final long baseOffset = 0L;

        /*
         * Note: We need to assign the addresses in strictly increasing order
         * with the just like a WORM store with a known header length so we can
         * read the data back from the file when we test flush through to the
         * disk and read back below.
         */
        final AtomicLong _addr = new AtomicLong(baseOffset);
        
        final File file = File.createTempFile(getName(), ".tmp");

        try {

            final ReopenFileChannel opener = new ReopenFileChannel(file, mode);

            final ByteBuffer buf = DirectBufferPool.INSTANCE.acquire();

            try {

                // The buffer size must be at least 1k for these tests.
                assertTrue(DirectBufferPool.INSTANCE.getBufferCapacity() >= Bytes.kilobyte32);

                // ctor correct rejection tests: baseOffset is negative.
                try {
                    new WriteCache.FileChannelWriteCache(-1L, buf, opener);
                    fail("Expected: " + IllegalArgumentException.class);
                } catch (IllegalArgumentException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: " + ex);
                }

                // ctor correct rejection tests: opener is null.
                try {
                    new WriteCache.FileChannelWriteCache(baseOffset, buf, null/* opener */);
                    fail("Expected: " + IllegalArgumentException.class);
                } catch (IllegalArgumentException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: " + ex);
                }

                // allocate write cache using our buffer.
                final WriteCache<FileChannel> writeCache = new WriteCache.FileChannelWriteCache(
                        baseOffset, buf, opener);

                // verify the write cache self-reported capacity.
                assertEquals(DirectBufferPool.INSTANCE.getBufferCapacity(),
                        writeCache.capacity());

                // correct rejection test for null write.
                try {
                    writeCache.write(1000L, null);
                    fail("Expecting: " + IllegalArgumentException.class);
                } catch (IllegalArgumentException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: " + ex);
                }

                // correct rejection test for empty write.
                try {
                    writeCache.write(1000L, ByteBuffer.allocate(0));
                    fail("Expecting: " + IllegalArgumentException.class);
                } catch (IllegalArgumentException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: " + ex);
                }

                /*
                 * Correct rejection test for empty write (capacity>0 but still
                 * empty).
                 */
                try {
                    final ByteBuffer data = ByteBuffer.allocate(10);
                    data.position(0);
                    data.limit(0);
                    writeCache.write(1000L, data);
                    fail("Expecting: " + IllegalArgumentException.class);
                } catch (IllegalArgumentException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: " + ex);
                }

                // Test successful write on the cache and immediate read back.
                final ByteBuffer data1 = getRandomData();
                final long addr1 = _addr.get();
                _addr.addAndGet(data1.capacity());
                {
                    assertEquals(-1L,writeCache.getFirstAddr());
                    // verify addr not found before write.
                    assertNull(writeCache.read(addr1));
                    // write record @ addr.
                    assertTrue(writeCache.write(addr1, data1));
                    // verify record @ addr can be read.
                    assertNotNull(writeCache.read(addr1));
                    // verify data read back @ addr.
                    assertEquals(data1, writeCache.read(addr1));
                    // verify address set after 1st write.
                    assertEquals(addr1,writeCache.getFirstAddr());
                }

                /*
                 * Test successful write on the cache with 0L addr and immediate
                 * read back. This verifies that the cache may be used with a
                 * store which does not reserve any space for root blocks, etc.
                 */
                final ByteBuffer data2 = getRandomData();
                final long addr2 = _addr.get();
                _addr.addAndGet(data2.capacity());
                {
                    // verify addr not found before write.
                    assertNull(writeCache.read(addr2));
                    // write record @ addr.
                    assertTrue(writeCache.write(addr2, data2));
                    // verify record @ addr can be read.
                    assertNotNull(writeCache.read(addr2));
                    // verify data read back @ addr.
                    assertEquals(data2, writeCache.read(addr2));
                    // Verify the first record can still be read back.
                    assertEquals(data1, writeCache.read(addr1));
                    // verify address still set after 2nd write.
                    assertEquals(addr1, writeCache.getFirstAddr());
                }

                /*
                 * Verify that update of a record not found in the cache returns
                 * false to indicate a cache miss.
                 */
                {
                    final long addr3 = _addr.get();
                    assertFalse(writeCache.update(addr3, 0/* off */, ByteBuffer
                            .allocate(10)));
                }

                // Correct rejection error if offset is negative.
                {
                    try {
                        assertFalse(writeCache.update(addr1, -1/* off */,
                                ByteBuffer.allocate(10)));
                        fail("Expecting: " + IllegalArgumentException.class);
                    } catch (IllegalArgumentException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }
                }

                /*
                 * Correct rejection error if offset is GTE the record length
                 * (a zero update length is illegal, hence GTE).
                 */
                {
                    // EQ
                    try {
                        assertFalse(writeCache.update(addr1,
                                data1.capacity()/* off */, ByteBuffer
                                        .allocate(1)));
                        fail("Expecting: " + IllegalArgumentException.class);
                    } catch (IllegalArgumentException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }
                    // GT
                    try {
                        assertFalse(writeCache.update(addr1,
                                data1.capacity()+1/* off */, ByteBuffer
                                        .allocate(1)));
                        fail("Expecting: " + IllegalArgumentException.class);
                    } catch (IllegalArgumentException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }
                }

                /*
                 * Correct rejection error if the offset plus the update run
                 * length is GT the record length (i.e., would overrun the
                 * record's extent in the write cache's internal buffer).
                 */
                {
                    try {
                        assertFalse(writeCache.update(addr1, 1/* off */,
                                ByteBuffer.allocate(data1.capacity())));
                        fail("Expecting: " + IllegalArgumentException.class);
                    } catch (IllegalArgumentException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }
                }

                /*
                 * Write one more record on the cache whose size is known in
                 * advance. Verify read back, then update the record in the
                 * cache and re-verify read back.
                 */
                final ByteBuffer data3b;
                final long addr3 = _addr.get();
                _addr.addAndGet(5/* capacity as allocated in test below */);
                {
                    // not found yet.
                    assertNull(writeCache.read(addr3));
                    // write record of known length.
                    final ByteBuffer data3a = ByteBuffer.wrap(new byte[] { 1,
                            2, 3, 4, 5 });
                    assertTrue(writeCache.write(addr3, data3a));
                    // verify read back.
                    assertEquals(data3a, writeCache.read(addr3));
                    // update the record in the cache.
                    assertTrue(writeCache.update(addr3, 1/* off */, ByteBuffer
                            .wrap(new byte[] { -2, -3, -4 })));
                    // verify read back after update.
                    data3b = ByteBuffer.wrap(new byte[] { 1, -2, -3, -4, 5 });
                    assertEquals(data3b, writeCache.read(addr3));
                }

                /*
                 * Now flush the write cache to the backing file and verify (a)
                 * that we can still read the data from the write cache and (b)
                 * that we can now read the data from the backing file.
                 */
                {

                    // file should be empty before this.
                    assertEquals(0L, file.length());
                    
                    // write to the backing file.
                    writeCache.flush(false/* force */);

                    // verify read back of cache still good.
                    assertEquals(data1, writeCache.read(addr1));
                    assertEquals(data2, writeCache.read(addr2));
                    assertEquals(data3b, writeCache.read(addr3));

                    // verify read back from file now good.
                    assertEquals(data1, opener.read(addr1, data1.capacity()));
                    assertEquals(data2, opener.read(addr2, data2.capacity()));
                    assertEquals(data3b, opener.read(addr3, data3b.capacity()));
                    
                }

                /*
                 * FIXME Now reset the write cache and verify that (a) read back
                 * of the old records fails; (b) the firstAddr was cleared to
                 * its distinguished value; (c) that the entire capacity of the
                 * cache is now available for a large record; and (d) that
                 * flushing the cache with that record sends the new data to the
                 * end of the file such that we can read back the large record
                 * from the cache and any of the records from the file.
                 */
                {
                    
                }
                
            } finally {

                DirectBufferPool.INSTANCE.release(buf);

                opener.destroy();

            }

        } finally {

            if (file.exists() && !file.delete()) {

                log.warn("Could not delete: file=" + file);

            }

        }

    }

    /**
     * Simple implementation for a {@link RandomAccessFile} with hook for
     * deleting the test file.
     */
    private static class ReopenFileChannel implements
            IReopenChannel<FileChannel> {

        final private File file;

        private final String mode;

        private volatile RandomAccessFile raf;

        public ReopenFileChannel(final File file, final String mode)
                throws IOException {

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
                if (!file.delete())
                    log.warn("Could not delete file: " + file);
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
        public ByteBuffer read(final long off, final int nbytes)
                throws IOException {

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

            if (log.isInfoEnabled())
                log.info("(Re-)opened file: " + file);

            return raf.getChannel();

        }

    };

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

        final byte[] bytes = new byte[nbytes];

        r.nextBytes(bytes);

        return ByteBuffer.wrap(bytes);

    }
    
    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
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
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
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

}
