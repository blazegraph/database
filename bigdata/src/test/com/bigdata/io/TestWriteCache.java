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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase2;

import com.bigdata.rawstore.Bytes;

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
         * Whether or nor the write cache will force writes to the disk. For
         * this test, force is false since it just does not matter whether the
         * data are restart safe.
         */
        final boolean force = false;
        
        /*
         * The offset into the file of the start of the "user" data. May be GTE
         * zero.
         */
        final long baseOffset = 0L;

        /*
         * Note: We need to assign the addresses in strictly increasing order
         * with the just like a WORM store with a known header length so we can
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
                final WriteCache writeCache = new WriteCache.FileChannelWriteCache(
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
                    assertEquals(-1L,writeCache.getFirstOffset());
                    // verify addr not found before write.
                    assertNull(writeCache.read(addr1));
                    // write record @ addr.
                    assertTrue(writeCache.write(addr1, data1));
                    // verify record @ addr can be read.
                    assertNotNull(writeCache.read(addr1));
                    // verify data read back @ addr.
                    assertEquals(data1, writeCache.read(addr1));
                    // verify address set after 1st write.
                    assertEquals(addr1,writeCache.getFirstOffset());
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
                    assertEquals(addr1, writeCache.getFirstOffset());
                }

//                /*
//                 * Verify that update of a record not found in the cache returns
//                 * false to indicate a cache miss.
//                 */
//                {
//                    final long addr3 = _addr.get();
//                    assertFalse(writeCache.update(addr3, 0/* off */, ByteBuffer
//                            .allocate(10)));
//                }
//
//                // Correct rejection error if offset is negative.
//                {
//                    try {
//                        assertFalse(writeCache.update(addr1, -1/* off */,
//                                ByteBuffer.allocate(10)));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                }
//
//                /*
//                 * Correct rejection error if offset is GTE the record length
//                 * (a zero update length is illegal, hence GTE).
//                 */
//                {
//                    // EQ
//                    try {
//                        assertFalse(writeCache.update(addr1,
//                                data1.capacity()/* off */, ByteBuffer
//                                        .allocate(1)));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                    // GT
//                    try {
//                        assertFalse(writeCache.update(addr1,
//                                data1.capacity()+1/* off */, ByteBuffer
//                                        .allocate(1)));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                }
//
//                /*
//                 * Correct rejection error if the offset plus the update run
//                 * length is GT the record length (i.e., would overrun the
//                 * record's extent in the write cache's internal buffer).
//                 */
//                {
//                    try {
//                        assertFalse(writeCache.update(addr1, 1/* off */,
//                                ByteBuffer.allocate(data1.capacity())));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                }

//                /*
//                 * Write one more record on the cache whose size is known in
//                 * advance. Verify read back, then update the record in the
//                 * cache and re-verify read back.
//                 */
//                final ByteBuffer data3b;
//                final long addr3 = _addr.get();
//                _addr.addAndGet(5/* capacity as allocated in test below */);
//                {
//                    // not found yet.
//                    assertNull(writeCache.read(addr3));
//                    // write record of known length.
//                    final ByteBuffer data3a = ByteBuffer.wrap(new byte[] { 1,
//                            2, 3, 4, 5 });
//                    assertTrue(writeCache.write(addr3, data3a));
//                    // verify read back.
//                    assertEquals(data3a, writeCache.read(addr3));
//                    // update the record in the cache.
//                    assertTrue(writeCache.update(addr3, 1/* off */, ByteBuffer
//                            .wrap(new byte[] { -2, -3, -4 })));
//                    // verify read back after update.
//                    data3b = ByteBuffer.wrap(new byte[] { 1, -2, -3, -4, 5 });
//                    assertEquals(data3b, writeCache.read(addr3));
//                }

                /*
                 * Now flush the write cache to the backing file and verify (a)
                 * that we can still read the data from the write cache and (b)
                 * that we can now read the data from the backing file.
                 */
                {

                    // file should be empty before this.
                    assertEquals(0L, file.length());
                    
                    // write to the backing file.
                    writeCache.flush(force);

                    // verify read back of cache still good.
                    assertEquals(data1, writeCache.read(addr1));
                    assertEquals(data2, writeCache.read(addr2));
//                    assertEquals(data3b, writeCache.read(addr3));

                    // verify read back from file now good.
                    assertEquals(data1, opener.read(addr1, data1.capacity()));
                    assertEquals(data2, opener.read(addr2, data2.capacity()));
//                    assertEquals(data3b, opener.read(addr3, data3b.capacity()));
                    
                }

                /*
                 * Now reset the write cache and verify that (a) the firstAddr
                 * was cleared to its distinguished value; (b) read back of the
                 * old records fails; (c) that the entire capacity of the cache
                 * is now available for a large record; and (d) that flushing
                 * the cache with that record sends the new data to the end of
                 * the file such that we can read back the large record from the
                 * cache and any of the records from the file.
                 */
                // exact file record for the cache.
                final ByteBuffer data4 = getRandomData(writeCache.capacity());
                final long addr4 = _addr.get();
                _addr.addAndGet(data4.capacity());
                {
                 
                    writeCache.reset();
                    
                    assertEquals(-1L,writeCache.getFirstOffset());

                    // verify read back of cache fails.
                    assertNull(writeCache.read(addr1));
                    assertNull(writeCache.read(addr2));
//                    assertNull(writeCache.read(addr3));

                    // verify read back from file still good.
                    assertEquals(data1, opener.read(addr1, data1.capacity()));
                    assertEquals(data2, opener.read(addr2, data2.capacity()));
//                    assertEquals(data3b, opener.read(addr3, data3b.capacity()));

                    // write record on the cache.
                    assertTrue(writeCache.write(addr4, data4));

                    // verify read back.
                    assertEquals(data4, writeCache.read(addr4));

                    // Verify no more writes are allowed on the cache (it is
                    // full).
                    assertFalse(writeCache.write(addr4 + 1, ByteBuffer
                            .wrap(new byte[] { 1 })));

                    // write on the disk.
                    writeCache.flush(force);

                    // verify read back of cache for other records still fails.
                    assertNull(writeCache.read(addr1));
                    assertNull(writeCache.read(addr2));
//                    assertNull(writeCache.read(addr3));
                    // verify read back from cache of the last record written.
                    assertEquals(data4, writeCache.read(addr4));

                    // verify read back from file still good.
                    assertEquals(data1, opener.read(addr1, data1.capacity()));
                    assertEquals(data2, opener.read(addr2, data2.capacity()));
//                    assertEquals(data3b, opener.read(addr3, data3b.capacity()));
                    assertEquals(data4, opener.read(addr4, data4.capacity()));


                }

                /*
                 * Test close() [verify API throws IllegalStateException].
                 */
                {

                    // close this instance.
                    writeCache.close();

                    // read fails.
                    try {
                        writeCache.read(1L/*addr*/);
                        fail("Expected: " + IllegalStateException.class);
                    } catch (IllegalStateException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }

                    // write fails.
                    try {
                        writeCache.write(1L/* addr */, ByteBuffer
                                .wrap(new byte[] { 1, 2, 3 }));
                        fail("Expected: " + IllegalStateException.class);
                    } catch (IllegalStateException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }

                    // does not throw an exception.
                    writeCache.close();
                    
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
     * Similar to writeCache01 but uses the ScatteredWrite. It uses the same simple random
     * data but writes on explicit out of order 1K boundaries.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void test_writeCacheScatteredWrites() throws IOException, InterruptedException {

        /*
         * Whether or nor the write cache will force writes to the disk. For
         * this test, force is false since it just does not matter whether the
         * data are restart safe.
         */
        final boolean force = false;
        
        /*
         * The offset into the file of the start of the "user" data. May be GTE
         * zero.
         */
        final long baseOffset = 0L;

        /*
         * Note: For the scattered writes we need a non-sequential order to mimic the
         * allocation of the RWStore.
         */
        long _addr[] = {4096,1024,3072,8192};
        
        final File file = File.createTempFile(getName(), ".tmp");
        

        try {

            final ReopenFileChannel opener = new ReopenFileChannel(file, mode);

            final ByteBuffer buf = DirectBufferPool.INSTANCE.acquire();
            try {

                // The buffer size must be at least 1k for these tests.
                assertTrue(DirectBufferPool.INSTANCE.getBufferCapacity() >= Bytes.kilobyte32);

                // ctor correct rejection tests: opener is null.
                try {
                    new WriteCache.FileChannelScatteredWriteCache(buf, null/* opener */);
                    fail("Expected: " + IllegalArgumentException.class);
                } catch (IllegalArgumentException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: " + ex);
                }

                // allocate write cache using our buffer.
                final WriteCache writeCache = new WriteCache.FileChannelScatteredWriteCache(
                        buf, opener);

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
                final long addr1 = _addr[0];
                {
                    assertEquals(-1L,writeCache.getFirstOffset());
                    // verify addr not found before write.
                    assertNull(writeCache.read(addr1));
                    // write record @ addr.
                    assertTrue(writeCache.write(addr1, data1));
                    // verify record @ addr can be read.
                    assertNotNull(writeCache.read(addr1));
                    // verify data read back @ addr.
                    assertEquals(data1, writeCache.read(addr1));
                    // verify address set after 1st write.
                    assertEquals(addr1,writeCache.getFirstOffset());
                }

                /*
                 * Test successful write on the cache with 0L addr and immediate
                 * read back. This verifies that the cache may be used with a
                 * store which does not reserve any space for root blocks, etc.
                 */
                final ByteBuffer data2 = getRandomData();
                final long addr2 = _addr[1];
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
                    assertEquals(addr1, writeCache.getFirstOffset());
                }

//                /*
//                 * Verify that update of a record not found in the cache returns
//                 * false to indicate a cache miss.
//                 */
//                {
//                    final long addr3 = _addr[2];
//                    assertFalse(writeCache.update(addr3, 0/* off */, ByteBuffer
//                            .allocate(10)));
//                }
//
//                // Correct rejection error if offset is negative.
//                {
//                    try {
//                        assertFalse(writeCache.update(addr1, -1/* off */,
//                                ByteBuffer.allocate(10)));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                }
//
//                /*
//                 * Correct rejection error if offset is GTE the record length
//                 * (a zero update length is illegal, hence GTE).
//                 */
//                {
//                    // EQ
//                    try {
//                        assertFalse(writeCache.update(addr1,
//                                data1.capacity()/* off */, ByteBuffer
//                                        .allocate(1)));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                    // GT
//                    try {
//                        assertFalse(writeCache.update(addr1,
//                                data1.capacity()+1/* off */, ByteBuffer
//                                        .allocate(1)));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                }
//
//                /*
//                 * Correct rejection error if the offset plus the update run
//                 * length is GT the record length (i.e., would overrun the
//                 * record's extent in the write cache's internal buffer).
//                 */
//                {
//                    try {
//                        assertFalse(writeCache.update(addr1, 1/* off */,
//                                ByteBuffer.allocate(data1.capacity())));
//                        fail("Expecting: " + IllegalArgumentException.class);
//                    } catch (IllegalArgumentException ex) {
//                        if (log.isInfoEnabled())
//                            log.info("Expected exception: " + ex);
//                    }
//                }

//                /*
//                 * Write one more record on the cache whose size is known in
//                 * advance. Verify read back, then update the record in the
//                 * cache and re-verify read back.
//                 */
//                final ByteBuffer data3b;
//                final long addr3 = _addr[2];
//                {
//                    // not found yet.
//                    assertNull(writeCache.read(addr3));
//                    // write record of known length.
//                    final ByteBuffer data3a = ByteBuffer.wrap(new byte[] { 1,
//                            2, 3, 4, 5 });
//                    assertTrue(writeCache.write(addr3, data3a));
//                    // verify read back.
//                    assertEquals(data3a, writeCache.read(addr3));
//                    // update the record in the cache.
//                    assertTrue(writeCache.update(addr3, 1/* off */, ByteBuffer
//                            .wrap(new byte[] { -2, -3, -4 })));
//                    // verify read back after update.
//                    data3b = ByteBuffer.wrap(new byte[] { 1, -2, -3, -4, 5 });
//                    assertEquals(data3b, writeCache.read(addr3));
//                }

                /*
                 * Now flush the write cache to the backing file and verify (a)
                 * that we can still read the data from the write cache and (b)
                 * that we can now read the data from the backing file.
                 */
                {

                    // file should be empty before this.
                    assertEquals(0L, file.length());
                    
                    // write to the backing file.
                    writeCache.flush(force);

                    // verify read back of cache still good.
                    assertEquals(data1, writeCache.read(addr1));
                    assertEquals(data2, writeCache.read(addr2));
//                    assertEquals(data3b, writeCache.read(addr3));

                    // verify read back from file now good.
                    assertEquals(data1, opener.read(addr1, data1.capacity()));
                    assertEquals(data2, opener.read(addr2, data2.capacity()));
//                    assertEquals(data3b, opener.read(addr3, data3b.capacity()));
                    
                }

                /*
                 * Now reset the write cache and verify that (a) the firstAddr
                 * was cleared to its distinguished value; (b) read back of the
                 * old records fails; (c) that the entire capacity of the cache
                 * is now available for a large record; and (d) that flushing
                 * the cache with that record sends the new data to the end of
                 * the file such that we can read back the large record from the
                 * cache and any of the records from the file.
                 */
                // exact file record for the cache.
                final ByteBuffer data4 = getRandomData(writeCache.capacity());
                final long addr4 = _addr[3];
                {
                 
                    writeCache.reset();
                    
                    assertEquals(-1L,writeCache.getFirstOffset());

                    // verify read back of cache fails.
                    assertNull(writeCache.read(addr1));
                    assertNull(writeCache.read(addr2));
//                    assertNull(writeCache.read(addr3));

                    // verify read back from file still good.
                    assertEquals(data1, opener.read(addr1, data1.capacity()));
                    assertEquals(data2, opener.read(addr2, data2.capacity()));
//                    assertEquals(data3b, opener.read(addr3, data3b.capacity()));

                    // write record on the cache.
                    assertTrue(writeCache.write(addr4, data4));
                    
                    assertEquals(data2, opener.read(addr2, data2.capacity()));

                    // verify read back.
                    assertEquals(data4, writeCache.read(addr4));

                    // Verify no more writes are allowed on the cache (it is
                    // full).
                    assertFalse(writeCache.write(addr4 + 1, ByteBuffer
                            .wrap(new byte[] { 1 })));

                    assertEquals(data2, opener.read(addr2, data2.capacity()));

                    // write on the disk.
                    writeCache.flush(force);

                    assertEquals(data2, opener.read(addr2, data2.capacity()));

                    // verify read back of cache for other records still fails.
                    assertNull(writeCache.read(addr1));
                    assertNull(writeCache.read(addr2));
//                    assertNull(writeCache.read(addr3));
                    // verify read back from cache of the last record written.
                    assertEquals(data4, writeCache.read(addr4));

                    // verify read back from file still good.
                    assertEquals(data1, opener.read(addr1, data1.capacity()));
                    assertEquals(data2, opener.read(addr2, data2.capacity()));
//                    assertEquals(data3b, opener.read(addr3, data3b.capacity()));
                    assertEquals(data4, opener.read(addr4, data4.capacity()));

                }

                /*
                 * Test close() [verify API throws IllegalStateException].
                 */
                {

                    // close this instance.
                    writeCache.close();

                    // read fails.
                    try {
                        writeCache.read(1L/*addr*/);
                        fail("Expected: " + IllegalStateException.class);
                    } catch (IllegalStateException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }

                    // write fails.
                    try {
                        writeCache.write(1L/* addr */, ByteBuffer
                                .wrap(new byte[] { 1, 2, 3 }));
                        fail("Expected: " + IllegalStateException.class);
                    } catch (IllegalStateException ex) {
                        if (log.isInfoEnabled())
                            log.info("Expected exception: " + ex);
                    }

                    // does not throw an exception.
                    writeCache.close();
                    
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

    /*
     * Now generate randomviews, first an ordered view of 10000 random lengths
     */
    class AllocView {
    	int addr;
    	ByteBuffer buf;
    	AllocView(int pa, int pos, int limit, ByteBuffer src) {
    		addr = pa;
//    		ByteBuffer vbuf = src.duplicate();
//    		vbuf.position(pos);
//    		vbuf.limit(pos + limit);
//    		buf = ByteBuffer.allocate(limit);
			// copy the data into [dst].
			// buf.put(vbuf);

    		buf = getRandomData(limit);
    		buf.mark();

    	}
    };

    /**
     * Generate large number of scattered writes to force flushing
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void test_writeCacheStressScatteredWrites() throws IOException, InterruptedException {

        /*
         * Whether or nor the write cache will force writes to the disk. For
         * this test, force is false since it just does not matter whether the
         * data are restart safe.
         */
        final boolean force = false;
        
        /*
         * We will create a list of Random 0-1024 byte writes by creating single random buffer
         * of 2K and generating random views of differing positions and lengths 
         */
        final ByteBuffer srcBuf = getRandomData(4096);
        
        final ByteBuffer buf = DirectBufferPool.INSTANCE.acquire();
        
        ArrayList<AllocView> allocs = new ArrayList<AllocView>();
        int curAddr = 0;
        for (int i = 0; i < 10000; i++) {
        	int pos = r.nextInt(3072);
        	int size = r.nextInt(1023)+1;
        	allocs.add(new AllocView(curAddr, pos, size, srcBuf));
        	curAddr += size;
        }
        
        // Now randomize the array for writing
        randomizeArray(allocs);
        
        final File file = File.createTempFile(getName(), ".tmp");
        

        try {

            final ReopenFileChannel opener = new ReopenFileChannel(file, mode);

            // allocate write cache using our buffer.
            final WriteCache writeCache = new WriteCache.FileChannelScatteredWriteCache(
                    buf, opener);

            /*
             * First write 500 records into the cache and confirm they can all be read okay
             */
            for (int i = 0; i < 500; i++) {
            	AllocView v = allocs.get(i);
            	writeCache.write(v.addr, v.buf);           	
            }
            for (int i = 0; i < 500; i++) {
            	AllocView v = allocs.get(i);
             	assertEquals(v.buf, writeCache.read(v.addr));     // expected, actual   	
            }
            /*
             * Flush to disk and reset the cache
             */
            writeCache.flush(true);
            writeCache.reset(); // clear cache
            /*
             * Now confirm that nothing is in cache and all on disk
             */
            for (int i = 0; i < 500; i++) {
            	AllocView v = allocs.get(i);
             	assertNull(writeCache.read(v.addr));     // should be nothing in cache   	
            }
            for (int i = 0; i < 500; i++) {
            	AllocView v = allocs.get(i);
            	assertEquals(v.buf, opener.read(v.addr, v.buf.capacity()));     // expected, actual   	
            }
            /*
             * Now add further 500 writes, flush and read full 1000 from disk
             */
            for (int i = 500; i < 1000; i++) {
            	AllocView v = allocs.get(i);
            	writeCache.write(v.addr, v.buf);           	
            }
            writeCache.flush(true);
            for (int i = 0; i < 1000; i++) {
            	AllocView v = allocs.get(i);
             	assertEquals(v.buf, opener.read(v.addr, v.buf.capacity()));     // expected, actual   	
            }
            /*
             * Now reset and write full 10000 records, checking for write success and if fail then flush/reset and
             * resubmit, asserting that resubmission is successful
             */
            writeCache.reset();
            for (int i = 1000; i < 10000; i++) {
            	AllocView v = allocs.get(i);
            	if (!writeCache.write(v.addr, v.buf)) {
            		log.info("flushing and resetting writeCache");
            		writeCache.flush(false);
            		writeCache.reset();
            		assertTrue(writeCache.write(v.addr, v.buf));
            	}
            }
            /*
             * Now flush and check if we can read in all records
             */
            writeCache.flush(true);
            for (int i = 0; i < 10000; i++) {
            	AllocView v = allocs.get(i);
             	assertEquals(v.buf, opener.read(v.addr, v.buf.capacity()));     // expected, actual   	
            }
            
            /*
             * Now reset, reshuffle and write full 10000 records, checking for write success and if fail then flush/reset and
             * resubmit, asserting that resubmission is successful
             */
            writeCache.reset();
            randomizeArray(allocs);
            for (int i = 0; i < 10000; i++) {
            	AllocView v = allocs.get(i);
            	v.buf.reset();
            	if (!writeCache.write(v.addr, v.buf)) {
            		log.info("flushing and resetting writeCache");
            		writeCache.flush(false);
            		writeCache.reset();
            		assertTrue(writeCache.write(v.addr, v.buf));
            	}
            }
            /*
             * Now flush and check if we can read in all records
             */
            writeCache.flush(true);
            for (int i = 0; i < 10000; i++) {
            	AllocView v = allocs.get(i);
             	assertEquals(v.buf, opener.read(v.addr, v.buf.capacity()));     // expected, actual   	
            }
        } finally {

            if (file.exists() && !file.delete()) {

                log.warn("Could not delete: file=" + file);

            }

        }
    }
    
    private void randomizeArray(ArrayList<AllocView> allocs) {
        for (int i = 0; i < 5000; i++) {
        	int swap1 = r.nextInt(10000);
        	int swap2 = r.nextInt(10000);
        	AllocView v1 = allocs.get(swap1);
        	AllocView v2 = allocs.get(swap2);
        	allocs.set(swap1, v2);
        	allocs.set(swap2, v1);
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

            expectedBuffer.get(expected);

        }

        // evaluate byte[] against actual.
        assertEquals(expected, actual);

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
