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
/*
 * Created on Feb 10, 2010
 */

package com.bigdata.io.writecache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;

import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.TestCase3;
import com.bigdata.io.writecache.TestWORMWriteCacheService.MyMockQuorumMember;
import com.bigdata.quorum.MockQuorumFixture;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.rwstore.RWWriteCacheService;

/**
 * Test suite for the {@link WriteCacheService} using scattered writes on a
 * backing file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestWriteCacheService.java 2866 2010-05-18 18:36:35Z
 *          thompsonbry $
 */
public class TestRWWriteCacheService extends TestCase3 {

    /**
     * 
     */
    public TestRWWriteCacheService() {
    }

    /**
     * @param name
     */
    public TestRWWriteCacheService(String name) {
        super(name);
    }

    // /**
    // *
    // * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
    // Thompson</a>
    // * @version $Id: TestRWWriteCacheService.java 4532 2011-05-20 16:06:11Z
    // thompsonbry $
    // * @param <S>
    // */
    // static private class MyMockQuorumMember<S extends HAPipelineGlue> extends
    // AbstractQuorumMember<S> {
    //
    // /**
    // * @param quorum
    // */
    // protected MyMockQuorumMember() {
    //
    // super(UUID.randomUUID());
    //
    // }
    //
    // @Override
    // public S getService(UUID serviceId) {
    // throw new UnsupportedOperationException();
    // }
    //
    // public Executor getExecutor() {
    // throw new UnsupportedOperationException();
    // }
    //
    // public S getService() {
    // throw new UnsupportedOperationException();
    // }
    //
    // }
    final int k = 1; // no write on pipeline
    MockQuorumFixture fixture = null;
    String logicalServiceId = null;
    MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = null;
    File file = null;
    ReopenFileChannel opener = null;
    RWWriteCacheService writeCache = null;

    protected void setUp() throws Exception {
        fixture = new MockQuorumFixture();
        logicalServiceId = "logicalService_" + getName();
        quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        file = null;
        opener = null;
        writeCache = null;
        fixture.start();

        quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,
                logicalServiceId));
        final QuorumActor<?, ?> actor = quorum.getActor();

        actor.memberAdd();
        fixture.awaitDeque();

        actor.pipelineAdd();
        fixture.awaitDeque();

        actor.castVote(0);
        fixture.awaitDeque();

        // Await quorum meet.
        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals(0L, quorum.token());
                } catch (Exception e) {
                    fail();
                }
            }

        }, 5000/*timeout*/, TimeUnit.MILLISECONDS);

        file = File.createTempFile(getName(), ".rw.tmp");

        opener = new ReopenFileChannel(file, "rw");

        final long fileExtent = opener.reopenChannel().size();

        final boolean prefixWrites = true;

        final int compactionThreshold = 30;

        final int hotCacheThreshold = 1;

        writeCache = new RWWriteCacheService(120/* nbuffers */,
                5/* minCleanListSize */, 5/*readCacheSize*/, prefixWrites, compactionThreshold,
                0/*hotCacheSize*/, hotCacheThreshold,
                fileExtent, opener, quorum, null);

    }

    protected void tearDown() throws Exception {
        if (writeCache != null)
            writeCache.close();

        if (opener != null) {
            opener.destroy();
        }
        quorum.terminate();
        fixture.terminate();

        super.tearDown();
    }

    public void test_simpleRWService() throws IOException, InterruptedException {

        writeCache.close();
        writeCache = null;

    }

    public void test_simpleDataRWService() throws IOException {
        try {
            final ByteBuffer data1 = getRandomData();
            final long addr1 = 2048;
            {
                assertNull(writeCache.read(addr1, data1.capacity()));
                // write record @ addr.
                assertTrue(writeCache.write(addr1, data1.asReadOnlyBuffer(),
                        ChecksumUtility.threadChk.get().checksum(data1)));
                // verify record @ addr can be read.
                assertNotNull(writeCache.read(addr1, data1.capacity()));
                // verify data read back @ addr.
                data1.position(0);
                assertEquals(data1, writeCache.read(addr1, data1.capacity()));
            }
        } catch (Exception e) {
            fail("Unexpected  Exception", e);
        }
    }

    private void randomizeArray(ArrayList<AllocView> allocs) {
    	final int slots = allocs.size();
        for (int i = 0; i < slots/2; i++) {
            int swap1 = r.nextInt(slots);
            int swap2 = r.nextInt(slots);
            AllocView v1 = allocs.get(swap1);
            AllocView v2 = allocs.get(swap2);
            allocs.set(swap1, v2);
            allocs.set(swap2, v1);
        }
    }

    public void test_stressDataRWService() throws InterruptedException,
            IOException {
        /*
         * Whether or nor the write cache will force writes to the disk. For
         * this test, force is false since it just does not matter whether the
         * data are restart safe.
         */
        final boolean force = false;

        /*
         * We will create a list of Random 0-1024 byte writes by creating single
         * random buffer of 2K and generating random views of differing
         * positions and lengths
         */
        final ByteBuffer srcBuf = getRandomData(4096);
        
        final int totalAllocs = 10000;

        ArrayList<AllocView> allocs = new ArrayList<AllocView>();
        int curAddr = 0;
        for (int i = 0; i < totalAllocs; i++) {
            int pos = r.nextInt(3072);
            int size = r.nextInt(1023) + 1;
            allocs.add(new AllocView(curAddr, pos, size, srcBuf));
            curAddr += (size + 4); // include space for chk;
        }

        final ChecksumUtility checker = new ChecksumUtility();

        // Now randomize the array for writing
        randomizeArray(allocs);

        /*
         * First write 500 records into the cache and confirm they can all be
         * read okay
         */
        for (int i = 0; i < 500; i++) {
            AllocView v = allocs.get(i);
            writeCache.write(v.addr, v.buf.asReadOnlyBuffer(), checker
                    .checksum(v.buf));
            v.buf.position(0);
        }
        for (int i = 0; i < 500; i++) {
            AllocView v = allocs.get(i);
            assertEquals(v.buf, writeCache.read(v.addr, v.nbytes)); // expected,
            // actual
        }
        /*
         * Flush to disk and reset the cache
         */
        writeCache.flush(true);
        /*
         * Now confirm that data is in the cache AND on disk
         */
        for (int i = 0; i < 500; i++) {
            AllocView v = allocs.get(i);
            assertEquals(v.buf, writeCache.read(v.addr, v.nbytes));
        }
        for (int i = 0; i < 500; i++) {
            AllocView v = allocs.get(i);
            assertEquals(v.buf, opener.read(v.addr, v.nbytes)); // expected,
            // actual
            // on
            // DISK
        }
        /*
         * Now add further 500 writes, flush and read full 1000 from disk
         */
        for (int i = 500; i < 1000; i++) {
            AllocView v = allocs.get(i);
            writeCache.write(v.addr, v.buf.asReadOnlyBuffer(), checker
                    .checksum(v.buf));
            v.buf.position(0);
        }
        writeCache.flush(true);
        for (int i = 0; i < 1000; i++) {
            AllocView v = allocs.get(i);
            try {
                assertEquals(v.buf, opener.read(v.addr, v.buf.capacity())); // expected,
                // actual
            } catch (AssertionFailedError e) {
                System.err.println("ERROR: i=" + i + ", v=" + v.buf);
                throw e;
            }
        }
        /*
         * Now write remaining records, checking for write success
         * and if fail then flush/reset and resubmit, asserting that
         * resubmission is successful
         */
        // writeCache.reset();
        for (int i = 1000; i < totalAllocs; i++) {
            AllocView v = allocs.get(i);
            if (!writeCache.write(v.addr, v.buf.asReadOnlyBuffer(), checker
                    .checksum(v.buf))) {
                log.info("flushing and resetting writeCache");
                writeCache.flush(false);
                // writeCache.reset();
                assertTrue(writeCache.write(v.addr, v.buf.asReadOnlyBuffer(),
                        checker.checksum(v.buf)));
            }
            v.buf.position(0);
        }
        /*
         * Now flush and check if we can read in all records
         */
        writeCache.flush(true);
        for (int i = 0; i < totalAllocs; i++) {
            AllocView v = allocs.get(i);
            assertEquals(v.buf, opener.read(v.addr, v.buf.capacity())); // expected,
            // actual
        }

        /*
         * Now reset, reshuffle and write full 10000 records, checking for write
         * success and if fail then flush/reset and resubmit, asserting that
         * resubmission is successful
         */        
        for (int i = 0; i < totalAllocs; i++) {
            AllocView v = allocs.get(i);
        	// must ensure any existing write is removed first (alternative to resetting the cache)
        	writeCache.clearWrite(v.addr, 0);
        }

        randomizeArray(allocs);
        int duplicates = 0;
        for (int i = 0; i < totalAllocs; i++) {
            AllocView v = allocs.get(i);
            v.buf.reset();

            // Any exception is an error!
            writeCache.write(v.addr, v.buf.asReadOnlyBuffer(), checker
                    .checksum(v.buf));
        }

        /*
         * Now flush and check if we can read in all records
         */
        writeCache.flush(true);
        for (int i = 0; i < totalAllocs; i++) {
            AllocView v = allocs.get(i);
            v.buf.position(0);
            assertEquals(v.buf, opener.read(v.addr, v.buf.capacity())); // expected,
            // actual
        }
    }

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
                log.error(e, e);
            }
            if (!file.delete())
                log.warn("Could not delete file: " + file);
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

            // flip for reading.
            tmp.flip();

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

    /*
     * Now generate randomviews, first an ordered view of 10000 random lengths
     */
    class AllocView {
        int addr;
        ByteBuffer buf;
        final int nbytes;

        AllocView(int pa, int pos, int limit, ByteBuffer src) {
            addr = pa;
            // ByteBuffer vbuf = src.duplicate();
            // vbuf.position(pos);
            // vbuf.limit(pos + limit);
            // buf = ByteBuffer.allocate(limit);
            // copy the data into [dst].
            // buf.put(vbuf);

            buf = getRandomData(limit);
            buf.mark();
            nbytes = buf.capacity();
        }
    };

    /**
     * Test simple compaction of WriteCache
     * @throws InterruptedException 
     */
    public void testCompactingCopy() throws InterruptedException {
        final WriteCache cache1 = writeCache.newWriteCache(null/* buf */,
                true, false/* bufferHasData */, opener, 0);

        final WriteCache cache2 = writeCache.newWriteCache(null/* buf */,
                true, false/* bufferHasData */, opener, 0);

        final ByteBuffer data1 = getRandomData();
        final ByteBuffer data2 = getRandomData();
        final ByteBuffer data3 = getRandomData();
        final long addr1 = 2048;
        final long addr2 = 12598;
        final long addr3 = 512800;
        try {
            assertNull(cache1.read(addr1, data1.capacity()));
            // write record @ addr.
            assertTrue(cache1.write(addr1, data1.asReadOnlyBuffer(),
                    ChecksumUtility.threadChk.get().checksum(data1)));
            assertTrue(cache1.write(addr2, data2.asReadOnlyBuffer(),
                    ChecksumUtility.threadChk.get().checksum(data2)));
            assertTrue(cache1.write(addr3, data3.asReadOnlyBuffer(),
                    ChecksumUtility.threadChk.get().checksum(data3)));
            // verify record @ addr can be read.
            assertNotNull(cache1.read(addr1, data1.capacity()));
            assertNotNull(cache1.read(addr2, data2.capacity()));
            assertNotNull(cache1.read(addr3, data3.capacity()));
            
            cache1.clearAddrMap(addr2, 0);

            WriteCache.transferTo(cache1, cache2, null, 0);

            assertNull(cache1.read(addr1, data1.capacity()));
            assertNotNull(cache2.read(addr1, data1.capacity()));
            assertNull(cache2.read(addr2, data2.capacity()));

            // verify data read back @ addr.
            data1.position(0);
            assertEquals(data1, cache2.read(addr1, data1.capacity()));

            // now go back the other way
            cache1.reset();
            WriteCache.transferTo(cache2, cache1, null, 0);

            data1.position(0);
            assertEquals(data1, cache1.read(addr1, data1.capacity()));

            data2.position(0);
            assertEquals(data3, cache1.read(addr3, data3.capacity()));
            
            
        } finally {
            // Release explicitly created caches
            cache1.close();
            cache2.close();
        }
    }
    
    public void testFullCompactingCopy() throws InterruptedException {
        final WriteCache src = writeCache.newWriteCache(null, true, false, opener, 0);
        final WriteCache dst = writeCache.newWriteCache(null, true, false, opener, 0);
        try {
            boolean notFull = true;
            long addr = 0;
            while (notFull) {
                final ByteBuffer data = getRandomData(r.nextInt(250)+1);
                notFull = src.write(addr, data.asReadOnlyBuffer(),
                        ChecksumUtility.threadChk.get().checksum(data));
                
                addr += 1000;
            }
            ;

            assertTrue(WriteCache.transferTo(src, dst, null, 0));
        } finally {
            src.close();
            dst.close();
        }
    }
    
    public void testSingleCompactingCopy() throws InterruptedException {
        final WriteCache src = writeCache.newWriteCache(null, true, false, opener, 0);
        final WriteCache dst = writeCache.newWriteCache(null, true, false, opener, 0);
        
        try {
            final long addr = 1000;
            final ByteBuffer data = getRandomData(r.nextInt(250)+1);
            src.write(addr, data.asReadOnlyBuffer(),
                    ChecksumUtility.threadChk.get().checksum(data));
                
            assertNotNull(src.read(addr, data.capacity()));

            final int sb = src.bytesWritten();
            assertTrue(WriteCache.transferTo(src, dst, null, 0));

            final int db = dst.bytesWritten();
            assertTrue(sb == db);
            
            assertNotNull(dst.read(addr, data.capacity()));
            
        } finally {
            src.close();
            dst.close();
        }
    }
    
    /**
     * 1) Creates five WriteCaches and writes until four full.
     * 
     * 2) Randomly removes half the writes.
     * 
     * 3) The compacts into final and newly emptied caches.
     */
    public void testStressCompactingCopy() throws InterruptedException {
        // five src caches
        final WriteCache[] srccaches = new WriteCache[] {
                writeCache.newWriteCache(null, true, false, opener, 0),
                writeCache.newWriteCache(null, true, false, opener, 0),
                writeCache.newWriteCache(null, true, false, opener, 0),
                writeCache.newWriteCache(null, true, false, opener, 0),
                writeCache.newWriteCache(null, true, false, opener, 0),
        };
        // single compacting cache, then space for newly clean caches to be
        //  moved.
        final WriteCache[] dstcaches = new WriteCache[] {
                writeCache.newWriteCache(null, true, false, opener, 0),
                null,
                null,
                null,
                null,
                null,
        };
        
        try {
            final HashMap<Long, WriteCache> map = new HashMap<Long, WriteCache>();
            long addr = 0;
            for (WriteCache src : srccaches) {
                boolean notFull = true;
                while (notFull) {
                    final ByteBuffer data = getRandomData();
                    notFull = src.write(addr, data.asReadOnlyBuffer(),
                            ChecksumUtility.threadChk.get().checksum(data));
                    if (notFull)
                        map.put(addr, src);
                    
                    addr += 1000;
                }
            }
            
            // okay source buffers are full
            // now clear every other write using map
            for (Entry<Long, WriteCache> entry : map.entrySet()) {
                final long k = entry.getKey();
                // clear every other address
                if ((k % 2000) == 0)
                    entry.getValue().clearAddrMap(k, 0);
            }
            
            int dstIndex = 0;
            int dstFree = 1;
            for (WriteCache src : srccaches) {
                boolean done = false;
                while (!done) {
                    done = WriteCache.transferTo(src, dstcaches[dstIndex], null, 0);
                    if (!done)
                        dstIndex++;
                }
                
                // Now add src to dstcaches, available to be used for compaction
                src.reset();
                dstcaches[dstFree++] = src;
            }
            
        } finally {
            // Release explicitly created caches
            for (WriteCache cache : dstcaches) {
                if (cache != null)
                    cache.close();
            }
            for (WriteCache cache : srccaches) {
                if (cache != null)
                    cache.close();
            }
        }
    }
}
