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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Adler32;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.journal.ha.HAGlue;
import com.bigdata.journal.ha.HAReceiveService;
import com.bigdata.journal.ha.HASendService;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.journal.ha.MockSingletonQuorumManager;
import com.bigdata.journal.ha.Quorum;
import com.bigdata.journal.ha.QuorumManager;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.ChecksumUtility;

/**
 * Test suite for the {@link WriteCacheService} using pure append writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestWriteCacheService.java 2866 2010-05-18 18:36:35Z
 *          thompsonbry $
 * 
 *          FIXME Also apply to the RW store. This will require prefix metadata.
 */
public class TestWORMWriteCacheService extends TestCase3 {

    /**
     * 
     */
    public TestWORMWriteCacheService() {
    }

    /**
     * @param name
     */
    public TestWORMWriteCacheService(String name) {
        super(name);
    }

    /**
     * The size of a {@link WriteCache} buffer.
     */
    private static int WRITE_CACHE_BUFFER_CAPACITY = DirectBufferPool.INSTANCE
            .getBufferCapacity();
    
    /*
     * Shared setup values.
     */
    
    /**
     * The #of records to write. 10k is small. 100k is reasonable.
     */
    static final int nrecs = 100000;

    /**
     * The maximum size of a normal record. The database averages 1k per record
     * (WORM) and 4-8k per record (RW).
     */
    static final int maxreclen = Bytes.kilobyte32;

    /**
     * The percentage of records which are larger than the {@link WriteCache}
     * buffer capacity. These records are written using a different code path.
     */
    static final double largeRecordRate = .001;

    /**
     * A test which looks for deadlock conditions (one buffer).
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheServiceWORM_1buffer() throws InterruptedException, IOException {

        final int nbuffers = 1;
        final boolean useChecksums = false;
        final boolean isHighlyAvailable = false;

        doWORMTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                isHighlyAvailable);

    }

    /**
     * A test which looks for starvation conditions (2 buffers).
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheServiceWORM_2buffers() throws InterruptedException, IOException {

        final int nbuffers = 2;
        final boolean useChecksums = false;
        final boolean isHighlyAvailable = false;

        doWORMTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                isHighlyAvailable);

    }

    /**
     * A high throughput configuration with record level checksums.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheServiceWORM_6buffers_recordChecksums()
            throws InterruptedException, IOException {

        final int nbuffers = 6;
        final boolean useChecksums = true;
        final boolean isHighlyAvailable = false;

        doWORMTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                isHighlyAvailable);

    }

    /**
     * A high throughput configuration with record level checksums and whole
     * buffer checksums.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheServiceWORM_6buffers_recordChecksums_wholeBufferChecksums()
            throws InterruptedException, IOException {

        final int nbuffers = 6;
        final boolean useChecksums = true;
        final boolean isHighlyAvailable = true;

        doWORMTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                isHighlyAvailable);

    }
    
//    interface WriteCacheServiceFactory {
//        
//        WriteCacheService newWriteCacheService(final int nbuffers,
//                final boolean useChecksums, final boolean isHighlyAvailable,
//                final boolean );
//
//    }
//    
//    static class WORMWriteCacheServiceFactory implements WriteCacheServiceFactory {
//
//        public WriteCacheService newWriteCacheService() {
//            
//            final QuorumManager qm = new MockSingletonQuorumManager();
//
//            return new WriteCacheService(nbuffers, useChecksums,
//                    fileExtent, opener, qm) {
//
//                @Override
//                public WriteCache newWriteCache(ByteBuffer buf,
//                        boolean useChecksum, boolean bufferHasData,
//                        IReopenChannel<? extends Channel> opener)
//                        throws InterruptedException {
//
//                    return new WORMWriteCacheImpl(0/* baseOffset */, buf,
//                            useChecksum, isHighlyAvailable, bufferHasData,
//                            (IReopenChannel<FileChannel>) opener);
//
//                }
//
//            };
//
//        }
//        
//    }

    protected void doWORMTest(final int nbuffers, final int nrecs,
            final int maxreclen, final double largeRecordRate,
            final boolean useChecksums, final boolean isHighlyAvailable)
            throws InterruptedException, IOException {

        // @todo override for replication
        final QuorumManager qm = new MockSingletonQuorumManager();

        doWORMTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                isHighlyAvailable, StoreTypeEnum.WORM, qm);

    }

    /**
     * A stress test for the {@link WriteCacheService} in the WORM mode.
     * 
     * @param nbuffers
     *            The #of {@link WriteCache} buffers.
     * @param nrecs
     *            The #of records to write.
     * @param maxreclen
     *            The maximum length of a record.
     * @param largeRecordRate
     *            The rate in [0:1) of records which will be larger than the
     *            {@link WriteCache} buffer size.
     * @param useChecksums
     *            When <code>true</code>, use record-level checksums.
     * @param isHighlyAvailable
     *            When <code>true</code>, compute the running checksums of the
     *            {@link WriteCache} as a whole.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    protected void doWORMTest(final int nbuffers, final int nrecs,
            final int maxreclen, final double largeRecordRate,
            final boolean useChecksums, final boolean isHighlyAvailable,
            final StoreTypeEnum storeType,
            final QuorumManager qm)
            throws InterruptedException, IOException {

        if (log.isInfoEnabled()) {
            log.info("\n====================================================\n"
                    + getName() + ": nbuffers=" + nbuffers + ", nrecs=" + nrecs
                    + ", maxreclen=" + maxreclen + ", largeRecordRate="
                    + largeRecordRate + ", useChecksums=" + useChecksums
                    + ", isHighlyAvailable=" + isHighlyAvailable);
        }
        
        File file = null;
        ReopenFileChannel opener = null;
        WriteCacheService writeCacheService = null;
        try {

            if (StoreTypeEnum.WORM != storeType) {

                /*
                 * The RW store is a bit different and might require a different
                 * test altogether since records are scattered not ordered (or
                 * we can just randomize their write order once generated).
                 */
                
                fail("RW not supported yet but this test suite");
                
            }
            
            file = File.createTempFile(getName(), ".worm.tmp");

            opener = new ReopenFileChannel(file, "rw");
            
            final long fileExtent = opener.reopenChannel().size();
            
            writeCacheService = new WriteCacheService(nbuffers, useChecksums,
                    fileExtent, opener, qm) {

                @Override
                public WriteCache newWriteCache(ByteBuffer buf,
                        boolean useChecksum, boolean bufferHasData,
                        IReopenChannel<? extends Channel> opener)
                        throws InterruptedException {

                    return new WORMWriteCacheImpl(0/* baseOffset */, buf,
                            useChecksum, isHighlyAvailable, bufferHasData,
                            (IReopenChannel<FileChannel>) opener);

                }

            };

            final MockRecord[] records;
            final long firstOffset = 0L;
            final long lastOffset;
            {

                // starting offset.
                long offset = firstOffset;

                // create a bunch of records.
                records = createMockRecords(offset, nrecs, maxreclen,
                        largeRecordRate, useChecksums);

                // the offset of the next possible record.
                offset += records[records.length - 1].offset
                        + records[records.length - 1].nbytes
                        + (useChecksums ? 4 : 0);

                lastOffset = offset;

            }

            /*
             * Pre-extend the file to the maximum length.
             * 
             * Note: This is done as a workaround for a JVM bug under at least
             * Windows where IOs with a concurrent file extend can lead to
             * corrupt data. In the WORMStrategy, we handle this using a
             * read/write lock to disallow IOs during a file extend. In this
             * unit test, we shortcut that logic by pre-extending the file.
             */
            {

                assertEquals("fileExtent", 0L, opener.reopenChannel().size());

                opener.truncate(lastOffset);
                
                assertEquals("fileExtent", lastOffset, opener.reopenChannel()
                        .size());
                
            }
            
            /*
             * Write the data onto the cache, which will incrementally write it
             * out onto the backing file.
             */
            // #of cache misses
            int nmiss = 0;
            int nlarge = 0;
            final ChecksumUtility checker = new ChecksumUtility();
            final long begin = System.nanoTime();
            for (int i = 0; i < records.length; i++) {

                final MockRecord rec = records[i];

                if (rec.nbytes > WRITE_CACHE_BUFFER_CAPACITY)
                    nlarge++;
                
                /*
                 * Write the record.
                 * 
                 * Note: The buffer is duplicated in order to prevent a
                 * side-effect on its position().
                 */

                writeCacheService.write(rec.offset, rec.data.asReadOnlyBuffer(),
                        rec.chksum);

                if(log.isTraceEnabled())
                    log.trace("wrote: i=" + i + ", rec=" + rec);

                {
                    // The index of some prior record.
                    final int prior = r.nextInt(i + 1);

                    final MockRecord expected = records[prior];

                    // Read on the cache.
                    ByteBuffer actual = writeCacheService.read(expected.offset);

                    final boolean cacheHit = actual != null;
                    if (actual == null) {
                        // Cache miss - read on the file.
                        nmiss++;
                        if (useChecksums) {
                            // read on file, including checksum field.
                            actual = opener.read(expected.offset,
                                    expected.nbytes + 4);
                            // get the checksum field.
                            final int chkOnDisk = actual
                                    .getInt(expected.nbytes);
                            // trim to the application data record.
                            actual.limit(expected.nbytes);
                            // verify on disk checksum against expected.
                            assertEquals("chkOnDisk", expected.chksum,
                                    chkOnDisk); 
                        } else {
                            // read on the file, no checksum field.
                            actual = opener.read(expected.offset,
                                    expected.nbytes);
                        }

                    }

                    final int actualChecksum = checker.checksum(actual);
                    
                    if(log.isDebugEnabled())
                        log.debug("read : i=" + i + ", prior=" + prior
                            + ", " + (cacheHit ? "hit" : "miss") + ", rec="
                            + expected + ", actualChecksum=" + actualChecksum
                            + ", actual=" + actual);

                    // Verify the data read from cache/disk.
                    assertEquals(expected.data, actual);

                }
                
            }

            // flush the write cache to the backing file.
            log.info("Service flush().");
            writeCacheService.flush(true/*force*/);
            log.info("Service flush() - done.");

            // verify the file size is as expected (we presize the file).
            assertEquals("fileExtent", lastOffset, opener.reopenChannel()
                    .size());
            
            // close the write cache.
            log.info("Service close().");
            writeCacheService.close();

            // verify the file size is as expected (we presize the file).
            assertEquals("fileExtent", lastOffset, opener.reopenChannel()
                    .size());

            final long elapsed = TimeUnit.NANOSECONDS.toMillis(System
                    .nanoTime()
                    - begin);

            if (log.isInfoEnabled()) {

                // data rate in MB/sec (counts read back as well).
                final double mbPerSec = (((double) lastOffset)
                        / Bytes.megabyte32 / (elapsed / 1000d));

                final NumberFormat fpf = NumberFormat.getNumberInstance();

                fpf.setGroupingUsed(false);

                fpf.setMaximumFractionDigits(2);

                log.info("#miss=" + nmiss + ", nrecs=" + nrecs + ", maxreclen="
                        + maxreclen + ", nlarge=" + nlarge + ", nbuffers="
                        + nbuffers + ", lastOffset=" + lastOffset
                        + ", mbPerSec=" + fpf.format(mbPerSec));
                
                log.info(writeCacheService.getCounters().toString());
                
            }
            
            /*
             * Verify the backing file against the mock data.
             */
            {

                for (int i = 0; i < nrecs; i++) {

                    final MockRecord expected = records[i];

                    final ByteBuffer actual = opener.read(expected.offset,
                            expected.nbytes);

                    final int actualChecksum = checker.checksum(actual);

                    if(log.isDebugEnabled())
                        log.debug("read : i=" + i + ", rec=" + expected
                            + ", actualChecksum=" + actualChecksum
                            + ", actual=" + actual);

                    // Verify the data read from cache/disk.
                    assertEquals(expected.data, actual);

                }

            }

        } finally {
            if (writeCacheService != null)
                try {
                    writeCacheService.close();
                } catch (InterruptedException e) {
                    log.error(e, e);
                }
            if (opener != null) {
                opener.destroy();
            }
        }
        
    }
    
    /*
     * Test helpers
     */
            
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
     * Simple implementation for unit tests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static private class WORMWriteCacheImpl extends WriteCache.FileChannelWriteCache {

        public WORMWriteCacheImpl(final long baseOffset, final ByteBuffer buf,
                final boolean useChecksum,
                final boolean isHighlyAvailable,
                final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener)
                throws InterruptedException {

            super(baseOffset, buf, useChecksum, isHighlyAvailable,
                    bufferHasData, opener);

        }

        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffset,
                final Map<Long, RecordMetadata> recordMapIsIgnored,
                final long nanos) throws InterruptedException, IOException {

            final long begin = System.nanoTime();

            final int dpos = data.position();

            final int nbytes = data.remaining();

            final int nwrites = FileChannelUtility.writeAll(opener, data,
                    firstOffset);

            final WriteCacheCounters counters = this.counters.get();
            counters.nwrite += nwrites;
            counters.bytesWritten += nbytes;
            counters.elapsedWriteNanos += (System.nanoTime() - begin);

            if (log.isInfoEnabled())
                log.info("wroteOnDisk: dpos=" + dpos + ", nbytes=" + nbytes
                        + ", firstOffset=" + firstOffset + ", nrecords="
                        + recordMapIsIgnored.size());

            return true;

        }

    }

    /**
     * An allocation to be written at some offset on a backing channel.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class MockRecord {
        
        /** The file offset at which the record will be written. */
        final long offset;

        /** The data (bytes from the position to the limit). */
        final ByteBuffer data;

        /**
         * The #of bytes in the application portion of the record (this DOES NOT
         * include the optional checksum field at the end of the record).
         */
        final int nbytes;
        
        /** The {@link Adler32} checksum of the data. */
        final int chksum;

        public String toString() {

            return getClass().getSimpleName() + "{offset=" + offset
                    + ",nbytes=" + nbytes + ",chksum=" + chksum + ",data="
                    + data + "}";
            
        }
        
        /**
         * 
         * @param offset
         *            The file offset.
         * @param data
         *            The data (bytes between the position and the limit).
         * @param chksum
         *            The checksum of the data.
         */
        public MockRecord(final long offset, final ByteBuffer data, final int chksum) {

            this.offset = offset;

            this.data = data.asReadOnlyBuffer();
            
            this.nbytes = data.limit();
            
            this.chksum = chksum;

        }

    }

    /**
     * Create and return an array of {@link MockRecord}s. The records will be
     * assigned to a dense region in the file, beginning with the given file
     * offset.
     * 
     * @param firstOffset
     *            The file offset of the first record.
     * @param nrecs
     *            The #of records to create.
     * @param maxreclen
     *            The maximum length of a record. Records will be in [1:m] bytes
     *            long and will contain random bytes.
     * @param largeRecordRate
     *            The rate in [0:1) of records which will be larger than the
     *            {@link WriteCache} buffer size.
     * @param useChecksums
     *            when <code>true</code> record level checksums are in use and
     *            the actual size of the record on the disk is 4 bytes larger
     *            than the application record size due to the checksum field
     *            stored at the end of the record.
     * 
     * @return The {@link MockRecord}s.
     */
    private MockRecord[] createMockRecords(final long firstOffset,
            final int nrecs, final int maxreclen, final double largeRecordRate,
            final boolean useChecksums) {

        final MockRecord[] a = new MockRecord[nrecs];

        long offset = firstOffset;

        final ChecksumUtility checker = new ChecksumUtility();

        for (int i = 0; i < nrecs; i++) {

            final int nbytes;
            final byte[] bytes;
            
            if (r.nextDouble() < largeRecordRate) {

                /*
                 * large record.
                 */
                nbytes = WRITE_CACHE_BUFFER_CAPACITY * (r.nextInt(3) + 1);

                bytes = new byte[nbytes];
    
            } else {

                nbytes = r.nextInt(maxreclen) + 1;

                bytes = new byte[nbytes];

            }

            r.nextBytes(bytes);

            // Create a record with random data for that offset.
            a[i] = new MockRecord(offset, ByteBuffer.wrap(bytes), checker
                    .checksum(bytes));

            // Update the current offset.
            offset += a[i].nbytes + (useChecksums ? 4 : 0);

        }

        return a;
        
    }

    /**
     * Simple implementation for a {@link RandomAccessFile} with hook for
     * deleting the test file.
     */
    private class ReopenFileChannel implements IReopenChannel<FileChannel> {

        final private File file;

        private final String mode;

        private volatile RandomAccessFile raf;

        private AtomicInteger nrepen = new AtomicInteger();
        
        public ReopenFileChannel(final File file, final String mode)
                throws IOException {

            this.file = file;

            this.mode = mode;

            reopenChannel();

        }

        public String toString() {

            return file.toString();

        }

        public void truncate(final long extent) throws IOException {

            reopenChannel();

            raf.setLength(extent);
            
            raf.getChannel().force(true/* metaData */);

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

            /*
             * As far as I can tell, this test suite should not cause the
             * reopener to run more than once (in the constructor).
             */
            final int nreopen = nrepen.incrementAndGet();
            if (nreopen > 1) {
//            if (log.isInfoEnabled()) {
                log.error("(Re-)opened file: " + file, new RuntimeException(
                        "nreopen=" + nreopen+", test="+getName()));
//            }
            }

            return raf.getChannel();

        }

    };

    /*
     * HA pipeline tests.
     */

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

            log.warn("suggestedPort is busy: suggestedPort=" + suggestedPort
                    + ", using port=" + port + " instead");

        }

        return port;

    }

    /**
     * A test of a quorum with k == 3, 2 running services, one buffer, and one
     * record written.
     * 
     * @throws InterruptedException
     * @throws IOException
     * 
     *             FIXME Verify the received data. Write on the disk and compare
     *             bits or a bit bucket and just do checksums?
     * 
     *             FIXME Test w/ {@link HASendService} and
     *             {@link HAReceiveService} otherwise we have no way to verify
     *             the whole-buffer checksums.
     *             <p>
     *             This is getting close. I need to actually handle the
     *             replicated records. For that purpose, it might be nice to
     *             reconcile the {@link MockQuorum} here with the one in the
     *             com.bigdata.journal.ha test package.
     */
    public void test_writeCacheServiceWORM_1buffer_HA_k3_size2()
            throws InterruptedException, IOException {

        final int nbuffers = 1;
        final int nrecs = 1;
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        final boolean isHighlyAvailable = true;

        final MockQuorumManager qm = new MockQuorumManager(3/* k */, 2/* size */);

        try {

            doWORMTest(nbuffers, nrecs, maxreclen, largeRecordRate,
                    useChecksums, isHighlyAvailable, StoreTypeEnum.WORM, qm);

        } finally {

            qm.terminate();

        }

    }

    /**
     * A mock {@link QuorumManager} used to test the HA write pipeline. This
     * sets up one {@link HASendService} and <code>k-1</code>
     * {@link HAReceiveService}s in a pipeline. 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class MockQuorumManager implements QuorumManager {

        /** the replication factor (desired replication count). */
        final private int k;

        /** the actual replication count. */
        final private int size; 
        
        /** one quorum per node. */
        final private Quorum[] quorums;
        
        /** the send/receive address of each service. */
        final InetSocketAddress[] addrs;

        /** send service for the leader. */
        final private HASendService sendService;

        /**
         * The receive services for the k-1 followers.
         * <p>
         * Note: you index directly into this array (the first element is
         * <code>null</code>).
         */
        final private HAReceiveService<HAWriteMessage> receiveServices[];

        /**
         * The buffer used to relay the data. This is only allocated for a
         * follower with a downstream follower. The indices are the quorum index
         * values. Quorum members that do not relay will have a
         * <code>null</code> at their corresponding index.
         */
        private final ByteBuffer receiveAndReplicateBuffer[];
        
        /** The current quorum token. */
        private long token = Quorum.NO_QUORUM;

        /**
         * 
         * @param k
         *            The replication factor.
         * @param size
         *            The replication count.
         * 
         * @throws IOException
         * @throws InterruptedException
         */
        public MockQuorumManager(final int k, final int size)
                throws IOException, InterruptedException {

            if (k < 1)
                throw new IllegalArgumentException();
            if ((k % 2) == 0)
                throw new IllegalArgumentException("k must be odd: "+k);
            if (size < 0)
                throw new IllegalArgumentException();
            if (size > k)
                throw new IllegalArgumentException();

            this.k = k;

            this.size = size;

            this.addrs = new InetSocketAddress[k];

            this.receiveServices = new HAReceiveService[k];
            
            this.receiveAndReplicateBuffer = new ByteBuffer[k];
            
            this.quorums = new Quorum[k];

            HASendService sendService = null;
            
            for (int i = size - 1; i >= 0; i--) {

                addrs[i] = new InetSocketAddress(getPort(0/* suggestedPort */));

                if (i == 0) {
                    
                    sendService = new HASendService(addrs[i + 1]);

                    sendService.start();

                } else {

                    if (addrs[i + 1] != null) {

                        /*
                         * Acquire buffer from the pool iff there is a service
                         * which is downstream from this service.
                         */
                        
                        receiveAndReplicateBuffer[i] = DirectBufferPool.INSTANCE
                                .acquire();
                        
                    }
                    
                    receiveServices[i] = new HAReceiveService<HAWriteMessage>(
                            addrs[i]/* self */, addrs[i + 1]/* next */);

                    receiveServices[i].start();

                }

            }

            this.sendService = sendService;

            for (int i = 0; i < size; i++) {
                
                quorums[i] = new MockQuorum(i);
                
            }

        }

        public int replicationFactor() {

            return k;
            
        }

        public boolean isHighlyAvailable() {
            
            return k > 1;
            
        }

        public void assertQuorum(final long token) {

            if (token == this.token)
                return;

            throw new IllegalStateException();

        }

        public void assertQuorumLeader(final long token) {

            if (token == this.token)
                return;

            throw new IllegalStateException();

        }

        /**
         * Returns the {@link Quorum} for the leader.
         */
        public Quorum awaitQuorum() throws InterruptedException {
            
            return quorums[0];
            
        }

        /**
         * Returns the {@link Quorum} for the leader.
         */
        public Quorum getQuorum() {

            return quorums[0];
            
        }

        public void terminate() {

            for (Quorum q : quorums) {

                if (q != null) {

                    q.invalidate();

                }

            }

        }

        /**
         * Inner {@link Quorum} implementation using the data on the outer
         * class.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class MockQuorum implements Quorum {

            private final int index;

            MockQuorum(final int index) {

                this.index = index;

            }

            public int replicationFactor() {
                
                return k;
                
            }

            public int size() {
                
                return size;
                
            }

            public long token() {

                return token;
                
            }

            public boolean isLeader() {
                
                return index == 0;
                
            }

            public boolean isLastInChain() {
                
                return index + 1 == size();
                
            }

            public boolean isQuorumMet() {

                return true;
                
            }

            public int getIndex() {

                return index;
                
            }

            public void invalidate() {
                if (index == 0) {
                    sendService.terminate();
                } else if (receiveServices[index] != null) {
                    receiveServices[index].terminate();
                    try {
                        /*
                         * Await shutdown so we can be positive that no thread
                         * can acquire() the direct buffer while the receive
                         * service might still be writing on it!
                         */
                        receiveServices[index].awaitShutdown();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    if (receiveAndReplicateBuffer[index] != null) {
                        try {
                            /*
                             * Release the buffer back to the pool.
                             */
                            DirectBufferPool.INSTANCE
                                    .release(receiveAndReplicateBuffer[index]);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }

            /*
             * Methods for the journal/data service which are not required here.
             */

            public void abort2Phase() throws IOException {
                throw new UnsupportedOperationException();
            }

            public void commit2Phase(long commitTime) throws IOException {
                throw new UnsupportedOperationException();
            }

            public HAGlue getHAGlue(int index) {
                if (index < 0 || index >= replicationFactor())
                    throw new IndexOutOfBoundsException();
                throw new UnsupportedOperationException();
            }

            public ExecutorService getExecutorService() {
                throw new UnsupportedOperationException();
            }

            public int prepare2Phase(IRootBlockView rootBlock, long timeout,
                    TimeUnit unit) throws InterruptedException,
                    TimeoutException, IOException {
                throw new UnsupportedOperationException();
            }

            public ByteBuffer readFromQuorum(long addr) {
                throw new UnsupportedOperationException();
            }

            /*
             * Methods that we need to implement for the write pipeline.
             */
            
            public HAReceiveService<HAWriteMessage> getHAReceiveService() {

                if(isLeader())
                    throw new UnsupportedOperationException();
                
                return receiveServices[index];
                
            }

            public HASendService getHASendService() {
                
                if(!isLeader())
                    throw new UnsupportedOperationException();
                
                return sendService;
                
            }

            /**
             * Not supported for a standalone journal.
             * 
             * @throws UnsupportedOperationException
             *             always.
             */
            public Future<Void> replicate(HAWriteMessage msg, ByteBuffer b)
                    throws IOException {

                if(!isLeader())
                    throw new UnsupportedOperationException();
                
                throw new UnsupportedOperationException();
                
            }

            /**
             * Not supported for a standalone journal.
             * 
             * @throws UnsupportedOperationException
             *             always.
             */
            public Future<Void> receiveAndReplicate(HAWriteMessage msg)
                    throws IOException {
                throw new UnsupportedOperationException();
            }

        } // MockQuorum

    } // MockQuorumManager

}
