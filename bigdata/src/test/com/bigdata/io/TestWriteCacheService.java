/**

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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;

import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ha.HAGlue;
import com.bigdata.journal.ha.Quorum;
import com.bigdata.journal.ha.QuorumManager;
import com.bigdata.rwstore.RWWriteCacheService;
import com.bigdata.util.ChecksumUtility;

/**
 * Test suite for the {@link WriteCacheService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestWriteCacheService extends TestCase2 {

    /**
     * 
     */
    public TestWriteCacheService() {
    }

    /**
     * @param name
     */
    public TestWriteCacheService(String name) {
        super(name);
    }

    public void test_simpleService() {
        
        // TODO: write test for WORM WriteCacheService
        
    }
    
    public void test_simpleRWService() {
        
        File file;
		try {
			file = File.createTempFile(getName(), ".rw.tmp");

            RWWriteCacheService writeCache = new RWWriteCacheService(5, file,
                    null, "rw", new MockSingletonQuorumManager());

		} catch (Exception e) {
			fail("Unexpected  Exception", e);
		}
        
    }
    
    public void test_simpleDataRWService() {
        
        File file;
		try {
			file = File.createTempFile(getName(), ".rw.tmp");

			RWWriteCacheService writeCache = new RWWriteCacheService(5, file, null, "rw",new MockSingletonQuorumManager());
			
            final ByteBuffer data1 = getRandomData();
            final long addr1 = 2048;
            {
                assertNull(writeCache.read(addr1));
                // write record @ addr.
                assertTrue(writeCache.write(addr1, data1,
                        ChecksumUtility.threadChk.get().checksum(data1)));
                // verify record @ addr can be read.
                assertNotNull(writeCache.read(addr1));
                // verify data read back @ addr.
                data1.position(0);
                assertEquals(data1, writeCache.read(addr1));
            }
		} catch (Exception e) {
			fail("Unexpected  Exception", e);
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

    public void test_stressDataRWService() throws InterruptedException, IOException {
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
        
        final ChecksumUtility checker = new ChecksumUtility();
        
        // Now randomize the array for writing
        randomizeArray(allocs);
        
        File file = null;
		try {
			file = File.createTempFile(getName(), ".rw.tmp");

            final ReopenFileChannel opener = new ReopenFileChannel(file, "rw");
            RWWriteCacheService writeCache = new RWWriteCacheService(4, file,
                    null, "rw", new MockSingletonQuorumManager());

            /*
             * First write 500 records into the cache and confirm they can all be read okay
             */
            for (int i = 0; i < 500; i++) {
            	AllocView v = allocs.get(i);
            	writeCache.write(v.addr, v.buf, checker.checksum(v.buf));  
            	v.buf.position(0);
            }
            for (int i = 0; i < 500; i++) {
            	AllocView v = allocs.get(i);
             	assertEquals(v.buf, writeCache.read(v.addr));     // expected, actual   	
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
             	assertEquals(v.buf, writeCache.read(v.addr)); 	
            }
            for (int i = 0; i < 500; i++) {
            	AllocView v = allocs.get(i);
            	assertEquals(v.buf, opener.read(v.addr, v.buf.capacity()));     // expected, actual on DISK  	
            }
            /*
             * Now add further 500 writes, flush and read full 1000 from disk
             */
            for (int i = 500; i < 1000; i++) {
            	AllocView v = allocs.get(i);
            	writeCache.write(v.addr, v.buf, checker.checksum(v.buf));           	
            	v.buf.position(0);
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
            // writeCache.reset();
            for (int i = 1000; i < 10000; i++) {
            	AllocView v = allocs.get(i);
            	if (!writeCache.write(v.addr, v.buf, checker.checksum(v.buf))) {
            		log.info("flushing and resetting writeCache");
            		writeCache.flush(false);
            		// writeCache.reset();
            		assertTrue(writeCache.write(v.addr, v.buf, checker.checksum(v.buf)));
            	}
            	v.buf.position(0);
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
            // writeCache.reset();
            randomizeArray(allocs);
            int duplicates = 0;
            for (int i = 0; i < 10000; i++) {
            	AllocView v = allocs.get(i);
        		v.buf.reset();           	
            	
        		try {
        			writeCache.write(v.addr, v.buf, checker.checksum(v.buf));
        		} catch (Throwable t) {
        			assertTrue(t instanceof AssertionError);
        		}
            }
            
            /*
             * Now flush and check if we can read in all records
             */
            writeCache.flush(true);
            for (int i = 0; i < 10000; i++) {
            	AllocView v = allocs.get(i);
               	v.buf.position(0);
             	assertEquals(v.buf, opener.read(v.addr, v.buf.capacity()));     // expected, actual   	
            }
        } finally {

            if (file != null && file.exists() && !file.delete()) {

                log.warn("Could not delete: file=" + file);

            }

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
            tmp.position(0);
            
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

    private static class MockSingletonQuorumManager implements QuorumManager {

        final int k = 1;
        final long token = 0L;

        public void assertQuorum(long token) {
            if (token != this.token)
                throw new IllegalStateException();
        }

        public Quorum awaitQuorum() throws InterruptedException {
            return quorum;
        }

        public Quorum getQuorum() {
            return quorum;
        }

        public int replicationFactor() {
            return k;
        }

        public void terminate() {
            // NOP
        }
        
        final Quorum quorum = new Quorum() {

            public boolean isMaster() {
                return true;
            }

            public boolean isQuorumMet() {
                return true;
            }

            public void abort2Phase() throws IOException {
                // TODO Auto-generated method stub
                
            }

            public void commit2Phase(long commitTime) throws IOException {
                // TODO Auto-generated method stub
                
            }

            public HAGlue getHAGlue(int index) {
                // TODO Auto-generated method stub
                return null;
            }

            public int prepare2Phase(IRootBlockView rootBlock, long timeout,
                    TimeUnit unit) throws InterruptedException,
                    TimeoutException, IOException {
                // TODO Auto-generated method stub
                return 0;
            }

            public void readFromQuorum(long addr, ByteBuffer b) {
                // TODO Auto-generated method stub
                
            }

            public int replicationFactor() {
                return k;
            }

            public int size() {
                return 1;
            }

            public long token() {
                return token;
            }
            
        };
        
    }

}
