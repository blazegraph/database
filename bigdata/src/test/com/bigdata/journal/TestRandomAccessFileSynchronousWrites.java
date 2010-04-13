/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Nov 19, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.apache.log4j.Logger;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import com.bigdata.rawstore.Bytes;

/**
 * Test suite for conformance with synchronous IO write requests made on a
 * {@link RandomAccessFile} opened using <code>rss</code> or <code>rdd</code>
 * mode. A conforming platform will NOT reorder writes and will a request to
 * {@link FileChannel#force(boolean)} will flush data through to stable media
 * before the write operation returns. A write cache in the operating system,
 * device driver, controller, or disk drive can defeat synchronous IO with the
 * results that: (a) the sequence in which writes are issued by the application
 * may not be the sequence in which the data are laid down on the disk; and (b)
 * the write operation may return before the data are stable on disk.
 * <p>
 * Both of these results can break the semantics of the atomic commit protocol
 * in at least the following ways:
 * <ul>
 * <li> If the root blocks are updated before the application data are on stable
 * media then a power failure will cause the application to read from the new
 * root block but the application data will not have been written.</li>
 * <li> If the write operation returns before the data are on stable media then
 * the application may conclude that the commit was successful when in fact the
 * data are not yet on disk. In fact, since the error is not reported
 * synchronously the application may never learn that the write has failed
 * unless it continues to write on the disk and a subsequent write turns up the
 * error.</li>
 * </ol>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRandomAccessFileSynchronousWrites extends TestCase {

    protected static final Logger log = Logger
            .getLogger(TestRandomAccessFileSynchronousWrites.class);

    public TestRandomAccessFileSynchronousWrites() {
        
    }

    public TestRandomAccessFileSynchronousWrites(String name) {
        
        super(name);
        
    }

    /**
     * Test verifies whether or not the platform appears to perform synchronous
     * IOs when creating a {@link RandomAccessFile} with mode <code>rws</code>.
     */
    public void test_syncWrites_rds() throws IOException {

        // Performance comparison when NOT requesting synchronous IO.
        final Stats rw = doSyncWriteTest("rw");
        
        final Stats rws = doSyncWriteTest("rws");

        assertWriteCacheDisabled(rw, rws);
        
    }
    
    /**
     * Test verifies whether or not the platform appears to perform synchronous
     * IOs when creating a {@link RandomAccessFile} with mode <code>rws</code>.
     */
    public void test_syncWrites_rdd() throws IOException {

        // Performance comparison when NOT requesting synchronous IO.
        final Stats rw = doSyncWriteTest("rw");
        
        final Stats rwd = doSyncWriteTest("rwd");

        assertWriteCacheDisabled(rw, rwd);
        
    }

    /**
     * Verify that the request to sync to disk with each IO (<code>rws</code> or
     * <code>rwd</code>) was honored by the underlying platform.
     * 
     * @param baseline
     *            The <code>rw</code> performance {@link Stats}.
     * @param syncio
     *            The performance for either the <code>rws</code> or
     *            <code>rwd</code> modes, which require synchronization to the
     *            disk after each write.
     * 
     * @throws AssertionFailedError
     *             unless the write IOPs are significantly lower for the
     *             <i>syncio</i> condition.
     */
    protected void assertWriteCacheDisabled(final Stats baseline,
            final Stats syncio) {

        final double ratio = Math
                .round(100. * (syncio.writesPerSec / (double) baseline.writesPerSec)) / 100.;

        final String msg = "ratio=" + ratio + ", " + baseline + ", " + syncio;
        
        if (ratio > .5) {

            /*
             * We are seeing more write operations per second (or more bytes
             * written per second) than can reasonably be expected synchronous
             * writes.
             */

            fail("Write cache in effect: " + msg);

        }
        
        System.out.println(msg);
        
    }

    /**
     * Test helper attempts to detect when a request for synchronous writes is
     * being ignored by the platform.
     * <p>
     * Note: The file is created using the temp file mechanisms so this is only
     * really testing the behavior of the disk on which the temp files are
     * stored.
     * <p>
     * Note: The more write operations that you request and the larger the file
     * on which those writes are randomly distributed the more you are likely to
     * defeat any cache mechanisms.
     * 
     * <pre>
     *    Results on a laptop class platform (Windows XP):
     *  
     *  write cache disabled in platform:
     *  
     *  elapsed=5063ms, mode=rwd, writesPerSec=988, bytesPerSec=1011258
     *  elapsed=5031ms, mode=rws, writesPerSec=994, bytesPerSec=1017690
     *  elapsed=109ms, mode=rw, writesPerSec=45872, bytesPerSec=46972477
     *  
     *  write cache enabled in platform:
     *  
     *  elapsed=1797ms, mode=rwd, writesPerSec=2782, bytesPerSec=2849193
     *  elapsed=1969ms, mode=rws, writesPerSec=2539, bytesPerSec=2600305
     *  elapsed=62ms, mode=rw, writesPerSec=80645, bytesPerSec=82580645
     * 
     * </pre>
     * 
     * Based on the data above, you can see that merely requesting synchronous
     * IO in Java clearly does not disable all layers of the write cache.
     * 
     * @param mode
     *            The file mode to be used.
     * 
     * @return The {@link Stats} for that mode.
     */
    protected Stats doSyncWriteTest(final String mode) throws IOException {

        final Random r = new Random();

        // #of records to write.
        final int LIMIT = 5000;
        
        // maximum size of the file on which the records will be written.
        final int MAXSIZE = 100 * Bytes.kilobyte32;
        
        // size of each record.
        final int RECSIZE = 1 * Bytes.kilobyte32;

        // create a record with random data.
        final byte[] record = new byte[RECSIZE];
        
        r.nextBytes(record);

        // create a temp file.
        final File file = File.createTempFile(getName(), ".tmp");

        try {

            // Note: can also test with rwd (synchronous metadata updates also).
            RandomAccessFile f = new RandomAccessFile(file, mode);

            try {

                final long begin = System.currentTimeMillis();
                
                for (int i = 0; i < LIMIT; i++) {

                    f.seek(r.nextInt(MAXSIZE));

                    f.write(record);
                    
                    final long elapsed = System.currentTimeMillis() - begin;

                    if(elapsed>5000) {

                        System.err.println("Test is taking too long - IO must be synchronous :-)");
                        
                        break;
                        
                    }

                }
                
                f.getChannel().force(true/*metaData*/);

                final long elapsed = System.currentTimeMillis() - begin;

                final long writesPerSec = (long)((LIMIT * 1000. / elapsed)+0.5);
                
                final long bytesPerSec = (long)((LIMIT*RECSIZE*1000./elapsed)+.5);
                
                final Stats stats = new Stats(mode,elapsed,writesPerSec,bytesPerSec);
                
                return stats;
                
            } finally {

                f.close();

            }

        } finally {

            if (!file.delete())
                log.warn("Could not delete: file=" + file);

        }

    }

    private static class Stats {

        final String mode;

        final long elapsed, writesPerSec, bytesPerSec;

        public Stats(final String mode, final long elapsed,
                final long writesPerSec, final long bytesPerSec) {

            this.mode = mode;
            this.elapsed = elapsed;
            this.writesPerSec = writesPerSec;
            this.bytesPerSec = bytesPerSec;

        }

        public String toString() {

            return "elapsed=" + elapsed + "ms, mode=" + mode
                    + ", writesPerSec=" + writesPerSec + ", bytesPerSec="
                    + bytesPerSec;

        }

    }

}
