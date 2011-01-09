/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on May 16, 2008
 */

package com.bigdata.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.apache.system.SystemUtil;

import com.bigdata.btree.BytesUtil;
import com.bigdata.rawstore.Bytes;

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
    final int FILE_SIZE = 20 * Bytes.megabyte32;
    
    final Random r = new Random();
   
    // size of a single buffer.
    final int bufferSize = DirectBufferPool.INSTANCE.getBufferCapacity();

    /** Start at any position in the source file (up to int32 offset). */
    protected long getRandomPosition(RandomAccessFile raf) throws IOException {
        
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
    protected int getRandomLength(RandomAccessFile raf, long pos) throws IOException {

        final int count = (int) Math.min(Integer.MAX_VALUE, Math.min(
            raf.length() - pos, bufferSize
                    * r.nextInt(3) + r.nextInt(bufferSize)));
       
        return count;
        
    }

    protected void assertSameData(byte[] expected, byte[] actual) {
        
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
                    
                    System.err.println("purturbing region after trial: trial="
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
            
            System.err.println("verifying data: pos="+pos+", count="+count);
            
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
                
                System.err.println("Note: read required: "+ioCount+" IOs");
                
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

        if(SystemUtil.isOSX()) {
            /*
             * FIXME For some reason, this unit test is hanging under OS X.
             */
            fail("Unit test hangs under OS X");
        }
        
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
                
                System.err.println("fromPosition="+fromPosition+", count="+count);

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
    
}
