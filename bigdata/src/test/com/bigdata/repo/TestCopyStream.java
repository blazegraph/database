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
 * Created on Jan 19, 2008
 */

package com.bigdata.repo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;

import com.bigdata.repo.BigdataRepository.FileVersionOutputStream;

/**
 * Unit tests for copying streams into the repository and reading them back.
 * 
 * FIXME test flush semantics at BLOCK_SIZE-1, BLOCK_SIZE, and BLOCK_SIZE+1.
 * 
 * @todo test: A flush or close on the output stream should cause the buffered
 *       data to be atomically appended as a (partial) block.
 * 
 * @todo test {@link FileVersionOutputStream} counters (#of bytes written on the
 *       stream, #of blocks written on the file version).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCopyStream extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestCopyStream() {
    }

    public TestCopyStream(String name) {
        
        super(name);
        
    }

    /*
     * @todo the stream tests should focus on a pipe model where you obtain an
     * OutputStream for a file version and write on it and it periodically
     * flushes blocks to the file version.
     */
    
    /**
     * Copies a short stream onto a file version and reads it back.
     */
    public void test_copyStream_smallStream() throws IOException {
        
        final String id = "test";
        
        final int version = 0;
        
        final byte[] expected = new byte[]{1,2,3};
        
        assertEquals("nbytes", expected.length, repo.copyStream(id, version,
                new ByteArrayInputStream(expected)));

        assertEquals("blockCount", (expected.length + BLOCK_SIZE) / BLOCK_SIZE,
                repo.getBlockCount(id, version));

        final byte[] actual = read(repo.inputStream(id, version));

        assertEquals("data", expected, actual);

    }

    /**
     * Test copy of an empty stream resulting in one block.
     * 
     * @throws IOException
     */
    public void test_copyStream_emptyBlock() throws IOException {
        
        final String id = "test";
        
        final int version = 0;
        
        final byte[] expected = new byte[]{};
        
        assertEquals("nbytes", expected.length, repo.copyStream(id, version,
                new ByteArrayInputStream(expected)));

        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        final byte[] actual = read(repo.inputStream(id, version));

        assertEquals("data", expected, actual);

    }

    /**
     * Test copy of an stream containing exactly one block's data.
     * 
     * @throws IOException
     */
    public void test_copyStream_fullBlock() throws IOException {
        
        final String id = "test";
        
        final int version = 0;
        
        Random r = new Random();
        
        final byte[] expected = new byte[BLOCK_SIZE];
        
        r.nextBytes(expected);
        
        assertEquals("nbytes", expected.length, repo.copyStream(id, version,
                new ByteArrayInputStream(expected)));

        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        final byte[] actual = read(repo.inputStream(id, version));

        assertEquals("data", expected, actual);

    }

    /**
     * Stress test writing small streams of random length (from zero bytes to a
     * full block in length).
     * 
     * @throws IOException 
     */
    public void test_copyStream_smallRandomStreams() throws IOException {
        
        final int LIMIT = 100;
        
        final Random r = new Random();

        int nzero = 0;
        int nfull = 0;
        
        for(int i=0; i<LIMIT; i++) {
        
            /*
             * Note: {id + version} are always unique for this test.
             */
            final String id = "test#" + r.nextInt(1000);

            final int version = i;

            /*
             * Note: size in [0:block_size] bytes.
             * 
             * Note: the distribution is adjusted to make near zero and near
             * block_size operations at least 10% of all operations.
             */
            final int len;
            {
                final int x = r.nextInt(100);
                if (x < 10) {
                    // short block length.
                    len = r.nextInt(5);
                } else if (x >= 90) {
                    // long block length (up to block_size).
                    len = r.nextInt(5) + BLOCK_SIZE - 4;
                } else {
                    // uniform random distribution.
                    len = r.nextInt(BLOCK_SIZE + 1);
                }
            }
            
            if(len==0) nzero++;
            if(len==BLOCK_SIZE) nfull++;
            
            final byte[] expected = new byte[len];
            
            // random data.
            r.nextBytes(expected);

            assertEquals("nbytes", expected.length, repo.copyStream(id,
                    version, new ByteArrayInputStream(expected)));

            assertEquals("blockCount", 1, repo.getBlockCount(id, version));

            final byte[] actual = read(repo.inputStream(id, version));

            assertEquals("data", expected, actual);

            log.warn("There were " + nzero + " zero length blocks and " + nfull
                    + " full length blocks out of " + LIMIT + " trials");
            
        }
        
    }

    /**
     * Test the ability to write a stream onto the index that is automatically
     * partitioned into blocks.
     * 
     * @throws IOException
     */
    public void test_copyStream_largeStream() throws IOException {
        
        final String id = "test";
        
        final int version = 0;
        
        Random r = new Random();
        
        final int N = 10;
        
        final byte[] expected = new byte[N * BLOCK_SIZE
                + r.nextInt(BLOCK_SIZE - 1) + 1];
        
        r.nextBytes(expected);
        
        assertEquals("nbytes", expected.length, repo.copyStream(id, version,
                new ByteArrayInputStream(expected)));

        assertEquals("blockCount", N+1, repo.getBlockCount(id, version));

        final byte[] actual = read(repo.inputStream(id, version));

        assertEquals("data", expected, actual);
    }
    
    /**
     * Stress test writing large streams of random length.
     * 
     * @throws IOException 
     */
    public void test_copyStream_largeRandomStreams() throws IOException {

        final int LIMIT = 100;
        
        final Random r = new Random();

//        int nzero = 0;
//        int nfull = 0;
        
        for(int i=0; i<LIMIT; i++) {
        
            /*
             * Note: {id + version} are always unique for this test.
             */
            final String id = "test#" + r.nextInt(1000);

            final int version = i;

            /*
             * Note: size in [0:block_size] bytes.
             * 
             * Note: the distribution is adjusted to make near zero and near
             * block_size operations at least 10% of all operations.
             */
            final int n = r.nextInt(10); // #of full blocks [0:N-1].
            final int len;
            {
                final int x = r.nextInt(BLOCK_SIZE); // #of bytes in last block.
                if (x < 10) {
                    // short block length.
                    len = n * BLOCK_SIZE + r.nextInt(5);
                } else if (x >= 90) {
                    // long block length (up to block_size - 1).
                    len = n * BLOCK_SIZE + r.nextInt(5) + BLOCK_SIZE - 5;
                } else {
                    // uniform random distribution.
                    len = n * BLOCK_SIZE + r.nextInt(BLOCK_SIZE + 1);
                }
            }
            final int nblocks = (len + BLOCK_SIZE) / BLOCK_SIZE;
            log.info("n=" + n + ", len=" + len + ", nblocks=" + nblocks);
            
//            if (len % BLOCK_SIZE == 0)
//                nzero++;
//
//            if (len % BLOCK_SIZE == BLOCK_SIZE)
//                nfull++;
            
            final byte[] expected = new byte[len];
            
            // random data.
            r.nextBytes(expected);

            assertEquals("nbytes", expected.length, repo.copyStream(id,
                    version, new ByteArrayInputStream(expected)));

            assertEquals("blockCount", nblocks, repo.getBlockCount(id, version));

            final byte[] actual = read(repo.inputStream(id, version));

            assertEquals("data", expected, actual);

//            log.warn("There were " + nzero + " zero length blocks and " + nfull
//                    + " full length blocks out of " + LIMIT + " trials");
            
        }
                
    }
  
}
