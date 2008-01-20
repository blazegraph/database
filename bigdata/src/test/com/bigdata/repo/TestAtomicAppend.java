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
 * Created on Jan 17, 2008
 */

package com.bigdata.repo;

import java.io.IOException;
import java.util.Random;

import com.bigdata.isolation.UnisolatedBTree;

/**
 * Test atomic append operations on the file data index for the
 * {@link BigdataRepository}.
 * 
 * @todo test exact computation of the content length (still an estimate since
 *       there is no guarentee that the file remains unmodified)? the only way
 *       to do this is to run an iterator over the data and aggregate the block
 *       lengths.
 * 
 * @todo test atomic append behavior after a file version has been deleted. Note
 *       that a kind of {@link UnisolatedBTree} is being used so it stores
 *       versioned data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAtomicAppend extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestAtomicAppend() {
    }

    /**
     * @param arg0
     */
    public TestAtomicAppend(String arg0) {
        super(arg0);
    }

    /**
     * Test the ability to write a byte[] onto the index and read it back.
     * 
     * @throws IOException
     */
    public void test_atomicAppendSmallBlock() throws IOException {
     
        final String id = "test";
        
        final int version = 0;
        
        final byte[] expected = new byte[]{1,2,3};
        
        assertEquals("nbytes", expected.length, repo.atomicAppend(id, version,
                expected, 0, expected.length));

        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        final byte[] actual = read(repo.inputStream(id, version));

        assertEquals("data", expected, actual);

    }

    /**
     * Atomic append of a zero length block.
     * 
     * @throws IOException
     */
    public void test_atomicAppendEmptyBlock() throws IOException {

        final String id = "test";

        final int version = 0;

        final byte[] expected = new byte[] {};

        assertEquals("nbytes", expected.length, repo.atomicAppend(id, version,
                expected, 0, expected.length));

        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        final byte[] actual = read(repo.inputStream(id, version));

        assertEquals("data", expected, actual);
        
    }

    /**
     * Atomic append of a full block.
     * 
     * @throws IOException
     */
    public void test_atomicAppendFullBlock() throws IOException {
        
        final String id = "test";
        
        final int version = 0;
        
        Random r = new Random();
        
        final byte[] expected = new byte[BLOCK_SIZE];

        r.nextBytes(expected);
        
        assertEquals("nbytes", expected.length, repo.atomicAppend(id, version,
                expected, 0, expected.length));

        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        final byte[] actual = read(repo.inputStream(id, version));

        assertEquals("data", expected, actual);
        
    }

    /**
     * Verify correct rejection: (a) writes that are larger than one block.
     * 
     * @todo do more correct rejection tests.
     * 
     * @throws IOException
     */
    public void test_atomicAppendCorrectRejection() throws IOException {
     
        final String id = "test";
        
        final int version = 0;
        
        // Note: too large by one byte.
        final byte[] expected = new byte[BLOCK_SIZE + 1];
        
        try {
            
            repo.atomicAppend(id, version, expected, 0, expected.length);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            log.info("Ignoring expected exception: "+ex);
            
        }

    }
    
    /**
     * A stress test for writing a partial and full blocks on a file and reading
     * back its data.  Each pass writes on a different file.
     * <p>
     * Note: By virtue of how the file names are choosen this also tests atomic
     * append of a file when there is an existing file and the new file would be
     * (a) before; and (b) after the existing file in the index order.
     * 
     * @throws IOException
     */
    public void test_atomicAppendStressTest() throws IOException {
        
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

            assertEquals("nbytes", expected.length, repo.atomicAppend(id,
                    version, expected, 0, expected.length));

            assertEquals("blockCount", 1, repo.getBlockCount(id, version));

            final byte[] actual = read(repo.inputStream(id, version));

            assertEquals("data", expected, actual);

            log.warn("There were " + nzero + " zero length blocks and " + nfull
                    + " full length blocks out of " + LIMIT + " trials");
            
        }
        
    }

}
