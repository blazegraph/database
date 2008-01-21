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
 * {@link BigdataRepository}. This also tests the correct assignment of block
 * identifiers, the ability to count the #of blocks in a file version, the
 * ability to visit the block identifiers for a file version, the ability to
 * read a specific block for a file version, and the ability to read all data
 * for a file version from an input stream.
 * 
 * FIXME test where there is only a single index partition and where there are
 * several. This will exercise both the logic that locates the index partition
 * into which the atomic append will go and the logic that computes the next
 * block identifer for the file version when entering a new partition. These
 * tests can be run by establishing the appropriate partitions in the data index
 * for the embedded federation.
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
public class TestAppendBlock extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestAppendBlock() {
    }

    /**
     * @param arg0
     */
    public TestAppendBlock(String arg0) {
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

        final long block = repo.appendBlock(id, version,
                expected, 0, expected.length);

        assertEquals("block#", 0L, block);
        
        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        assertSameIterator("block identifiers", new Long[] { block }, repo
                .blocks(id, version));
        
        assertEquals("readBlock", expected, repo.readBlock(id, version, block));
        
        assertEquals("inputStream", expected, read(repo.inputStream(id, version)));

    }

    /**
     * Test the ability to write two partial blocks in a row onto a file version
     * and then read them back (test of append semantics).
     * 
     * @throws IOException
     */
    public void test_atomicAppend2SmallBlocks() throws IOException {
     
        final String id = "test";
        
        final int version = 0;
        
        final byte[] expected0 = new byte[]{1,2,3};
        final byte[] expected1 = new byte[]{4,5,6};
        
        // 1st block
        final long block0 = repo.appendBlock(id, version,
                expected0, 0, expected0.length);

        assertEquals("block#", 0L, block0);
        
        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        assertSameIterator("block identifiers", new Long[] { block0 }, repo
                .blocks(id, version));

        assertEquals("data", expected0, repo.readBlock(id, version, block0));

        assertEquals("data", expected0, read(repo.inputStream(id, version)));

        // 2nd block
        final long block1 = repo.appendBlock(id, version, expected1, 0,
                expected1.length);

        assertEquals("block#", 1L, block1);

        assertEquals("blockCount", 2, repo.getBlockCount(id, version));

        assertSameIterator("block identifiers", new Long[] { block0, block1 },
                repo.blocks(id, version));

        assertEquals("data", expected0, repo.readBlock(id, version, block0));
        assertEquals("data", expected1, repo.readBlock(id, version, block1));

        assertEquals("data", new byte[] { 1, 2, 3, 4, 5, 6 }, read(repo
                .inputStream(id, version)));

    }

    /**
     * Test the ability to write three partial blocks in a row onto a file
     * version and then read them back (test of append semantics).
     * 
     * @throws IOException
     */
    public void test_atomicAppend3SmallBlocks() throws IOException {
     
        final String id = "test";
        
        final int version = 0;
        
        final byte[] expected0 = new byte[]{1,2,3};
        final byte[] expected1 = new byte[]{4,5,6};
        final byte[] expected2 = new byte[]{7,8,9};
        
        // 1st block
        final long block0 = repo.appendBlock(id, version, expected0, 0,
                expected0.length);

        assertEquals("block#", 0L, block0);

        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        assertEquals("readBlock", expected0, repo
                .readBlock(id, version, block0));

        assertEquals("inputStream", expected0, read(repo.inputStream(id,
                version)));

        // 2nd block
        final long block1 = repo.appendBlock(id, version, expected1, 0,
                expected1.length);

        assertEquals("block#", 1L, block1);

        assertEquals("blockCount", 2, repo.getBlockCount(id, version));

        assertEquals("readBlock", expected1, repo
                .readBlock(id, version, block1));

        assertEquals("data", new byte[] { 1, 2, 3, 4, 5, 6 }, read(repo
                .inputStream(id, version)));

        // 3rd block
        final long block2 = repo.appendBlock(id, version, expected2, 0,
                expected2.length);

        assertEquals("block#", 2L, block2);

        assertEquals("blockCount", 3, repo.getBlockCount(id, version));

        assertEquals("data", expected2, repo.readBlock(id, version, block2));

        assertEquals("data", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                read(repo.inputStream(id, version)));

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

        final long block0 = repo.appendBlock(id, version,
                expected, 0, expected.length);
        
        assertEquals("block#",0L,block0);
        
        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        assertEquals("readBlock", expected, repo.readBlock(id, version, block0));

        assertEquals("inputStream", expected, read(repo.inputStream(id, version)));
        
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
        
        final long block0 = repo.appendBlock(id, version,
                expected, 0, expected.length);
        
        assertEquals("block#",0L,block0);
        
        assertEquals("blockCount", 1, repo.getBlockCount(id, version));

        assertEquals("readBlock", expected, repo.readBlock(id, version, block0));

        assertEquals("inputStream", expected, read(repo.inputStream(id, version)));
        
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
            
            repo.appendBlock(id, version, expected, 0, expected.length);
            
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

            final long block0 = repo.appendBlock(id, version,
                    expected, 0, expected.length);
            
            assertEquals("block#",0L,block0);
            
            assertEquals("blockCount", 1, repo.getBlockCount(id, version));

            assertEquals("readBlock", expected, repo.readBlock(id, version, block0));

            assertEquals("inputStream", expected, read(repo.inputStream(id, version)));

        }
        
        log.warn("There were " + nzero + " zero length blocks and " + nfull
                + " full length blocks out of " + LIMIT + " trials");
        
    }

}
