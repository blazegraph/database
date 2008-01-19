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

import com.bigdata.scaleup.MetadataIndex;

/**
 * Test atomic append operations on the file data index for the
 * {@link BigdataRepository}.
 * 
 * @todo test append (i.e., write) with read back (read assembles a series of
 *       chunks into a byte stream).
 * 
 * @todo test read of byte ranges of the file (used by split).
 * 
 * @todo test split of a large file into blocks and the read of each block by
 *       its appropriate client. (@todo also test ability to figure out which
 *       client is "near" the blocks by consulting the {@link MetadataIndex} and
 *       an as yet undefined network topology model.)
 * 
 * @todo test estimate of the content length
 * 
 * @todo test exact computation of the content length (still an estimate since
 *       there is no guarentee that the file remains unmodified).
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

    static protected final int BLOCK_SIZE = BigdataRepository.BLOCK_SIZE; 
    
    /**
     * Test the ability to write a byte[] onto the index and read it back.
     * 
     * @throws IOException
     * 
     * @todo test when the byte[] spans multiple blocks.
     */
    public void test_atomicAppend123() throws IOException {
     
        final String id = "test";
        
        final int version = 0;
        
        final byte[] expected = new byte[]{1,2,3};
        
        assertEquals("nblocks", 1, repo.atomicAppend(id, version, expected, 0, expected.length));

        assertEquals("blockCount",1,repo.getBlockCount(id, version));
        
        final byte[] actual = read(repo.inputStream(id,version));
        
        assertEquals("data",expected,actual);
        
    }

    /**
     * Atomic append of a zero length block.
     * 
     * @throws IOException
     */
    public void test_atomicAppendZeroLength() throws IOException {
        
        final String id = "test";
        
        final int version = 0;
        
        final byte[] expected = new byte[]{};
        
        assertEquals("nblocks", 1, repo.atomicAppend(id, version, expected, 0, expected.length));
        
        assertEquals("blockCount",1,repo.getBlockCount(id, version));
        
        final byte[] actual = read(repo.inputStream(id,version));
        
        assertEquals("data",expected,actual);
        
    }

    /**
     * A stress test for writing a (partial) block on a file and reading back
     * its data.
     * <p>
     * Note: By virtue of how the file names are choosen this also tests atomic
     * append of a file when there is an existing file and the new file would be
     * (a) before; and (b) after the existing file in the index order.
     * 
     * @throws IOException
     */
    public void test_atomicAppendPartialBlockStressTest() throws IOException {
        
        final int LIMIT = 100;
        
        final Random r = new Random();
        
        for(int i=0; i<LIMIT; i++) {
        
            /*
             * Note: {id + version} are always unique for this test.
             */
            final String id = "test#" + r.nextInt(1000);

            final int version = i;

            // does not exceed one block in length (zero bytes is allowed).
            final int len = r.nextInt(BLOCK_SIZE);
            
            final byte[] expected = new byte[len];
            
            // random data.
            r.nextBytes(expected);

            assertEquals("nblocks", 1, repo.atomicAppend(id, version, expected, 0, expected.length));

            assertEquals("blockCount", 1, repo.getBlockCount(id, version));

            final byte[] actual = read(repo.inputStream(id, version));

            assertEquals("data", expected, actual);

        }
        
    }

    /*
     * @todo the stream tests should focus on a pipe model where you obtain an
     * OutputStream for a file version and write on it and it periodically
     * flushes blocks to the file version.
     */
    
    /**
     * @todo Test the ability to write a stream onto the index that is
     *       automatically partitioned into blocks. A flush or close on the
     *       output stream should cause the buffered data to be atomically
     *       appended as a (partial) block.
     * 
     * @todo The next append should add another block after that (partial)
     *       block. The caller should be able to re-write the (partial) block,
     *       changing its size to any size between [0:64k] bytes (including 0?
     *       delete the block if length is zero?) - that is you can do random
     *       access read/write by block.
     */
    public void test_atomicAppend_largeStream() {
        
    }
    
    /**
     * @todo Stress test writing small streams of random length.
     */
    public void test_atomicAppend_smallRandomStreams() {
        
    }

    /**
     * @todo Stress test writing large streams of random length.
     */
    public void test_atomicAppend_largeRandomStreams() {
        
    }
  
}
