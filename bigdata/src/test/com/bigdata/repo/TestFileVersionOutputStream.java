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
 * Created on Jan 21, 2008
 */

package com.bigdata.repo;

import java.io.IOException;

import com.bigdata.repo.BigdataRepository.FileVersionOutputStream;

/**
 * Tests some specifics of the {@link FileVersionOutputStream}. These behaviors
 * are only observable to callers that cast
 * {@link BigdataRepository#outputStream(String, int)} to a
 * {@link FileVersionOutputStream}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFileVersionOutputStream extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestFileVersionOutputStream() {
    }

    /**
     * @param arg0
     */
    public TestFileVersionOutputStream(String arg0) {
        super(arg0);
    }

    /**
     * A test of the flush semantics (writes a partial block, but never an empty
     * block) and of the counters exposed by the {@link FileVersionOutputStream}.
     * @throws IOException 
     */
    public void test_flushAndCounters() throws IOException {
        
        final String id = "test";
        
        final int version = 0;
        
        final FileVersionOutputStream os = (FileVersionOutputStream) repo
                .outputStream(id, version);
        
        assertEquals("byteCount",0L,os.getByteCount());

        assertEquals("blockCount",0L,os.getBlockCount());
        
        // write an empty byte[].
        os.write(new byte[]{});

        assertEquals("byteCount",0L,os.getByteCount());

        assertEquals("blockCount",0L,os.getBlockCount());

        // flush - NOP since buffer is empty.
        os.flush();
        
        assertEquals("byteCount",0L,os.getByteCount());

        assertEquals("blockCount",0L,os.getBlockCount());

        // write a byte[].
        os.write(new byte[]{1,2,3});

        assertEquals("byteCount",3L,os.getByteCount());

        assertEquals("blockCount",0L,os.getBlockCount());

        // write another byte[].
        os.write(new byte[]{4,5,6});

        assertEquals("byteCount",6L,os.getByteCount());

        assertEquals("blockCount",0L,os.getBlockCount());

        assertEquals("blockCount",0L,repo.getBlockCount(id, version));

        assertSameIterator("block identifiers", new Long[] {}, repo.blocks(id,
                version));

        // Note: input stream is null until 1st block is written.
        assertEquals("inputStream", null, repo.inputStream(id, version));

        // flush - writes one block with 6 bytes.
        os.flush();
        
        assertEquals("byteCount",6L,os.getByteCount());

        assertEquals("blockCount",1L,os.getBlockCount());

        assertEquals("blockCount",1L,repo.getBlockCount(id, version));
        
        assertSameIterator("block identifiers", new Long[] {0L}, repo.blocks(id,
                version));

        assertEquals("inputStream", new byte[] { 1, 2, 3, 4, 5, 6 }, read(repo
                .inputStream(id, version)));
        
        // write another byte[].
        os.write(new byte[]{7,8,9});

        assertEquals("byteCount",9L,os.getByteCount());

        assertEquals("blockCount",1L,os.getBlockCount());

        assertEquals("blockCount",1L,repo.getBlockCount(id, version));

        // read back is unchanged since buffer was not flushed.
        assertEquals("inputStream", new byte[] { 1, 2, 3, 4, 5, 6 }, read(repo
                .inputStream(id, version)));

        // flush - writes 2nd block with 3 bytes.
        os.flush();
        
        assertEquals("byteCount",9L,os.getByteCount());

        assertEquals("blockCount",2L,os.getBlockCount());

        assertEquals("blockCount",2L,repo.getBlockCount(id, version));
        
        assertSameIterator("block identifiers", new Long[] { 0L, 1L }, repo.blocks(
                id, version));

        assertEquals("inputStream", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, read(repo
                .inputStream(id, version)));

    }
    
}
