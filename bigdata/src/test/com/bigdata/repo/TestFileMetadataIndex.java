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

import com.bigdata.repo.BigdataRepository.RepositoryDocumentImpl;

/**
 * Test operations on the file metadata index for the {@link BigdataRepository}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFileMetadataIndex extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestFileMetadataIndex() {
    }

    /**
     * @param arg0
     */
    public TestFileMetadataIndex(String arg0) {
        super(arg0);
    }

    /**
     * 
     * @throws IOException
     */
    public void test_create_binary01() throws IOException {
        
        final String id = "test";
        
        final String mimeType = "application/octet-stream";
        
        final byte[] content = new byte[]{1,2,3,4,5,6};
        
        DocumentImpl doc = new DocumentImpl();
        
        doc.setId(id);
        
        doc.setContentType(mimeType);
        
        doc.set("foo","bar");
        
        doc.copyStream(content);
        
        repo.create(doc);

        Document actual = repo.read(id);
        
        assertEquals("version", 0, ((RepositoryDocumentImpl) actual)
                .getVersion());

        assertEquals("Content-Type", mimeType, actual.getContentType());

        assertEquals("content", content, read(actual.getInputStream()));

    }
    
    /**
     * Test create a document with text content and verifies that we can read
     * back its metadata and textual content.
     * 
     * @throws IOException
     * 
     * @todo test create of a file without any content and read back of its
     *       metadata.
     * 
     * @todo test create of a file with content, read back its metadata and
     *       verify that the data was written into the data index as well.
     */
    public void test_create_text01() throws IOException {
        
        final String id = "test";
        
        final String encoding = "UTF-8";
        
        final String mimeType = "text/plain; charset="+encoding;
        
        final String content = "Hello world!";
        
        DocumentImpl doc = new DocumentImpl();
        
        doc.setId(id);
        
        doc.setContentType(mimeType);
        
        doc.setContentEncoding(encoding);
        
        doc.set("foo","bar");
        
        doc.copyString(encoding, content);
        
        repo.create(doc);

        Document actual = repo.read(id);
        
        assertEquals("version", 0, ((RepositoryDocumentImpl) actual)
                .getVersion());

        assertEquals("Content-Type", mimeType, actual.getContentType());

        assertEquals("Content-Encoding", encoding, actual.getContentEncoding());

        assertEquals("content", content, read(actual.getReader()));

    }
    
    /**
     * @todo test delete of the metadata and data for a file (in which order
     *       should this operation take place?)
     */
    public void test_delete01() {
     
        fail("write test");
        
    }
    
    /**
     * @todo test update of a file with no data (initial write or atomic
     *       append).
     * 
     * @todo test replacement of the file's data (this raises an interesting
     *       point - I suppose that we just delete the file's data and then
     *       write new records. atomic delete is not going to be possible for
     *       very large files (ones that span more than one index partition)).
     */
    public void test_update01() {
        
        fail("write test");
        
    }

    /**
     * @todo have both the high-level read returning a {@link Document} (rename
     *       that class?) and low-level reads that deal directly with the Id and
     *       the data index.  This class should test high-level reads only.  Test
     *       low-level reads with the rest of the data index operations.
     */
    public void test_read() {
     
        fail("write test");

    }
    
}
