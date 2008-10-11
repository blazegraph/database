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

package com.bigdata.bfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.Document;
import com.bigdata.bfs.DocumentImpl;
import com.bigdata.bfs.FileMetadataSchema;
import com.bigdata.bfs.RepositoryDocumentImpl;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.ITPV;

/**
 * Test operations on the file metadata index for the {@link BigdataFileSystem}.
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
     * Create a binary file and verifies its metadata and content.
     * 
     * @throws IOException
     */
    public void test_create_binary01() throws IOException {

        final String id = "test";

        final String mimeType = "application/octet-stream";

        final byte[] content = new byte[] { 1, 2, 3, 4, 5, 6 };

        DocumentImpl doc = new DocumentImpl();

        doc.setId(id);

        doc.setContentType(mimeType);

        doc.setProperty("foo", "bar");

        doc.copyStream(content);

        repo.create(doc);

        Document actual = repo.read(id);

        assertEquals("version", 0, ((RepositoryDocumentImpl) actual)
                .getVersion());

        assertEquals("user property", "bar", actual.getProperty("foo"));

        assertEquals("Content-Type", mimeType, actual.getContentType());

        assertEquals("content", content, read(actual.getInputStream()));

    }

    /**
     * Create a text file and verify its metadata and content.
     * 
     * @throws IOException
     */
    public void test_create_text01() throws IOException {

        final String id = "test";

        final String encoding = "UTF-8";

        final String mimeType = "text/plain; charset=" + encoding;

        final String content = "Hello world!";

        DocumentImpl doc = new DocumentImpl();

        doc.setId(id);

        doc.setContentType(mimeType);

        doc.setContentEncoding(encoding);

        doc.setProperty("foo", "bar");

        doc.copyString(encoding, content);

        repo.create(doc);

        Document actual = repo.read(id);

        assertEquals("version", 0, ((RepositoryDocumentImpl) actual)
                .getVersion());

        assertEquals("Content-Type", mimeType, actual.getContentType());

        assertEquals("Content-Encoding", encoding, actual.getContentEncoding());

        assertEquals("user property", "bar", actual.getProperty("foo"));

        assertEquals("content", content, read(actual.getReader()));

    }

    /**
     * Create an empty file and verify its metadata.
     */
    public void test_create_empty() {

        final String id = "test";

        final Map<String, Object> metadata = new HashMap<String, Object>();

        metadata.put(FileMetadataSchema.ID, id);

        metadata.put("foo", "bar");

        final int version = repo.create(metadata);

        metadata.put(FileMetadataSchema.VERSION, Integer.valueOf(version));

        assertEquals("version", 0, version);

        RepositoryDocumentImpl doc = (RepositoryDocumentImpl) repo.read(id);

        assertTrue("exists", doc.exists());

        assertEquals("version", version, doc.getVersion());

        assertNotSame("versionCreateTime", 0L, doc.getVersionCreateTime());

        assertEquals("earliestVersionCreateTime", doc.getVersionCreateTime(), doc.getEarliestVersionCreateTime());

        assertEquals("metadataUpdateTime", doc.getVersionCreateTime(), doc.getMetadataUpdateTime());

        Map<String, Object> actual = doc.asMap();

        assertEquals("id", id, actual.get(FileMetadataSchema.ID));

        assertEquals("version", version, actual.get(FileMetadataSchema.VERSION));

        assertEquals("user property", "bar", actual.get("foo"));

        assertEquals("size", metadata.size(), actual.size());

    }

    /**
     * Create an empty file and write some data on it. Then update its metadata,
     * verify the new metadata and the updated version, and then write some data
     * on the new version. Verify the both file versions can be read.
     * 
     * @throws IOException
     */
    public void test_create_update() throws IOException {

        final String id = "test";

        final Map<String, Object> metadata = new HashMap<String, Object>();

        metadata.put(FileMetadataSchema.ID, id);

        metadata.put("foo", "bar");

        final int version0;
        final long createTime0;
        final byte[] expected0 = new byte[] { 1, 2, 3 };
        {

            version0 = repo.create(metadata);

            metadata.put(FileMetadataSchema.VERSION, Integer.valueOf(version0));

            assertEquals("version", 0, version0);

            RepositoryDocumentImpl doc = (RepositoryDocumentImpl) repo.read(id);

            createTime0 = doc.getVersionCreateTime();

            Map<String, Object> actual = doc.asMap();

            assertEquals("id", id, actual.get(FileMetadataSchema.ID));

            assertEquals("version", version0, actual
                    .get(FileMetadataSchema.VERSION));

            assertEquals("user property", "bar", actual.get("foo"));

            assertEquals("size", metadata.size(), actual.size());

            // write on the file version.
            repo.copyStream(id, version0, new ByteArrayInputStream(expected0));

            // verify read back.
            assertEquals("version0", expected0, read(repo.inputStream(id,
                    version0)));

        }

        // update (new file version).
        final int version1;
        final long createTime1;
        final byte[] expected1 = new byte[] { 4, 5, 6 };
        {

            // modify a user defined property.
            metadata.put("foo", "baz");

            DocumentImpl doc1 = new DocumentImpl(metadata);
            
            doc1.copyStream(expected1);
            
            version1 = repo.update( doc1 );
            
            assertEquals("version", 1, version1);

            metadata.put(FileMetadataSchema.VERSION, Integer.valueOf(version1));

            RepositoryDocumentImpl doc = (RepositoryDocumentImpl) repo.read(id);

            createTime1 = doc.getVersionCreateTime();

            assertNotSame("createTime", 0L, createTime1);

            assertNotSame("createTime", createTime0, createTime1);

            Map<String, Object> actual = doc.asMap();

            assertEquals("id", id, actual.get(FileMetadataSchema.ID));

            assertEquals("version", version1, actual
                    .get(FileMetadataSchema.VERSION));

            assertEquals("user property", "baz", actual.get("foo"));

            assertEquals("size", metadata.size(), actual.size());

//            // write on the file version.
//            repo.copyStream(id, version1, new ByteArrayInputStream(expected1));

            // verify read back.
            assertEquals("version1", expected1, read(repo.inputStream(id,
                    version1)));

            /*
             * verify read back of version0 is now an empty byte[] since that
             * version was deleted.
             */
            assertEquals("version0", new byte[]{}, read(repo.inputStream(id,
                    version0)));

            /*
             * verify that version0 is now marked as deleted.
             */
            {

                /*
                 * all metadata for the file up to (but excluding) the create
                 * time for version1.
                 */
                ITPS tps = repo.readMetadata(id, createTime1 - 1L);

                /*
                 * The version property for version0. This should have been
                 * overwritten to be deleted (a null) immediately before the new
                 * file version was created.
                 */
                ITPV tpv = tps.get(FileMetadataSchema.VERSION);

                assertEquals("version", null, tpv.getValue());

            }

        }

    }

    /**
     * Test of delete a file version verifies that the old version is marked as
     * deleted and that the data for that version are deleted as well. The test
     * also verifies that the deleted file version metadata and data remain
     * readable.
     * 
     * @todo test that the metadata and data are no longer readable after a
     *       suitable compacting merge.
     * 
     * @throws IOException
     */
    public void test_delete01() throws IOException {

        final String id = "test";

        final Map<String, Object> metadata = new HashMap<String, Object>();

        metadata.put(FileMetadataSchema.ID, id);

        metadata.put("foo", "bar");

        final int version0;
        final long createTime0;
        final byte[] expected0 = new byte[] { 1, 2, 3 };
        {

            version0 = repo.create(metadata);

            metadata.put(FileMetadataSchema.VERSION, Integer.valueOf(version0));

            assertEquals("version", 0, version0);

            RepositoryDocumentImpl doc = (RepositoryDocumentImpl) repo.read(id);

            createTime0 = doc.getVersionCreateTime();

            assertNotSame("createTime", 0L, createTime0);

            Map<String, Object> actual = doc.asMap();

            assertEquals("id", id, actual.get(FileMetadataSchema.ID));

            assertEquals("version", version0, actual
                    .get(FileMetadataSchema.VERSION));

            assertEquals("user property", "bar", actual.get("foo"));

            assertEquals("size", metadata.size(), actual.size());

            // write on the file version.
            repo.copyStream(id, version0, new ByteArrayInputStream(expected0));

            // verify read back.
            assertEquals("version0", expected0, read(repo.inputStream(id,
                    version0)));

        }

        /*
         * Delete the file version.
         * 
         * @todo test that double-delete has no effect (returns false, does (or
         * does not?) write another delete marker).
         */
        assertTrue(repo.delete(id) > 0);
        
        /*
         * Verify the file version history metadata.
         */

        ITPV[] a = repo.getAllVersionInfo(id);

        assertEquals(2, a.length);
        
        assertEquals("v0",0,((Integer)a[0].getValue()).intValue());

        assertNull("delete(v0)", a[1].getValue());

        /*
         * verify read back of version0 is now an empty byte[] since that
         * version was deleted.
         */
        assertEquals("version0", new byte[]{}, read(repo.inputStream(id,
                version0)));

        /*
         * Verify that you can still read back the file version data using a
         * historical view of the file data index whose commit time is less than
         * or equal to version1's timestamp. If it is equal then you will be
         * reading from the last state of the prior version. If it is less than
         * then you may be reading from some historical state of the prior
         * version.
         */
        {

            // the timestamp before the version was deleted.
            final long timestamp = a[1].getTimestamp();
            
            // verify read back.
            assertEquals("version0", expected0, read(repo.inputStream(id,
                    version0, -timestamp)));
            
        }
        
    }

}
