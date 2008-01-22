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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bigdata.repo.BigdataRepository.MetadataSchema;
import com.bigdata.repo.BigdataRepository.RepositoryDocumentImpl;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.ITPV;

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

        metadata.put(MetadataSchema.ID, id);

        metadata.put("foo", "bar");

        final int version = repo.create(metadata);

        metadata.put(MetadataSchema.VERSION, Integer.valueOf(version));

        assertEquals("version", 0, version);

        RepositoryDocumentImpl doc = (RepositoryDocumentImpl) repo.read(id);

        assertNotSame("createTime", 0L, doc.createTime());

        Map<String, Object> actual = doc.asMap();

        assertEquals("id", id, actual.get(MetadataSchema.ID));

        assertEquals("version", version, actual.get(MetadataSchema.VERSION));

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

        metadata.put(MetadataSchema.ID, id);

        metadata.put("foo", "bar");

        final int version0;
        final long createTime0;
        final byte[] expected0 = new byte[] { 1, 2, 3 };
        {

            version0 = repo.create(metadata);

            metadata.put(MetadataSchema.VERSION, Integer.valueOf(version0));

            assertEquals("version", 0, version0);

            RepositoryDocumentImpl doc = (RepositoryDocumentImpl) repo.read(id);

            createTime0 = doc.createTime();

            assertNotSame("createTime", 0L, createTime0);

            Map<String, Object> actual = doc.asMap();

            assertEquals("id", id, actual.get(MetadataSchema.ID));

            assertEquals("version", version0, actual
                    .get(MetadataSchema.VERSION));

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

            version1 = repo.update(metadata);

            metadata.put(MetadataSchema.VERSION, Integer.valueOf(version1));

            assertEquals("version", 1, version1);

            RepositoryDocumentImpl doc = (RepositoryDocumentImpl) repo.read(id);

            createTime1 = doc.createTime();

            assertNotSame("createTime", 0L, createTime1);

            assertNotSame("createTime", createTime0, createTime1);

            Map<String, Object> actual = doc.asMap();

            assertEquals("id", id, actual.get(MetadataSchema.ID));

            assertEquals("version", version1, actual
                    .get(MetadataSchema.VERSION));

            assertEquals("user property", "baz", actual.get("foo"));

            assertEquals("size", metadata.size(), actual.size());

            // write on the file version.
            repo.copyStream(id, version1, new ByteArrayInputStream(expected1));

            // verify read back.
            assertEquals("version1", expected1, read(repo.inputStream(id,
                    version1)));

            /*
             * verify read back of version0 - it was deleted.
             * 
             * FIXME Verify that you can still read back the file version data
             * using a historical view of the file data index whose commit time
             * is less than version1's timestamp. This introduces some
             * interesting twists - the sparse row store is willing to assign
             * timestamps that proceed the probably commit timestamp that will
             * be applied during the group commit - so want can people know when
             * they try to read the deleted file version? Get the historical
             * view having a commit time strictly less than the timestamp when
             * the version was deleted? That logic will have to be encapsulated
             * for mere mortals.
             * 
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
                ITPV tpv = tps.get(MetadataSchema.VERSION);

                assertEquals("version", null, tpv.getValue());

            }

        }

    }

    /**
     * @todo test delete of the metadata and data for a file (in which order
     *       should this operation take place?) make sure that the old version
     *       is marked as deleted and that the data for that version are deleted
     *       as well.
     */
    public void test_delete01() {

        fail("write test");

    }

}
