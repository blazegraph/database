/**

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
 * Created on Feb 3, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;

/**
 * Test suite for restart-safety of {@link BTree}s backed by an
 * {@link IJournal}.
 * 
 * @todo explore flushing the indexCache as if a GC had occurred after a commit?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCommitList extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestCommitList() {
    }

    /**
     * @param name
     */
    public TestCommitList(String name) {
        super(name);
    }

//    public Properties getProperties() {
//
//        if (properties == null) {
//
//            properties = super.getProperties();
//
//            // we need to use a persistent mode of the journal (not transient).
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
//                    .toString());
//
//            properties.setProperty(Options.CREATE_TEMP_FILE, "true");
//
//            properties.setProperty(Options.DELETE_ON_EXIT,"true");
//
//        }
//
//        return properties;
//
//    }
//
//    private Properties properties;
    
//    /**
//     * Re-open the same backing store.
//     * 
//     * @param store
//     *            the existing store.
//     * 
//     * @return A new store.
//     * 
//     * @exception Throwable
//     *                if the existing store is not closed, e.g., from failure to
//     *                obtain a file lock, etc.
//     */
//    protected Journal reopenStore(Journal store) {
//        
//        // close the store.
//        store.close();
//        
//        Properties properties = (Properties)getProperties().clone();
//        
//        // Turn this off now since we want to re-open the same store.
//        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
//        
//        // The backing file that we need to re-open.
//        File file = store.getFile();
//        
//        assertNotNull(file);
//        
//        // Set the file property explictly.
//        properties.setProperty(Options.FILE,file.toString());
//        
//        return new Journal( properties );
//        
//    }

    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(int branchingFactor, Journal journal) {

        final BTree btree;
        {
            
            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor(branchingFactor);
            
            btree = BTree.create(journal, metadata);
            
        }

        return btree;
            
    }

    /**
     * Test verifies that a named index is found on the commit list when (a) it
     * is newly created; and (b) when an entry is written on the index.
     * <p>
     * Note: {@link #test_commitList_001_restartSafe()} SHOULD be maintained in
     * parallel with this test -- it additionally verifies that the changes are
     * made restart safe by the commit.
     * 
     * @see #test_commitList_001_restartSafe()
     */
    public void test_commitList_001() {

        Journal journal = new Journal(getProperties());

        final String name = "abc";

        // register index.
        BTree ndx = (BTree)journal.registerIndex(name);
        
        // verify index was flushed to the backing store.
        assertFalse(ndx.needsCheckpoint());
        
        // verify a new index is on the commit list.
        assertTrue(journal.name2Addr.willCommit(name));

        // commit.
        journal.commit();

        // same index object after the commit.
        assertEquals(ndx,journal.getIndex(name));

        // no longer on the commit list.
        assertFalse(journal.name2Addr.willCommit(name));

        // write an entry on the index - the index becomes dirty and should
        // show up on the commit list.
        ndx.insert(new byte[]{1,2,3}, new byte[]{1,2,3});
        
        // verify on the commit list.
        assertTrue(journal.name2Addr.willCommit(name));
        
        // commit.
        journal.commit();

        // same index object after the commit.
        assertEquals(ndx,journal.getIndex(name));
        
        // no longer on the commit list.
        assertFalse(journal.name2Addr.willCommit(name));
        
        // verify entry written by the commit.
        assertEquals(new byte[]{1,2,3},(byte[])ndx.lookup(new byte[]{1,2,3}));
        
        // still not on the commit list.
        assertFalse(journal.name2Addr.willCommit(name));
        
        journal.destroy();
        
    }
    
    /**
     * A variant of {@link #test_commitList_001()} in which we re-open the store
     * after each commit and verify that the record written can be read so that
     * we know that the commit was made restart safe.
     */
    public void test_commitList_001_restartSafe() {
        
        Journal journal = new Journal(getProperties());

        try {

            final String name = "abc";

            // register index.
            IIndex ndx = journal.registerIndex(name);

            // verify a new index is on the commit list.
            assertTrue(journal.name2Addr.willCommit(name));

            // commit.
            journal.commit();

            if (journal.isStable()) {

                // re-open the store.
                journal = reopenStore(journal);

                // get the index object from the re-opened store..
                ndx = journal.getIndex(name);

                assertNotNull(ndx);

                // not on the commit list.
                assertFalse(journal.name2Addr.willCommit(name));

                // write an entry on the index - the index becomes dirty and
                // should
                // show up on the commit list.
                ndx.insert(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 });

                // verify on the commit list.
                assertTrue(journal.name2Addr.willCommit(name));

                // commit.
                journal.commit();

                // no longer on the commit list.
                assertFalse(journal.name2Addr.willCommit(name));

                // re-open the store.
                journal = reopenStore(journal);

                // get the index object from the re-opened store..
                ndx = journal.getIndex(name);

                assertNotNull(ndx);

                // not on the commit list.
                assertFalse(journal.name2Addr.willCommit(name));

                // verify entry written by the commit.
                assertEquals(new byte[] { 1, 2, 3 }, (byte[]) ndx
                        .lookup(new byte[] { 1, 2, 3 }));

                // still not on the commit list.
                assertFalse(journal.name2Addr.willCommit(name));

            }

        } finally {

            journal.destroy();

        }
        
    }

    /**
     * Test creates an index, writes an entry on the index, and then commits the
     * index (so writing on the index immediately after it is created without an
     * intervening commit). The test then re-opens the store and verifies that
     * the data are restart safe.
     */
    public void test_commitList002() {
        
        Journal journal = new Journal(getProperties());

        try {

            final String name = "abc";

            // register index.
            IIndex ndx = journal.registerIndex(name);

            // verify a new index is on the commit list.
            assertTrue(journal.name2Addr.willCommit(name));

            // write an entry on the index.
            ndx.insert(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 });

            // verify on the commit list.
            assertTrue(journal.name2Addr.willCommit(name));

            // commit.
            journal.commit();

            // no longer on the commit list.
            assertFalse(journal.name2Addr.willCommit(name));

            if (journal.isStable()) {

                // re-open the store.
                journal = reopenStore(journal);

                // get the index object from the re-opened store..
                ndx = journal.getIndex(name);

                assertNotNull(ndx);

                // not on the commit list.
                assertFalse(journal.name2Addr.willCommit(name));

                // verify entry written by the commit.
                assertEquals(new byte[] { 1, 2, 3 }, (byte[]) ndx
                        .lookup(new byte[] { 1, 2, 3 }));

                // still not on the commit list.
                assertFalse(journal.name2Addr.willCommit(name));

            }

        } finally {

            journal.destroy();

        }

    }
    
}
