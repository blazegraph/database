/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 3, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.SimpleEntry;

/**
 * Test suite for restart-safety of {@link BTree}s backed by an
 * {@link IJournal}.
 * 
 * @todo explore flushing the indexCache as if a GC had occurred after a commit?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCommitList extends AbstractBTreeTestCase {

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

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            // we need to use a persistent mode of the journal (not transient).
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());

            properties.setProperty(Options.CREATE_TEMP_FILE, "true");

            properties.setProperty(Options.DELETE_ON_EXIT,"true");

        }

        return properties;

    }

    private Properties properties;
    
    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is not closed, e.g., from failure to
     *                obtain a file lock, etc.
     */
    protected Journal reopenStore(Journal store) {
        
        // close the store.
        store.close();
        
        Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        File file = store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explictly.
        properties.setProperty(Options.FILE,file.toString());
        
        return new Journal( properties );
        
    }

    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * The serializer requires that values in leaves are {@link SimpleEntry}
     * objects.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(int branchingFactor, Journal journal) {

        BTree btree = new BTree(journal, branchingFactor, UUID.randomUUID(),
                SimpleEntry.Serializer.INSTANCE);

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
        IIndex ndx = journal.registerIndex(name);
        
        // verify a new index is on the commit list.
        assertTrue(journal.name2Addr.willCommit(ndx));

        // commit.
        journal.commit();

        // same index object after the commit.
        assertEquals(ndx,journal.getIndex(name));

        // no longer on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));

        // write an entry on the index - the index becomes dirty and should
        // show up on the commit list.
        ndx.insert(new byte[]{1,2,3}, new byte[]{1,2,3});
        
        // verify on the commit list.
        assertTrue(journal.name2Addr.willCommit(ndx));
        
        // commit.
        journal.commit();

        // same index object after the commit.
        assertEquals(ndx,journal.getIndex(name));
        
        // no longer on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));
        
        // verify entry written by the commit.
        assertEquals(new byte[]{1,2,3},(byte[])ndx.lookup(new byte[]{1,2,3}));
        
        // still not on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));
        
        journal.closeAndDelete();
        
    }
    
    /**
     * A variant of {@link #test_commitList_001()} in which we re-open the store
     * after each commit and verify that the record written can read so that we
     * know that the commit was made restart safe.
     */
    public void test_commitList_001_restartSafe() {

        Journal journal = new Journal(getProperties());

        final String name = "abc";

        // register index.
        IIndex ndx = journal.registerIndex(name);
        
        // verify a new index is on the commit list.
        assertTrue(journal.name2Addr.willCommit(ndx));

        // commit.
        journal.commit();

        // re-open the store.
        journal = reopenStore(journal);
        
        // get the index object from the re-opened store..
        ndx = journal.getIndex(name);

        assertNotNull(ndx);
        
        // not on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));

        // write an entry on the index - the index becomes dirty and should
        // show up on the commit list.
        ndx.insert(new byte[]{1,2,3}, new byte[]{1,2,3});
        
        // verify on the commit list.
        assertTrue(journal.name2Addr.willCommit(ndx));
        
        // commit.
        journal.commit();

        // no longer on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));

        // re-open the store.
        journal = reopenStore(journal);
        
        // get the index object from the re-opened store..
        ndx = journal.getIndex(name);

        assertNotNull(ndx);

        // not on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));
        
        // verify entry written by the commit.
        assertEquals(new byte[]{1,2,3},(byte[])ndx.lookup(new byte[]{1,2,3}));
        
        // still not on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));

        journal.closeAndDelete();
        
    }

    /**
     * Test creates an index, writes an entry on the index, and then commits the
     * index (so writing on the index immediately after it is created without an
     * intervening commit). The test then re-opens the store and verifies that
     * the data are restart safe.
     */
    public void test_commitList002() {
        
        Journal journal = new Journal(getProperties());

        final String name = "abc";

        // register index.
        IIndex ndx = journal.registerIndex(name);
        
        // verify a new index is on the commit list.
        assertTrue(journal.name2Addr.willCommit(ndx));

        // write an entry on the index.
        ndx.insert(new byte[]{1,2,3}, new byte[]{1,2,3});
        
        // verify on the commit list.
        assertTrue(journal.name2Addr.willCommit(ndx));
        
        // commit.
        journal.commit();

        // no longer on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));

        // re-open the store.
        journal = reopenStore(journal);
        
        // get the index object from the re-opened store..
        ndx = journal.getIndex(name);

        assertNotNull(ndx);

        // not on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));
        
        // verify entry written by the commit.
        assertEquals(new byte[]{1,2,3},(byte[])ndx.lookup(new byte[]{1,2,3}));
        
        // still not on the commit list.
        assertFalse(journal.name2Addr.willCommit(ndx));

        journal.closeAndDelete();

    }
    
}
