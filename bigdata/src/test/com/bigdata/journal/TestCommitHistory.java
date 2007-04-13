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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;

import com.bigdata.btree.BTree;

/**
 * Test the ability to get (exact match) and find (most recent less than or
 * equal to) historical commit records in a {@link Journal}. Also verifies that
 * a canonicalizing cache is maintained (you never obtain distinct concurrent
 * instances of the same commit record).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCommitHistory extends ProxyTestCase {

    /**
     * 
     */
    public TestCommitHistory() {
    }

    /**
     * @param name
     */
    public TestCommitHistory(String name) {
        super(name);
    }

    /**
     * Compare two {@link ICommitRecord}s for equality in their data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(ICommitRecord expected, ICommitRecord actual) {
        
        if (expected == null)
            assertNull("Expected actual to be null", actual);
        else
            assertNotNull("Expected actual to be non-null", actual);
        
        assertEquals("timestamp", expected.getTimestamp(), actual.getTimestamp());

        assertEquals("#roots", expected.getRootAddrCount(), actual.getRootAddrCount());
        
        final int n = expected.getRootAddrCount();
        
        for(int i=0; i<n; i++) {
        
            if(expected.getRootAddr(i) != actual.getRootAddr(i)) {
                
                assertEquals("rootAddr[" + i + "]", expected.getRootAddr(i),
                        actual.getRootAddr(i));
            }
            
        }
        
    }

    /**
     * Test that {@link Journal#getCommitRecord(long)} returns null if invoked
     * before anything has been committed.
     */
    public void test_behaviorBeforeAnythingIsCommitted() {

        Journal journal = new Journal(getProperties());

        assertNull(journal.getCommitRecord(journal.nextTimestamp()));
        
        journal.closeAndDelete();
        
    }
    
    /**
     * Test the ability to recover a {@link ICommitRecord} from the
     * {@link CommitRecordIndex}.
     */
    public void test_recoverCommitRecord() {
        
        Journal journal = new Journal(getProperties());

        /*
         * The first commit flushes the root leaves of some indices so we get
         * back a non-zero commit timestamp.
         */
        assertTrue(0L!=journal.commit());

        /*
         * A follow up commit in which nothing has been written should return a
         * 0L timestamp.
         */
        assertEquals(0L,journal.commit());
        
        journal.write(ByteBuffer.wrap(new byte[]{1,2,3}));
        
        final long commitTime1 = journal.commit();
        
        assertTrue(commitTime1!=0L);
        
        ICommitRecord commitRecord = journal.getCommitRecord(commitTime1);

        assertNotNull(commitRecord);
        
        assertNotNull(journal.getCommitRecord());
        
        assertEquals(commitTime1, journal.getCommitRecord().getTimestamp());
        
        assertEquals(journal.getCommitRecord(),commitRecord);
        
        journal.closeAndDelete();
        
    }
    
    /**
     * Tests whether the {@link CommitRecordIndex} is restart-safe.
     */
    public void test_commitRecordIndex_restartSafe() {
        
        Journal journal = new Journal(getProperties());
        
        if(!journal.isStable()) {
            
            // test only applies to restart-safe journals.
            
            journal.closeAndDelete();
            
            return;
            
        }

        /*
         * Write a record directly on the store in order to force a commit to
         * write a commit record (if you write directly on the store it will not
         * cause a state change in the root addresses, but it will cause a new
         * commit record to be written with a new timestamp).
         */
        
        // write some data.
        journal.write(ByteBuffer.wrap(new byte[]{1,2,3}));
        
        // commit the store.
        final long commitTime1 = journal.commit();
        
        assertTrue(commitTime1!=0L);
        
        ICommitRecord commitRecord1 = journal.getCommitRecord(commitTime1);

        assertEquals(commitTime1, commitRecord1.getTimestamp());
        
        assertEquals(commitTime1, journal.getRootBlockView().getCommitTimestamp());
        
        /*
         * Close and then re-open the store and verify that the correct commit
         * record is returned.
         */
        journal = reopenStore(journal);
        
        ICommitRecord commitRecord2 = journal.getCommitRecord();

        assertEquals(commitRecord1, commitRecord2);

        /*
         * Now recover the commit record by searching the commit record index.
         */
        ICommitRecord commitRecord3 = journal.getCommitRecord(commitTime1);

        assertEquals(commitRecord1, commitRecord3);
        assertEquals(commitRecord2, commitRecord3);
        
        journal.closeAndDelete();

    }
    
    /**
     * Tests for finding (less than or equal to) historical commit records using
     * the commit record index. This also tests restart-safety of the index with
     * multiple records (if the store is stable).
     */
    public void test_commitRecordIndex_find() {
        
        Journal journal = new Journal(getProperties());

        final int limit = 10;
        
        final long[] commitTime = new long[limit];

        final long[] commitRecordIndexAddrs = new long[limit];
        
        final ICommitRecord[] commitRecords = new ICommitRecord[limit];

        for(int i=0; i<limit; i++) {

            // write some data.
            journal.write(ByteBuffer.wrap(new byte[]{1,2,3}));
        
            // commit the store.
            commitTime[i] = journal.commit();
            
            assertTrue(commitTime[i]!=0L);

            if (i > 0)
                assertTrue(commitTime[i] > commitTime[i - 1]);
         
            commitRecordIndexAddrs[i] = journal.getRootBlockView().getCommitRecordIndexAddr();

            assertTrue(commitRecordIndexAddrs[i]!=0L);

            if (i > 0)
                assertTrue(commitRecordIndexAddrs[i] > commitRecordIndexAddrs[i - 1]);

            // get the current commit record.
            commitRecords[i] = journal.getCommitRecord();
            
            // test exact match on this timestamp.
            assertEquals(commitRecords[i],journal.getCommitRecord(commitTime[i]));
            
            if(i>0) {

                // test exact match on the prior timestamp.
                assertEquals(commitRecords[i-1],journal.getCommitRecord(commitTime[i-1]));
                
            }

            /*
             * Obtain a unique timestamp from the same source that the journal
             * is using to generate the commit timestamps. This ensures that
             * there will be at least one possible timestamp between each commit
             * timestamp.
             */
            final long ts = journal.nextTimestamp();
            
            assertTrue(ts>commitTime[i]);
            
        }
        
        if (journal.isStable()) {

            /*
             * Close and then re-open the store so that we will also be testing
             * restart-safety of the commit record index.
             */

            journal = reopenStore(journal);

        }

        /*
         * Verify the historical commit records on exact match (get).
         */
        {
            
            for( int i=0; i<limit; i++) {
                
                assertEquals(commitRecords[i], journal
                        .getCommitRecord(commitTime[i]));
                
            }
            
        }
        
        /*
         * Verify access to historical records on LTE search (find).
         * 
         * We ensured above that there is at least one possible timestamp value
         * between each pair of commit timestamps. We already verified that
         * timestamps that exactly match a known commit time return the
         * associated commit record.
         * 
         * Now we verify that timestamps which proceed a known commit time but
         * follow after any earlier commit time, return the proceeding commit
         * record (finds the most recent commit record having a commit time less
         * than or equal to the probe time).
         */
        
        {
            
            for( int i=1; i<limit; i++) {
                
                assertEquals(commitRecords[i - 1], journal
                        .getCommitRecord(commitTime[i] - 1));
                
            }

            /*
             * Verify a null return if we probe with a timestamp before any
             * commit time.
             */
            assertNull(journal.getCommitRecord(commitTime[0] - 1));
            
        }
        
        journal.closeAndDelete();
        
    }

    /**
     * Test verifies that exact match and find always return the same reference
     * for the same commit record (at least as long as the test holds a hard
     * reference to the commit record of interest).
     */
    public void test_canonicalizingCache() {
        
        Journal journal = new Journal(getProperties());

        /*
         * The first commit flushes the root leaves of some indices so we get
         * back a non-zero commit timestamp.
         */
        final long commitTime0 = journal.commit();

        assertTrue(commitTime0 != 0L);
        
        /*
         * obtain the commit record for that commit timestamp.
         */
        ICommitRecord commitRecord0 = journal.getCommitRecord(commitTime0);

        // should be the same instance that is held by the journal.
        assertTrue(commitRecord0 == journal.getCommitRecord());

        /*
         * write a record on the store, commit the store, and note the commit
         * time.
         */
        journal.write(ByteBuffer.wrap(new byte[]{1,2,3}));
        
        final long commitTime1 = journal.commit();
        
        assertTrue(commitTime1!=0L);
        
        /*
         * obtain the commit record for that commit timestamp.
         */
        ICommitRecord commitRecord1 = journal.getCommitRecord(commitTime1);

        // should be the same instance that is held by the journal.
        assertTrue(commitRecord1 == journal.getCommitRecord());
        
        /*
         * verify that we obtain the same instance with find as with an exact
         * match.
         */ 
        
        assertTrue(commitRecord0 == journal.getCommitRecord(commitTime1 - 1));
        
        assertTrue(commitRecord1 == journal.getCommitRecord(commitTime1 + 0 ));

        assertTrue(commitRecord1 == journal.getCommitRecord(commitTime1 + 1));

        journal.closeAndDelete();

    }
    
    /**
     * Test of the canonicalizing object cache used to prevent distinct
     * instances of a historical index from being created. The test also
     * verifies that the historical named index is NOT the same instance as the
     * current unisolated index by that name.
     */
    public void test_objectCache() {
        
        Journal journal = new Journal(getProperties());

        final String name = "abc";

        final BTree liveIndex = (BTree) journal.registerIndex(name);
        
        final long commitTime0 = journal.commit();

        assertTrue(commitTime0 != 0L);
        
        /*
         * obtain the commit record for that commit timestamp.
         */
        ICommitRecord commitRecord0 = journal.getCommitRecord(commitTime0);

        // should be the same instance that is held by the journal.
        assertTrue(commitRecord0 == journal.getCommitRecord());

        /*
         * verify that a request for last committed state the named index
         * returns a different instance than the "live" index.
         */
        
        final BTree historicalIndex0 = (BTree)journal.getIndex(name, commitRecord0);
        
        assertTrue(liveIndex != historicalIndex0);

        // re-request is still the same object.
        assertTrue(historicalIndex0 == (BTree) journal.getIndex(name,
                commitRecord0));
        
        /*
         * The re-load address for the live index as of that commit record.
         */
        final long liveIndexAddr0 = liveIndex.getMetadata().getMetadataAddr();
        
        /*
         * write a record on the store, commit the store, and note the commit
         * time.
         */
        journal.write(ByteBuffer.wrap(new byte[]{1,2,3}));
        
        final long commitTime1 = journal.commit();
        
        assertTrue(commitTime1!=0L);
        
        /*
         * we did NOT write on the named index, so its address in the store must
         * not change.
         */
        assertEquals(liveIndexAddr0,liveIndex.getMetadata().getMetadataAddr());
        
        // obtain the commit record for that commit timestamp.
        ICommitRecord commitRecord1 = journal.getCommitRecord(commitTime1);

        // should be the same instance that is held by the journal.
        assertTrue(commitRecord1 == journal.getCommitRecord());

        /*
         * verify that we get the same historical index object for the new
         * commit record since the index state was not changed and it will be
         * reloaded from the same address.
         */
        assertTrue(historicalIndex0 == (BTree) journal.getIndex(name,
                commitRecord1));

        // re-request is still the same object.
        assertTrue(historicalIndex0 == (BTree) journal.getIndex(name,
                commitRecord0));

        // re-request is still the same object.
        assertTrue(historicalIndex0 == (BTree) journal.getIndex(name,
                commitRecord1));

        /*
         * Now write on the live index and commit. verify that there is a new
         * historical index available for the new commit record, that it is not
         * the same as the live index, and that it is not the same as the
         * previous historical index (which should still be accessible).
         */
        
        // live index is the same reference.
        assertTrue(liveIndex == journal.getIndex(name));
        
        liveIndex.insert(new byte[]{1,2}, new byte[]{1,2});
        
        final long commitTime2 = journal.commit();
        
        // obtain the commit record for that commit timestamp.
        ICommitRecord commitRecord2 = journal.getCommitRecord(commitTime2);

        // should be the same instance that is held by the journal.
        assertTrue(commitRecord2 == journal.getCommitRecord());
        
        // must be a different index object.
        
        BTree historicalIndex2 = (BTree) journal.getIndex(name, commitRecord2);
        
        assertTrue(historicalIndex0 != historicalIndex2);

        // the live index must be distinct from the historical index.
        assertTrue(liveIndex != historicalIndex2); 
        
        journal.closeAndDelete();

    }
    
}
