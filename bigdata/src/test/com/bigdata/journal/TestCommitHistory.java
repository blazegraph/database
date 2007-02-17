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
import java.util.Properties;

/**
 * Test the ability to get (exact match) and find (most recent less than
 * or equal to) historical commit records in a {@link Journal}.
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

        assertNull(journal.getCommitRecord(journal.timestampFactory.nextTimestamp()));
        
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
        
    }
    
    /**
     * Tests whether the {@link CommitRecordIndex} is restart-safe.
     */
    public void test_commitRecordIndex_restartSafe() {
        
        Properties properties = getProperties();
        
        properties.setProperty(Options.DELETE_ON_CLOSE,"false");
        
        Journal journal = new Journal(properties);
        
        if(!journal.isStable()) {
            
            // test only applies to restart-safe journals.
            
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
        journal.close();
        
        journal = new Journal(properties);
        
        ICommitRecord commitRecord2 = journal.getCommitRecord();

        assertEquals(commitRecord1, commitRecord2);

        /*
         * Now recover the commit record by searching the commit record index.
         */
        ICommitRecord commitRecord3 = journal.getCommitRecord(commitTime1);

        assertEquals(commitRecord1, commitRecord3);
        assertEquals(commitRecord2, commitRecord3);

    }
    
    /**
     * Tests for finding (less than or equal to) historical commit records using
     * the commit record index. This also tests restart-safety of the index with
     * multiple records (if the store is stable).
     */
    public void test_commitRecordIndex_find() {
        
        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE,"false");
        
        Journal journal = new Journal(properties);

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
            final long ts = journal.timestampFactory.nextTimestamp();
            
            assertTrue(ts>commitTime[i]);
            
        }
        
        if (journal.isStable()) {

            /*
             * Close and then re-open the store so that we will also be testing
             * restart-safety of the commit record index.
             */
            journal.close();

            journal = new Journal(properties);

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
        
    }
    
}
