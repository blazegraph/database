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
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Tuple;
import com.bigdata.isolation.IsolatedFusedView;

/**
 * Test suite for fully-isolated read-write transactions.
 * 
 * @todo verify with writes on multiple indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTx extends ProxyTestCase<Journal> {
    
    public TestTx() {
    }

    public TestTx(String name) {
        
        super(name);
        
    }

//    /**
//     * Writes some interesting constants on {@link System#err}.
//     */
//    public void test_constants() {
//        
//        System.err.println("min : "+new Date(Long.MIN_VALUE));
//        System.err.println("min1: "+new Date(Long.MIN_VALUE+1));
//        System.err.println("-1L : "+new Date(-1));
//        System.err.println(" 0L : "+new Date(0L));
//        System.err.println("max1: "+new Date(Long.MAX_VALUE-1));
//        System.err.println("max : "+new Date(Long.MAX_VALUE));
//        
//    }
    
    /**
     * Test verifies that a transaction may start when there are (a) no commits
     * on the journal; and (b) no indices have been registered.
     * <P>
     * Note: The transaction will be unable to isolate an index if the index has
     * not been registered already by an unisolated operation.
     */
    public void test_noIndicesRegistered() {

        final Journal journal = getStore();

        try {

            journal.commit();

            final long tx = journal.newTx(ITx.UNISOLATED);

            /*
             * nothing written on this transaction.
             */

            // commit.
            assertEquals(0L, journal.commit(tx));

        } finally {

            journal.destroy();
            
        }
        
    }

    /**
     * Verify that an index is not visible in the tx unless the native
     * transaction in which it is registered has already committed before the tx
     * starts.
     */
    public void test_indexNotVisibleUnlessCommitted() {
       
        final Journal journal = getStore();

        try {
        
            final String name = "abc";

            // register index in unisolated scope, but do not commit yet.
            {

                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

            }

            // start tx1.
            final long tx1 = journal.newTx(ITx.UNISOLATED);

            // the index is not visible in tx1.
            assertNull(journal.getIndex(name, tx1));

            // do unisolated commit.
            assertNotSame(0L, journal.commit());

            // start tx2.
            final long tx2 = journal.newTx(ITx.UNISOLATED);

            // the index still is not visible in tx1.
            assertNull(journal.getIndex(name, tx1));

            // the index is visible in tx2.
            assertNotNull(journal.getIndex(name, tx2));

            journal.abort(tx1);

            journal.abort(tx2);

        } finally {

            journal.destroy();

        }
        
    }

    /**
     * Test verifies that you always get the same object back when you ask for
     * an isolated named index. This is important both to conserve resources and
     * since the write set is in the isolated index -- you lose it and it is
     * gone.
     */
    public void test_sameIndexObject() {

        final Journal journal = getStore();
        
        try {

            final String name = "abc";

            {

                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                journal.commit();

            }

            final long tx1 = journal.newTx(ITx.UNISOLATED);

            final IIndex ndx1 = journal.getIndex(name, tx1);

            assertNotNull(ndx1);

            final long tx2 = journal.newTx(ITx.UNISOLATED);

            final IIndex ndx2 = journal.getIndex(name, tx2);

            assertTrue(tx1 != tx2);

            assertTrue(ndx1 != ndx2);

            assertNotNull(ndx2);

            assertTrue(ndx1 == journal.getIndex(name, tx1));

            assertTrue(ndx2 == journal.getIndex(name, tx2));

        } finally {

            journal.destroy();

        }
        
    }
    
    /**
     * Create a journal, setup an index, write an entry on that index, and
     * commit the store. Setup a transaction and verify that we can isolate that
     * index and read the written value. Write a value on the unisolated index
     * and verify that it is not visible within the transaction.
     */
    public void test_readIsolation() {
        
        final Journal journal = getStore();

        try {

            final String name = "abc";

            final byte[] k1 = new byte[] { 1 };
            final byte[] k2 = new byte[] { 2 };

            final byte[] v1 = new byte[] { 1 };
            final byte[] v2 = new byte[] { 2 };

            {

                /*
                 * register the index, write an entry on the unisolated index,
                 * and commit the journal.
                 */

                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                IIndex index = journal.getIndex(name);

                assertNull(index.insert(k1, v1));

                assertNotSame(0L, journal.commit());

            }

            final long tx1 = journal.newTx(ITx.UNISOLATED);

            {

                /*
                 * verify that the write is visible in a transaction that starts
                 * after the commit.
                 */

                IIndex index = journal.getIndex(name, tx1);

                assertTrue(index.contains(k1));

                assertEquals(v1, (byte[]) index.lookup(k1));

            }

            {

                /*
                 * obtain the unisolated index and write another entry and
                 * commit the journal.
                 */

                IIndex index = journal.getIndex(name);

                assertNull(index.insert(k2, v2));

                assertNotSame(0L, journal.commit());

            }

            {

                /*
                 * verify that the entry written on the unisolated index is not
                 * visible to the transaction that started before that write.
                 */

                IIndex index = journal.getIndex(name, tx1);

                assertTrue(index.contains(k1));
                assertFalse(index.contains(k2));

            }

            final long tx2 = journal.newTx(ITx.UNISOLATED);

            {

                /*
                 * start another transaction and verify that the 2nd committed
                 * write is now visible to that transaction.
                 */

                IIndex index = journal.getIndex(name, tx2);

                assertTrue(index.contains(k1));
                assertTrue(index.contains(k2));

            }

            journal.abort(tx1);

            journal.abort(tx2);

        } finally {

            journal.destroy();

        }
        
    }

    /**
     * Test verifies that an isolated write is visible inside of a transaction
     * (tx1) but not in a concurrent transaction (tx2) and not in the unisolated
     * index until the tx1 commits. Once the tx1 commits, the write is visible
     * in the unisolated index. The write never becomes visible in tx2. If tx2
     * attempts to write a value under the same key then a write-write conflict
     * is reported and validation fails.
     */
    public void test_writeIsolation() {

        final Journal journal = getStore();

        try {

            final String name = "abc";

            final byte[] k1 = new byte[] { 1 };

            final byte[] v1 = new byte[] { 1 };
            final byte[] v1a = new byte[] { 1, 1 };

            {

                /*
                 * register an index and commit the journal.
                 */

                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                assertNotSame(0L, journal.commit());

            }

            /*
             * create two transactions.
             */

            final long tx1 = journal.newTx(ITx.UNISOLATED);

            final long tx2 = journal.newTx(ITx.UNISOLATED);

            assertNotSame(tx1, tx2);

            assertTrue(Math.abs(tx1) >= journal.getRootBlockView()
                    .getLastCommitTime());

            assertTrue(Math.abs(tx2) > Math.abs(tx1));

            {

                /*
                 * write an entry in tx1. verify that the entry is not visible
                 * in the unisolated index or in the index as isolated by tx2.
                 */

                IsolatedFusedView ndx1 = (IsolatedFusedView) journal.getIndex(
                        name, tx1);

                assertFalse(ndx1.contains(k1));

                assertNull(ndx1.insert(k1, v1));

                // existence check in tx1.
                assertTrue(ndx1.contains(k1));

                // not visible in the other tx.
                assertFalse(journal.getIndex(name, tx2).contains(k1));

                // not visible in the unisolated index.
                assertFalse(journal.getIndex(name).contains(k1));

                /*
                 * commit tx1. verify that the write is still not visible in tx2
                 * but that it is now visible in the unisolated index.
                 */

                // commit tx1.
                final long commitTime1 = journal.commit(tx1);
                assertNotSame(0L, commitTime1);

                // still not visible in the other tx.
                assertFalse(journal.getIndex(name, tx2).contains(k1));

                // but now visible in the unisolated index.
                assertTrue(journal.getIndex(name).contains(k1));

                // check the version timestamp in the unisolated index.
                {

                    final BTree btree = ((BTree) journal.getIndex(name));

                    final ITuple tuple = btree.lookup(k1, new Tuple(btree,
                            IRangeQuery.ALL));

                    assertNotNull(tuple);

                    assertFalse(tuple.isDeletedVersion());

                    // FIXME verify the revision timestamp!
//                    assertEquals("versionTimestamp", commitTime1, tuple
//                            .getVersionTimestamp());

                }

                /*
                 * write a conflicting entry in tx2 and verify that validation
                 * of tx2 fails.
                 */

                assertNull(journal.getIndex(name, tx2).insert(k1, v1a));

                // check the version counter in tx2.
                {

                    final IsolatedFusedView isolatedView = (IsolatedFusedView) journal
                            .getIndex(name, tx2);

                    final BTree btree = ((BTree) journal.getIndex(name));

                    Tuple tuple = btree.lookup(k1, new Tuple(btree,
                            IRangeQuery.ALL));

                    tuple = isolatedView.getWriteSet().lookup(k1, tuple);

                    assertNotNull(tuple);

                    assertFalse(tuple.isDeletedVersion());

                    // FIXME verify the revision timestamp!
//                    assertEquals("versionTimestamp", tx2, tuple
//                            .getVersionTimestamp());

                }

//                ITx tmp = journal.getTx(tx2);

                try {

                    journal.commit(tx2);

                    fail("Expecting: " + ValidationError.class);

                } catch (ValidationError ex) {

                    System.err.println("Ignoring expected exception: " + ex);

                }

//                assertTrue(tmp.isAborted());

            }

        } finally {

            journal.destroy();

        }
        
    }

    //
    // Delete object.
    //

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0 and then
     * another version (v1) is written on tx0 under the same key. Both
     * transactions prepare and commit. The end state is that (id0,v1) is
     * visible in the database after the commit.
     */
    public void test_delete001() {

        final Journal journal = getStore();

        try {

            final String name = "abc";

            {
                /*
                 * register an index and commit the journal.
                 */
                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                journal.commit();

            }

            /*
             * create transactions.
             */

            final long tx0 = journal.newTx(ITx.UNISOLATED);

            final long tx1 = journal.newTx(ITx.UNISOLATED);

            assertNotSame(tx0, tx1);

            assertTrue(Math.abs(tx0) >= journal.getRootBlockView().getLastCommitTime());

            assertTrue(Math.abs(tx1) > Math.abs(tx0));

            /*
             * Write v0 on tx0.
             */
            final byte[] id0 = new byte[] { 0 };
            final byte[] v0 = getRandomData().array();

            journal.getIndex(name, tx0).insert(id0, v0);

            assertEquals(v0, journal.getIndex(name, tx0).lookup(id0));

            /*
             * Verify that the version does NOT show up in a concurrent
             * transaction.
             */
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // delete the version.
            assertEquals(v0, journal.getIndex(name, tx0).remove(id0));

            // no longer visible in that transaction.
            assertFalse(journal.getIndex(name, tx0).contains(id0));

            /*
             * Test delete after delete (succeeds, but returns null).
             */
            assertNull(journal.getIndex(name, tx0).remove(id0));

            /*
             * Test write after delete (succeeds, returning null).
             */
            final byte[] v1 = getRandomData().array();
            assertNull(journal.getIndex(name, tx0).insert(id0, v1));

            // Still not visible in concurrent transaction.
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // Still not visible in global scope.
            assertFalse(journal.getIndex(name).contains(id0));

            // Prepare and commit tx0.
            assertNotSame(0L, journal.commit(tx0));

            // Still not visible in concurrent transaction.
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // Now visible in global scope.
            assertTrue(journal.getIndex(name).contains(id0));

            // Prepare and commit tx1 (no writes).
            assertEquals(0L, journal.commit(tx1));

            // Still visible in global scope.
            assertTrue(journal.getIndex(name).contains(id0));

        } finally {

            journal.destroy();

        }
    
    }

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0 and then
     * another version (v1) is written on tx0 under the same key and isolation
     * is re-verified. Finally, v1 is deleted from tx0. Both transactions
     * prepare and commit. The end state is that no entry for id0 is visible in
     * the database after the commit.
     */
    public void test_delete002() {

        final Journal journal = getStore();

        try {

            final String name = "abc";

            {
                /*
                 * register an index and commit the journal.
                 */
                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                journal.commit();

            }

            /*
             * create transactions.
             */

            final long tx0 = journal.newTx(ITx.UNISOLATED);

            final long tx1 = journal.newTx(ITx.UNISOLATED);

            assertNotSame(tx0, tx1);

            assertTrue(Math.abs(tx0) >= journal.getRootBlockView().getLastCommitTime());

            assertTrue(Math.abs(tx1) > Math.abs(tx0));

            /*
             * Write v0 on tx0.
             */
            final byte[] id0 = new byte[] { 1 };
            final byte[] v0 = getRandomData().array();
            journal.getIndex(name, tx0).insert(id0, v0);
            assertEquals(v0, journal.getIndex(name, tx0).lookup(id0));

            /*
             * Verify that the version does NOT show up in a concurrent
             * transaction.
             */
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // delete the version.
            assertEquals(v0, (byte[]) journal.getIndex(name, tx0).remove(id0));

            // no longer visible in that transaction.
            assertFalse(journal.getIndex(name, tx0).contains(id0));

            /*
             * Test delete after delete (succeeds, but returns null).
             */
            assertNull(journal.getIndex(name, tx0).remove(id0));

            /*
             * Test write after delete (succeeds, returning null).
             */
            final byte[] v1 = getRandomData().array();
            assertNull(journal.getIndex(name, tx0).insert(id0, v1));

            // Still not visible in concurrent transaction.
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // Still not visible in global scope.
            assertFalse(journal.getIndex(name).contains(id0));

            /*
             * Delete v1.
             */
            assertEquals(v1, (byte[]) journal.getIndex(name, tx0).remove(id0));

            // Still not visible in concurrent transaction.
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // Still not visible in global scope.
            assertFalse(journal.getIndex(name).contains(id0));

            /*
             * Prepare and commit tx0.
             * 
             * Note: We MUST NOT propagate a delete marker onto the unisolated
             * index since no entry for that key is visible was visible when the
             * tx0 began.
             */
            assertNotSame(0L, journal.commit(tx0));

            // Still not visible in concurrent transaction.
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // Still not visible in global scope.
            assertFalse(journal.getIndex(name).contains(id0));

            // Prepare and commit tx1 (no writes).
            assertEquals(0L, journal.commit(tx1));

            // Still not visible in global scope.
            assertFalse(journal.getIndex(name).contains(id0));

        } finally {

            journal.destroy();

        }

    }

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0. Another
     * version (v1) is written on tx1 under the same key and isolation is
     * re-verified. Both transactions prepare and commit. Since no entry for id0
     * was pre-existing in the global scope the delete in tx0 does not propagate
     * into the global scope and the end state is that the write (id0,v1) from
     * tx1 is visible in the database after the commit.
     */
    public void test_delete003() {

        final Journal journal = getStore();

        try {

            final String name = "abc";

            {
                /*
                 * register an index and commit the journal.
                 */
                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                journal.commit();

            }

            /*
             * create transactions.
             */

            final long tx0 = journal.newTx(ITx.UNISOLATED);

            final long tx1 = journal.newTx(ITx.UNISOLATED);

            assertNotSame(tx0, tx1);

            assertTrue(Math.abs(tx0) >= journal.getRootBlockView()
                    .getLastCommitTime());

            assertTrue(Math.abs(tx1) > Math.abs(tx0));

            /*
             * Write v0 on tx0.
             */
            final byte[] id0 = new byte[] { 1 };
            final byte[] v0 = getRandomData().array();
            journal.getIndex(name, tx0).insert(id0, v0);
            assertEquals(v0, journal.getIndex(name, tx0).lookup(id0));

            /*
             * Verify that the version does NOT show up in a concurrent
             * transaction.
             */
            assertFalse(journal.getIndex(name, tx1).contains(id0));

            // delete the version.
            assertEquals(v0, (byte[]) journal.getIndex(name, tx0).remove(id0));

            // no longer visible in that transaction.
            assertFalse(journal.getIndex(name, tx0).contains(id0));

            /*
             * write(id0,v1) in tx1.
             */
            final byte[] v1 = getRandomData().array();
            assertNull(journal.getIndex(name, tx1).insert(id0, v1));

            // Still not visible in concurrent transaction.
            assertFalse(journal.getIndex(name, tx0).contains(id0));

            // Still not visible in global scope.
            assertFalse(journal.getIndex(name).contains(id0));

            /*
             * Prepare and commit tx0.
             * 
             * Note: We MUST NOT propagate a delete marker onto the unisolated
             * index since no entry for that key is visible was visible when the
             * tx0 began.
             */
            assertNotSame(0L, journal.commit(tx0));

            // Prepare and commit tx1.
            assertNotSame(0L, journal.commit(tx1));

            // (id0,v1) is now visible in global scope.
            assertTrue(journal.getIndex(name).contains(id0));
            assertEquals(v1, (byte[]) journal.getIndex(name).lookup(id0));

        } finally {

            journal.destroy();

        }

    }

    /*
     * Transaction semantics tests.
     */
    
    /**
     * Simple test of commit semantics (no conflict). Four transactions are
     * started: tx0, which starts first. tx1 which starts next and on which we
     * will write one data version; tx2, which begins after tx1 but before tx1
     * commits - the change will NOT be visible in this transaction; and tx3,
     * which begins after tx1 commits - the change will be visible in this
     * transaction.
     */
    public void test_commit_noConflict01() {

        final Journal journal = getStore();

        try {

            final String name = "abc";
            final long commitTime0;
            {

                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                commitTime0 = journal.commit();
                System.err.println("commitTime0: " + journal.getCommitRecord());

                assertNotSame(0L, commitTime0);
                assertEquals("commitCounter", 1L, journal.getCommitRecord()
                        .getCommitCounter());

            }

            /*
             * Transaction that starts before the transaction on which we write.
             * The change will not be visible in this scope.
             */
            final long tx0 = journal.newTx(ITx.UNISOLATED);

            // transaction on which we write and later commit.
            final long tx1 = journal.newTx(ITx.UNISOLATED);

            // new transaction - commit will not be visible in this scope.
            final long tx2 = journal.newTx(ITx.UNISOLATED);

            System.err.println("commitTime0   =" + commitTime0);
            System.err.println("tx0: startTime=" + tx0);
            System.err.println("tx1: startTime=" + tx1);
            System.err.println("tx2: startTime=" + tx2);

            assertTrue(commitTime0 <= Math.abs(tx0));
            assertTrue(Math.abs(tx0) < Math.abs(tx1));
            assertTrue(Math.abs(tx1) < Math.abs(tx2));

            final byte[] id1 = new byte[] { 1 };

            final byte[] v0 = getRandomData().array();

            // write data version on tx1
            assertNull(journal.getIndex(name, tx1).insert(id1, v0));

            // data version visible in tx1.
            assertEquals(v0, (byte[]) journal.getIndex(name, tx1).lookup(id1));

            // data version not visible in global scope.
            assertNull(journal.getIndex(name).lookup(id1));

            // data version not visible in tx0.
            assertNull(journal.getIndex(name, tx0).lookup(id1));

            // data version not visible in tx2.
            assertNull(journal.getIndex(name, tx2).lookup(id1));

            // commit.
            final long tx1CommitTime = journal.commit(tx1);
            assertNotSame(0L, tx1CommitTime);
            System.err.println("tx1: startTime=" + tx1 + ", commitTime="
                    + tx1CommitTime);
            System.err.println("tx1: after commit: "
                    + journal.getCommitRecord());
            assertEquals("commitCounter", 2L, journal.getCommitRecord()
                    .getCommitCounter());

            // data version now visible in global scope.
            assertEquals(v0, (byte[]) journal.getIndex(name).lookup(id1));

            // new transaction - commit is visible in this scope.
            final long tx3 = journal.newTx(ITx.UNISOLATED);
            assertTrue(Math.abs(tx2) < Math.abs(tx3));
            assertTrue(Math.abs(tx3) >= tx1CommitTime);
            System.err.println("tx3: startTime=" + tx3);
            // System.err.println("tx3: ground state:
            // "+((Tx)journal.getTx(tx3)).commitRecord);

            // data version still not visible in tx0.
            assertNull(journal.getIndex(name, tx0).lookup(id1));

            // data version still not visible in tx2.
            assertNull(journal.getIndex(name, tx2).lookup(id1));

            /*
             * What commit record was written by tx1 and what commit record is
             * being used by tx3?
             */

            // data version visible in the new tx (tx3).
            assertEquals(v0, (byte[]) journal.getIndex(name, tx3).lookup(id1));

            /*
             * commit tx0 - nothing was written, no conflict should result.
             */
            assertEquals(0L, journal.commit(tx0));
            assertEquals("commitCounter", 2L, journal.getCommitRecord()
                    .getCommitCounter());

            /*
             * commit tx1 - nothing was written, no conflict should result.
             */
            assertEquals(0L, journal.commit(tx2));
            assertEquals("commitCounter", 2L, journal.getCommitRecord()
                    .getCommitCounter());

            // commit tx3 - nothing was written, no conflict should result.
            assertEquals(0L, journal.commit(tx3));
            assertEquals("commitCounter", 2L, journal.getCommitRecord()
                    .getCommitCounter());

            // data version in global scope was not changed by any other commit.
            assertEquals(v0, (byte[]) journal.getIndex(name).lookup(id1));

        } finally {

            journal.destroy();

        }

    }

    /**
     * Test in which a transaction deletes a pre-existing version (that is, a
     * version that existed in global scope when the transaction was started).
     */
    public void test_deletePreExistingVersion_noConflict() {

        final Journal journal = getStore();

        try {

            final String name = "abc";

            {
                IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                journal.commit();

            }

            final byte[] id0 = new byte[] { 1 };

            final byte[] v0 = getRandomData().array();

            // data version not visible in global scope.
            assertNull(journal.getIndex(name).lookup(id0));

            // write data version in global scope.
            journal.getIndex(name).insert(id0, v0);

            // data version visible in global scope.
            assertEquals(v0, journal.getIndex(name).lookup(id0));

            // commit the unisolated write.
            journal.commit();

            // start transaction.
            final long tx0 = journal.newTx(ITx.UNISOLATED);

            // data version visible in the transaction.
            assertEquals(v0, journal.getIndex(name, tx0).lookup(id0));

            // delete version in transaction scope.
            assertEquals(v0, journal.getIndex(name, tx0).remove(id0));

            // data version still visible in global scope.
            assertTrue(journal.getIndex(name).contains(id0));
            assertEquals(v0, journal.getIndex(name).lookup(id0));

            // data version not visible in transaction.
            assertFalse(journal.getIndex(name, tx0).contains(id0));
            assertNull(journal.getIndex(name, tx0).lookup(id0));

            // commit.
            journal.commit(tx0);

            // data version now deleted in global scope.
            assertFalse(journal.getIndex(name).contains(id0));

        } finally {

            journal.destroy();

        }

    }

}
