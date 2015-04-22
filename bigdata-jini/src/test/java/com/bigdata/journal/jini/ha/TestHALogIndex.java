/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Oct 14, 2006
 */
package com.bigdata.journal.jini.ha;

import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.ITupleIterator;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.journal.jini.ha.HALogIndex.IHALogRecord;
import com.bigdata.journal.jini.ha.HALogIndex.HALogRecord;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.TestWormAddressManager;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.util.ChecksumUtility;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Test suite for the {@link HALogIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHALogIndex extends TestCase2 {

    public TestHALogIndex() {
        super();
    }
    
    public TestHALogIndex(final String name) {
        super(name);
    }

    private Random r = null;
    private long m_lastCommitCounter = 0L;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        r = new Random();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        r = null;
    }
    
    /**
     * Generate a random empty root block (no commits). It will be well-formed,
     * but it will contain nonsense.
     */
    protected IRootBlockView newEmptyRootBlock() {

        return newRootBlock(false/* anyTransaction */);

    }

    /**
     * Generate a random non-empty root block (at least one commit point). It
     * will be well-formed, but it will contain nonsense.
     */
    protected IRootBlockView newNonEmptyRootBlock() {

        return newRootBlock(true/* anyTransaction */);

    }

    /**
     * Generate a random root block.
     * <p>
     * Note: The commit timestamps are assigned using {@link #nextTimestamp()}
     * and will be be montonically increasing when
     * <code>anyTransactions:=true</code>.
     * 
     * @param anyTransactions
     *            when <code>true</code> there will be at least one commit
     *            point.
     */
    protected IRootBlockView newRootBlock(final boolean anyTransactions) {

        final boolean rootBlock0 = r.nextBoolean();

        final ChecksumUtility checker = new ChecksumUtility();

        final long firstCommitTime = anyTransactions ? nextTimestamp() : 0L;

        // note: always greater than or equal to the first transaction
        // timestamp.
        final long lastCommitTime = anyTransactions?nextTimestamp():0L;
        
        // any legal value for offsetBits. @todo parameterize for RW vs WORM!!!
        final int offsetBits = r.nextInt(WormAddressManager.MAX_OFFSET_BITS
                - WormAddressManager.MIN_OFFSET_BITS)
                + WormAddressManager.MIN_OFFSET_BITS;
        final WormAddressManager am = new WormAddressManager(offsetBits);
        final long nextOffset = anyTransactions?TestWormAddressManager.nextNonZeroOffset(r,am):0L;
        final long commitCounter = anyTransactions ? m_lastCommitCounter
                + r.nextInt(Integer.MAX_VALUE - 1) + 1 : 0L;
        if (commitCounter > 0) {
            m_lastCommitCounter = commitCounter;
        }
        final long commitRecordAddr = anyTransactions?TestWormAddressManager.nextNonZeroAddr(r, am,nextOffset):0L;
        final long commitRecordIndexAddr = anyTransactions?TestWormAddressManager.nextNonZeroAddr(r, am,nextOffset):0L;
        final UUID uuid = UUID.randomUUID();
        final long blockSequence = r.nextBoolean()?IRootBlockView.NO_BLOCK_SEQUENCE:Math.abs(r.nextLong());
        final long quorum = r.nextBoolean()?Quorum.NO_QUORUM:Math.abs(r.nextLong());
        final long metaStartAddr = 0L;
        final long metaBitsAddr = 0L;
        final StoreTypeEnum storeType = StoreTypeEnum.WORM;
        final long createTime = nextTimestamp();
        final long closeTime = (r.nextInt(100)<10?(createTime+r.nextInt(10000)):0L);
        
        final RootBlockView rootBlock = new RootBlockView(rootBlock0, offsetBits,
                nextOffset, firstCommitTime, lastCommitTime, commitCounter,
                commitRecordAddr, commitRecordIndexAddr, uuid, 
                blockSequence, quorum,//
                metaStartAddr, metaBitsAddr, storeType, createTime,
                closeTime, RootBlockView.currentVersion, checker);

        return rootBlock;
        
    }
    
    private long nextTimestamp() {
        
        return MillisecondTimestampFactory.nextMillis();
        
    }

    /**
     * Verify field access methods.
     */
    public void test_HALogRecordClass_getters() {

        final IRootBlockView rb = newNonEmptyRootBlock();
        
        final long sizeOnDisk = 12L;

        final IHALogRecord sr0 = new HALogRecord(rb, sizeOnDisk);

        assertEquals(rb, sr0.getRootBlock());

        assertEquals(sizeOnDisk, sr0.sizeOnDisk());

    }
    
    /**
     * Verify serializable.
     */
    public void test_HALogRecordClass_Serializable() {

        final IRootBlockView rb = newNonEmptyRootBlock();
        
        final long sizeOnDisk = 12L;

        final IHALogRecord sr0 = new HALogRecord(rb, sizeOnDisk);

        final byte[] a = SerializerUtil.serialize(sr0);
        
        final IHALogRecord sr1 = (IHALogRecord) SerializerUtil
                .deserialize(a);

        assertEquals(rb, sr1.getRootBlock());

        assertEquals(sizeOnDisk, sr1.sizeOnDisk());

    }

    /**
     * Test of an empty index and tests of the various methods against that
     * empty index, including test of legal and illegal arguments.
     */
    public void test_emptyIndex() {

        final HALogIndex ndx = HALogIndex.createTransient();

        // index is empty.
        assertEquals(0L, ndx.getEntryCount());

        {
            try {
                // -1L is not allowed as a timestamp.
                assertNull(ndx.findIndexOf(-1L/* timestamp */));
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                // ignore
            }

            // Legal.
            assertEquals(-1, ndx.findIndexOf(0L/* timestamp */));
            assertEquals(-1, ndx.findIndexOf(1L/* timestamp */));
        }
        
        {
            try {
                // -1L is not allowed as a timestamp.
                assertNull(ndx.find(-1L/* timestamp */));
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                // ignore
            }

            // Legal.
            assertNull(ndx.find(0L/* timestamp */));
            assertNull(ndx.find(1L/* timestamp */));
        }
        
        {
            try {
                // -1L is not allowed as a timestamp.
                assertNull(ndx.findNext(-1L/* timestamp */));
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                // ignore
            }
            // Legal.
            assertNull(ndx.findNext(0L/*timestamp*/));
            assertNull(ndx.findNext(1L/*timestamp*/));
        }
        
        assertNull(ndx.getOldestEntry());
        
        assertNull(ndx.getNewestEntry());

        {
            try {
                assertNull(ndx.getEntryByReverseIndex(-1));
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                // ignore
            }
            // Legal.
            assertNull(ndx.getEntryByReverseIndex(0));
            assertNull(ndx.getEntryByReverseIndex(1));
            assertNull(ndx.getEntryByReverseIndex(2));
        }

        {
            try {
                // -1L is not allowed.
                ndx.findByCommitCounter(-1L/* commitCounter*/);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                // ignore
            }
            // Legal.
            assertNull(ndx.findByCommitCounter(0L/* commitCounter */));
            assertNull(ndx.findByCommitCounter(1L/* commitCounter */));
        }

    }

    /**
     * Verify methods to add/remove an index entry.
     */
    public void test_addRemove() {

        final HALogIndex ndx = HALogIndex.createTransient();

        final IHALogRecord sr0 = new HALogRecord(newNonEmptyRootBlock(),
                12L/* bytesOnDisk */);

        // index is empty.
        assertEquals(0L, ndx.getEntryCount());

        // Add record.
        ndx.add(sr0);

        assertEquals(1L, ndx.getEntryCount());

        // Remove record.
        ndx.remove(sr0.getRootBlock().getLastCommitTime());

        assertEquals(0L, ndx.getEntryCount());

    }

    /**
     * Tests against an index with a few well known entries.
     */
    public void test_nonEmptyIndex() {
        
        final HALogIndex ndx = HALogIndex.createTransient();

        final IHALogRecord sr0 = new HALogRecord(newNonEmptyRootBlock(),
                12L/* bytesOnDisk */);
        assertTrue(sr0.getRootBlock().getLastCommitTime() > 0);

        final IHALogRecord sr1 = new HALogRecord(newNonEmptyRootBlock(),
                22L/* bytesOnDisk */);
        assertTrue(sr1.getRootBlock().getLastCommitTime() > sr0.getRootBlock()
                .getLastCommitTime());

        final IHALogRecord sr2 = new HALogRecord(newNonEmptyRootBlock(),
                32L/* bytesOnDisk */);
        assertTrue(sr2.getRootBlock().getLastCommitTime() > sr1.getRootBlock()
                .getLastCommitTime());

        // index is empty.
        assertEquals(0L, ndx.getEntryCount());

        // Add records.
        ndx.add(sr0);
        ndx.add(sr1);
        ndx.add(sr2);

        assertEquals(3L, ndx.getEntryCount());

        {

            // Nothing for commitTime:=0.
            assertEquals(-1L, ndx.findIndexOf(0L/* commitTime */));
            
            // In sequence by their associated commitTimes.
            assertEquals(0L,
                    ndx.findIndexOf(sr0.getRootBlock().getLastCommitTime()));
            assertEquals(1L,
                    ndx.findIndexOf(sr1.getRootBlock().getLastCommitTime()));
            assertEquals(2L,
                    ndx.findIndexOf(sr2.getRootBlock().getLastCommitTime()));
            
        }

        {
            // Nothing for commitTime:=0.
            assertNull(ndx.find(0L/* commitTime*/));
            
            // Able to find record for each commitTime.
            assertEquals(sr0, ndx.find(sr0.getRootBlock().getLastCommitTime()));
            assertEquals(sr1, ndx.find(sr1.getRootBlock().getLastCommitTime()));
            assertEquals(sr2, ndx.find(sr2.getRootBlock().getLastCommitTime()));
        }

        {
            // Note: This incantation always finds the first index entry.
            assertEquals(sr0, ndx.findNext(0L/* timestamp */));
            
            // Find the success index entry.
            assertEquals(sr1, ndx.findNext(sr0.getRootBlock().getLastCommitTime()));
            assertEquals(sr2, ndx.findNext(sr1.getRootBlock().getLastCommitTime()));
            assertEquals(null, ndx.findNext(sr2.getRootBlock().getLastCommitTime()));
            
        }

        assertEquals(sr2, ndx.getNewestEntry());
        assertEquals(sr0, ndx.getOldestEntry());

        {
            // Indexing backwards from the end.
            assertEquals(sr2, ndx.getEntryByReverseIndex(0));
            assertEquals(sr1, ndx.getEntryByReverseIndex(1));
            assertEquals(sr0, ndx.getEntryByReverseIndex(2));
        }

        {
            assertEquals(null, ndx.findByCommitCounter(0L/* commitCounter */));
            assertEquals(sr0, ndx.findByCommitCounter(sr0.getRootBlock()
                    .getCommitCounter()));
            assertEquals(sr1, ndx.findByCommitCounter(sr1.getRootBlock()
                    .getCommitCounter()));
            assertEquals(sr2, ndx.findByCommitCounter(sr2.getRootBlock()
                    .getCommitCounter()));
        }

        // test iterator.
        {
            @SuppressWarnings("unchecked")
            final ITupleIterator<IHALogRecord> itr = ndx.rangeIterator();

            assertTrue(itr.hasNext());

            assertEquals(sr0, itr.next().getObject());
            assertEquals(sr1, itr.next().getObject());
            assertEquals(sr2, itr.next().getObject());
            assertFalse(itr.hasNext());

        }

    }

}
