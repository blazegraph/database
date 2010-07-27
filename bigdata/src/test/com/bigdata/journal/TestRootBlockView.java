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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.rawstore.TestWormAddressManager;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.util.ChecksumUtility;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Test suite for {@link RootBlockView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRootBlockView extends TestCase2 {

    /**
     * 
     */
    public TestRootBlockView() {
    }

    /**
     * @param arg0
     */
    public TestRootBlockView(String arg0) {
        super(arg0);
    }

    private long nextTimestamp() {
        
        return MillisecondTimestampFactory.nextMillis();
        
    }
    
    /**
     * Constructor correct acceptance stress test.
     */
    public void test_ctor() {
        
        final Random r = new Random();
        
        final int nrounds = 5000;
        
        final ChecksumUtility checker = new ChecksumUtility();
        
        for (int i = 0; i < nrounds; i++) {

            final boolean rootBlock0 = r.nextBoolean();
//            final int segmentId = r.nextInt();
            final boolean anyTransactions = r.nextInt(100)>90;
            final long firstCommitTime = anyTransactions?nextTimestamp():0L;
            // note: always greater than or equal to the first transaction timestamp.
            final long lastCommitTime = anyTransactions?nextTimestamp():0L;
            // any legal value for offsetBits.
            final int offsetBits = r.nextInt(WormAddressManager.MAX_OFFSET_BITS
                    - WormAddressManager.MIN_OFFSET_BITS)
                    + WormAddressManager.MIN_OFFSET_BITS;
            final WormAddressManager am = new WormAddressManager(offsetBits);
            final long nextOffset = anyTransactions?TestWormAddressManager.nextNonZeroOffset(r,am):0L;
            final long commitCounter = anyTransactions?r.nextInt(Integer.MAX_VALUE-1)+1:0L;
            final long commitRecordAddr = anyTransactions?TestWormAddressManager.nextNonZeroAddr(r, am,nextOffset):0L;
            final long commitRecordIndexAddr = anyTransactions?TestWormAddressManager.nextNonZeroAddr(r, am,nextOffset):0L;
            final UUID uuid = UUID.randomUUID();
            final long createTime = nextTimestamp();
            final long closeTime = (r.nextInt(100)<10?(createTime+r.nextInt(10000)):0L);
            
            RootBlockView rootBlock = new RootBlockView(rootBlock0, offsetBits,
                    nextOffset, firstCommitTime, lastCommitTime, 
                    commitCounter, commitRecordAddr, commitRecordIndexAddr,
                    uuid, createTime, closeTime, checker);

            System.err.println("pass=" + i + " of " + nrounds + " : challisField="
                    + rootBlock.getChallisField());

            // the local time stored on the root block.
            final long localTime = rootBlock.getLocalTime();
            final long challisField = rootBlock.getChallisField();
            final long checksum = rootBlock.getChecksum(null); // read but do not validate.
            rootBlock.getChecksum(checker); // compute and self-test.
            
            // Verify the view.
            rootBlock.valid();
            assertEquals("rootBlock0", rootBlock0, rootBlock.isRootBlock0());
            assertEquals("offsetBits", offsetBits, rootBlock.getOffsetBits());
            assertEquals("nextOffset", nextOffset, rootBlock.getNextOffset());
            assertEquals("firstCommitTime", firstCommitTime, rootBlock.getFirstCommitTime());
            assertEquals("lastCommitTime", lastCommitTime, rootBlock.getLastCommitTime());
            assertEquals("commitCounter", commitCounter, rootBlock.getCommitCounter());
            assertEquals("commitRecordAddr", commitRecordAddr, rootBlock.getCommitRecordAddr());
            assertEquals("commitRecordIndexAddr", commitRecordIndexAddr, rootBlock.getCommitRecordIndexAddr());
            assertEquals("uuid",uuid,rootBlock.getUUID());

            // create a view from the backing byte buffer.
            rootBlock = new RootBlockView(rootBlock0,rootBlock.asReadOnlyBuffer(),checker);
            
            // Verify the view.
            assertEquals("challisField", challisField, rootBlock.getChallisField());
            assertEquals("checksum", checksum, rootBlock.getChecksum(null));// read but do not validate.
            assertEquals("rootBlock0", rootBlock0, rootBlock.isRootBlock0());
            assertEquals("offsetBits", offsetBits, rootBlock.getOffsetBits());
            assertEquals("localTime", localTime, rootBlock.getLocalTime());
            assertEquals("nextOffset", nextOffset, rootBlock.getNextOffset());
            assertEquals("firstCommitTime", firstCommitTime, rootBlock.getFirstCommitTime());
            assertEquals("lastCommitTime", lastCommitTime, rootBlock.getLastCommitTime());
            assertEquals("commitCounter", commitCounter, rootBlock.getCommitCounter());
            assertEquals("commitRecordAddr", commitRecordAddr, rootBlock.getCommitRecordAddr());
            assertEquals("commitRecordIndexAddr", commitRecordIndexAddr, rootBlock.getCommitRecordIndexAddr());
            assertEquals("uuid",uuid,rootBlock.getUUID());
            rootBlock.valid();

            /*
             * verify that each read only view has independent position, mark, and offset.
             */
            {
                
                ByteBuffer view1 = rootBlock.asReadOnlyBuffer();

                ByteBuffer view2 = rootBlock.asReadOnlyBuffer();
                
                assertNotSame(view1, view2);
                assertEquals(0,view1.position());
                assertEquals(RootBlockView.SIZEOF_ROOT_BLOCK,view1.limit());
                
                // verify independent position, mark, and limit.
                view1.position(1);
                view1.mark();
                view1.limit(view1.limit()-1);

                // position and limit were unchanged (mark is not being tested).
                assertEquals(0,view2.position());
                assertEquals(RootBlockView.SIZEOF_ROOT_BLOCK,view2.limit());
                
            }
         
            /*
             * Verify correct rejection when the root block data is partly
             * overwritten.
             */
            {
                
                ByteBuffer view = rootBlock.asReadOnlyBuffer();
                
                /*
                 * clone the view of the root block.
                 */
                byte[] tmp = new byte[view.limit()];
                
                view.get(tmp); // read into a byte[].
                
                ByteBuffer modified = ByteBuffer.wrap(tmp);
                
                // verify clone is valid.
                new RootBlockView(rootBlock0,modified,checker);
                
                /*
                 * modify the cloned data.
                 */
                modified.putLong(RootBlockView.OFFSET_COMMIT_CTR, rootBlock
                        .getCommitCounter() + 1);

                // verify modified buffer causes checksum error.
                try {
                    
                    new RootBlockView(rootBlock0,modified,checker);
                    
                    fail("Expecting: "+RootBlockException.class);
                    
                } catch(RootBlockException ex) {
                    
                    System.err.println("Ignoring expected exception: "+ex);
                    
                }
                
                /*
                 * verify that we can read that root block anyway if we provide
                 * a [null] checksum utility.
                 */
                
                new RootBlockView(rootBlock0,modified,null/*checker*/);

            }
            
            /*
             * Verify correct rejection when the checksum field is bad.
             */
            {

                ByteBuffer view = rootBlock.asReadOnlyBuffer();
                
                /*
                 * clone the view of the root block.
                 */
                byte[] tmp = new byte[view.limit()];
                
                view.get(tmp); // read into a byte[].
                
                ByteBuffer modified = ByteBuffer.wrap(tmp);
                
                // verify clone is valid.
                new RootBlockView(rootBlock0,modified,checker);
                
                /*
                 * modify the cloned data.
                 */
                modified.putInt(RootBlockView.OFFSET_CHECKSUM, rootBlock
                        .getChecksum(null) + 1);

                // verify modified buffer causes checksum error.
                try {
                    
                    new RootBlockView(rootBlock0,modified,checker);
                    
                    fail("Expecting: "+RootBlockException.class);
                    
                } catch(RootBlockException ex) {
                    
                    System.err.println("Ignoring expected exception: "+ex);
                    
                }
                
                /*
                 * verify that we can read that root block anyway if we provide
                 * a [null] checksum utility.
                 */
                
                new RootBlockView(rootBlock0,modified,null/*checker*/);

            }
            
        }
        
    }

    /**
     * Correct rejection tests for the constructor.
     */
    public void test_ctor_correctRejection() {

        System.err.println("sizeof(RootBlock): "+RootBlockView.SIZEOF_ROOT_BLOCK);
        
        final boolean rootBlock0 = true; // all values are legal.
        //
//        final int segmentId = 0; // no constraint
        //
        final int offsetBitsOk = WormAddressManager.SCALE_UP_OFFSET_BITS;
        final int offsetBitsBad = WormAddressManager.MIN_OFFSET_BITS - 1;
        final int offsetBitsBad2 = WormAddressManager.MAX_OFFSET_BITS + 1;
        // used to form valid addresses.
        WormAddressManager am = new WormAddressManager(offsetBitsOk);
        //
        final long nextOffsetOk = 100;
        final long nextOffsetBad = -1;
        // note: choose timestamps in named sets (first,last,commit) for tests.
        final long firstCommitTimeOk = 0L;
        final long lastCommitTimeOk = 0L;
//        final long commitTimeOk = 0L;
        //
        final long firstCommitTimeOk2 = nextTimestamp();
        final long lastCommitTimeOk2 = nextTimestamp();
//        final long commitTimeOk2 = TimestampFactory.nextNanoTime();
        //
        final long firstCommitTimeBad1 = nextTimestamp();
        final long lastCommitTimeBad1 = 0L;
//        final long commitTimeBad1 = TimestampFactory.nextNanoTime();
        //
        final long firstCommitTimeBad2 = 0L;
        final long lastCommitTimeBad2 = nextTimestamp();
//        final long commitTimeBad2 = TimestampFactory.nextNanoTime();
        //
        final long lastCommitTimeBad3 = nextTimestamp(); // note: out of order.
        final long firstCommitTimeBad3 = nextTimestamp(); // note: out of order.
//        final long commitTimeBad3 = TimestampFactory.nextNanoTime();
        //
//        final long commitTimeBad4 = TimestampFactory.nextNanoTime(); // note: out of order.
        final long lastCommitTimeBad4 = nextTimestamp(); // note: out of order.
        final long firstCommitTimeBad4 = nextTimestamp();
        //
        // @todo present bad combinations of {commitCounter, rootsAddr, and commitRecordIndex}.
        //
        final long commitCounterOk = 0;
        final long commitCounterBad = -1; // negative
        final long commitCounterBad2 = Long.MAX_VALUE; // too large.
        //
        final long rootsAddrOk = 0L; // null reference
        final long rootsAddrOk2 = am.toAddr(3, 12L); // non-null reference.
        final long rootsAddrBad = -1;
        //
        final long commitRecordIndexOk = 0L; // null reference.
        final long commitRecordIndexOk2 = am.toAddr(30,23L); // non-null reference.
        final long commitRecordIndexBad = -1L;
        //
        final UUID uuidOk = UUID.randomUUID();
        final UUID uuidBad = null;
        //
        final long createTimeOk = nextTimestamp();
        final long createTimeBad = 0L;
        //
        final long closeTimeOk = 0L;
        final long closeTimeOk2 = createTimeOk + 1;
        final long closeTimeBad = createTimeOk - 1;
        //
        final ChecksumUtility checkerOk = new ChecksumUtility();
        final ChecksumUtility checkerBad = null;
        
        // legit.
        new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk, firstCommitTimeOk,
                lastCommitTimeOk, commitCounterOk, rootsAddrOk, commitRecordIndexOk,
                uuidOk, createTimeOk, closeTimeOk, checkerOk);
        // legit (firstCommitTimeOk2,lastCommitTimeOk2).
        new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk, firstCommitTimeOk2,
                lastCommitTimeOk2, commitCounterOk, rootsAddrOk, commitRecordIndexOk,
                uuidOk, createTimeOk, closeTimeOk, checkerOk);
        // legit (rootsAddr2, commitRecordIndex2)
        new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk, firstCommitTimeOk,
                lastCommitTimeOk, commitCounterOk, rootsAddrOk2, commitRecordIndexOk2,
                uuidOk, createTimeOk, closeTimeOk, checkerOk);
        // legit (closeTime2)
        new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                closeTimeOk2, checkerOk);

        // bad offsetBits.
        try {
            new RootBlockView(rootBlock0, offsetBitsBad, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrBad, commitRecordIndexOk2, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, offsetBitsBad2, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrBad, commitRecordIndexOk2, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad next offset
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetBad,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad first,last transaction start timestamps and commit timestamp.
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeBad1, lastCommitTimeBad1, commitCounterOk,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
        } catch (IllegalArgumentException ex) {
            fail("Unexpected exception", ex);
        }
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeBad2, lastCommitTimeBad2, commitCounterOk,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeBad3, lastCommitTimeBad3, commitCounterOk,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
        } catch (IllegalArgumentException ex) {
            fail("Unexpected exception", ex);
        }
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeBad4, lastCommitTimeBad4, commitCounterOk,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
        } catch (IllegalArgumentException ex) {
            fail("Unexpected exception", ex);
        }

        // bad commit counter
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterBad,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterBad2,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad {commit record, commit record index} combinations.
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrBad, commitRecordIndexOk2, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrOk2, commitRecordIndexBad, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            /*
             * Note: this combination is illegal since the commit record index
             * address is 0L while the commit record addr is defined.
             */
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrOk2, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad UUID.
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrBad, commitRecordIndexOk2, uuidBad, createTimeOk,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad createTime.
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrBad, commitRecordIndexOk2, uuidBad, createTimeBad,
                    closeTimeOk, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad closeTime.
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrBad, commitRecordIndexOk2, uuidBad, createTimeOk,
                    closeTimeBad, checkerOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad checker
        try {
            new RootBlockView(rootBlock0, offsetBitsOk, nextOffsetOk,
                    firstCommitTimeOk, lastCommitTimeOk, commitCounterOk,
                    rootsAddrOk, commitRecordIndexOk, uuidOk, createTimeOk,
                    closeTimeOk, checkerBad);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }

}
