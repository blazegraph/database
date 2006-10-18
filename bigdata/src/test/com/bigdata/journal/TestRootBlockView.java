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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.util.Random;

import junit.framework.TestCase;

/**
 * Test suite for {@link RootBlockView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRootBlockView extends TestCase {

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

    /**
     * Test verifies that nano times are always distinct from the last generated
     * nanos time (as assigned by {@link System#nanoTime()}.  If this test passes
     * then it shows that nanos can not be assigned quickly enough to result in
     * duplicate values.
     */
    public void test_nextNanoTime() {

        final int limit = 1000000;
        
        long lastNanoTime = System.nanoTime();
        long nanoTime;
        long minDiff = Long.MAX_VALUE;
        
        for( int i=0; i<limit; i++ ) {

            nanoTime = System.nanoTime();
            
            if( nanoTime == lastNanoTime ) fail("Same nano time?");

            long diff = nanoTime - lastNanoTime;
            
            if( diff < 0 ) diff = -diff;
            
            if( diff < minDiff ) minDiff = diff;
            
            lastNanoTime = nanoTime;
            
        }
        
        System.err.println("Minimum difference in nanos is " + minDiff
                + " over " + limit + " trials");
        
    }
    
    /**
     * Test verifies that nano times are always distinct from the last generated
     * nanos time (as assigned by {@link RootBlockView#nextNanoTime()}.
     */
    public void test_nextNanoTime2() {

        final int limit = 1000000;
        
        long lastNanoTime = System.nanoTime() - 1;
        long nanoTime;
        long minDiff = Long.MAX_VALUE;
        
        for( int i=0; i<limit; i++ ) {

            nanoTime = RootBlockView.nextNanoTime();
            
            if( nanoTime == lastNanoTime ) fail("Same nano time?");

            long diff = nanoTime - lastNanoTime;
            
            if( diff < 0 ) diff = -diff;
            
            if( diff < minDiff ) minDiff = diff;
            
            lastNanoTime = nanoTime;
            
        }
        
        System.err.println("Minimum difference in nanos is " + minDiff
                + " over " + limit + " trials");
        
    }
    
    /**
     * Constructor correct acceptance stress test.
     */
    public void test_ctor() {
        
        final Random r = new Random();
        
        final int limit = 10000;
        
        for (int i = 0; i < limit; i++) {

            final long segmentId = r.nextLong();
            final int slotSize = SlotMath.HEADER_SIZE + r.nextInt(1024) + 1;
            final int slotLimit = 100 + r.nextInt(10000);
            final int slotChain = r.nextInt(slotLimit);
            final int objectIndex = r.nextInt(slotLimit);
            final long commitCounter = r.nextInt(Integer.MAX_VALUE);

            RootBlockView rootBlock = new RootBlockView(segmentId, slotSize, slotLimit,
                    slotChain, objectIndex, commitCounter );

            System.err.println("pass=" + i + " of " + limit + " : timestamp="
                    + rootBlock.getTimestamp());

            // Verify the view.
            rootBlock.valid();
            assertEquals("segmentId", segmentId, rootBlock.getSegmentId());
            assertEquals("slotSize", slotSize, rootBlock.getSlotSize());
            assertEquals("slotLimit", slotLimit, rootBlock.getSlotLimit());
            assertEquals("slotChain", slotChain, rootBlock
                    .getSlotIndexChainHead());
            assertEquals("objectIndex", objectIndex, rootBlock
                    .getObjectIndexRoot());
            assertEquals("commitCounter", commitCounter, rootBlock
                    .getCommitCounter());

            // create a view from the backing byte buffer.
            rootBlock = new RootBlockView(rootBlock.asReadOnlyBuffer());
            
            // Verify the view.
            rootBlock.valid();
            assertEquals("segmentId", segmentId, rootBlock.getSegmentId());
            assertEquals("slotSize", slotSize, rootBlock.getSlotSize());
            assertEquals("slotLimit", slotLimit, rootBlock.getSlotLimit());
            assertEquals("slotChain", slotChain, rootBlock
                    .getSlotIndexChainHead());
            assertEquals("objectIndex", objectIndex, rootBlock
                    .getObjectIndexRoot());
            assertEquals("commitCounter", commitCounter, rootBlock
                    .getCommitCounter());

        }
        
    }

    /**
     * Correct rejection tests for the constructor.
     */
    public void test_ctor_correctRejection() {

        final long segmentId = 0L; // no constraint
        final int slotSizeOk = SlotMath.HEADER_SIZE + 100;
        final int slotSizeBad = SlotMath.HEADER_SIZE; // too small.
        final int slotSizeBad2 = -1; // negative.
        final int slotLimit = 100; // no constraint.
        final int slotChainOk = slotLimit - 1;
        final int slotChainOk2 = 0;
        final int slotChainBad = slotLimit; // too large
        final int slotChainBad2 = -1; // negative
        final int objectIndexOk = slotLimit - 1;
        final int objectIndexOk2 = 0;
        final int objectIndexBad = slotLimit; // too large
        final int objectIndexBad2 = -1; // negative
        final long commitCounterOk = 0;
        final long commitCounterBad = -1; // negative
        final long commitCounterBad2 = Long.MAX_VALUE; // too large.
        
        // legit.
        new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainOk,
                objectIndexOk, commitCounterOk);
        new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainOk2,
                objectIndexOk, commitCounterOk);
        new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainOk,
                objectIndexOk2, commitCounterOk);

        // bad slot size.
        try {
            new RootBlockView(segmentId, slotSizeBad, slotLimit, slotChainOk,
                    objectIndexOk, commitCounterOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad slot size.
        try {
            new RootBlockView(segmentId, slotSizeBad2, slotLimit, slotChainOk,
                    objectIndexOk, commitCounterOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad slot chain.
        try {
            new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainBad,
                    objectIndexOk, commitCounterOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainBad2,
                    objectIndexOk, commitCounterOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad object index.
        try {
            new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainOk,
                    objectIndexBad, commitCounterOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainOk,
                    objectIndexBad2, commitCounterOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad commit counter
        try {
            new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainOk,
                    objectIndexOk, commitCounterBad);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(segmentId, slotSizeOk, slotLimit, slotChainOk,
                    objectIndexOk, commitCounterBad2);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }

}
