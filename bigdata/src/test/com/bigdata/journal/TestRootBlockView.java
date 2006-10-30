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

import junit.framework.TestCase2;

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

    /**
     * Constructor correct acceptance stress test.
     */
    public void test_ctor() {
        
        final Random r = new Random();
        
        final int limit = 10000;
        
        final int[] objectIndexSizes = new int[]{16,32,64,128,256,512,1024};
        
        for (int i = 0; i < limit; i++) {

            final boolean rootBlock0 = r.nextBoolean();
            final long segmentId = r.nextLong();
            final int slotSize = Journal.MIN_SLOT_SIZE + r.nextInt(1024);
            final int slotLimit = 100 + r.nextInt(10000);
            final int objectIndexSize = objectIndexSizes[r.nextInt(objectIndexSizes.length)];
            final int slotChain = r.nextInt(slotLimit);
            final int objectIndex = r.nextInt(slotLimit);
            final long commitCounter = r.nextInt(Integer.MAX_VALUE);
            final int[] rootIds = new int[RootBlockView.MAX_ROOT_ID];
            for( int j=0; j<rootIds.length; j++ ) {
                rootIds[j] = r.nextInt();
            }

            RootBlockView rootBlock = new RootBlockView(rootBlock0, segmentId,
                    slotSize, slotLimit, objectIndexSize, slotChain,
                    objectIndex, commitCounter, rootIds);

            System.err.println("pass=" + i + " of " + limit + " : timestamp="
                    + rootBlock.getTimestamp());

            // Verify the view.
            rootBlock.valid();
            assertEquals("rootBlock0", rootBlock0, rootBlock.isRootBlock0());
            assertEquals("segmentId", segmentId, rootBlock.getSegmentId());
            assertEquals("slotSize", slotSize, rootBlock.getSlotSize());
            assertEquals("slotLimit", slotLimit, rootBlock.getSlotLimit());
            assertEquals("slotChain", slotChain, rootBlock
                    .getSlotIndexChainHead());
            assertEquals("objectIndex", objectIndex, rootBlock
                    .getObjectIndexRoot());
            assertEquals("rootIds", rootIds, rootBlock.getRootIds());
            for( int j=0; j<rootIds.length; j++ ) {
                assertEquals("rootId[" + j + "]", rootIds[j], rootBlock
                        .getRootId(j));
            }
            assertEquals("commitCounter", commitCounter, rootBlock
                    .getCommitCounter());

            // create a view from the backing byte buffer.
            rootBlock = new RootBlockView(rootBlock0,rootBlock.asReadOnlyBuffer());
            
            // Verify the view.
            rootBlock.valid();
            assertEquals("rootBlock0", rootBlock0, rootBlock.isRootBlock0());
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

        final boolean rootBlock0 = true; // all values are legal.
        final long segmentId = 0L; // no constraint
        final int slotSizeOk = 100;
        final int slotSizeBad = Journal.MIN_SLOT_SIZE - 1; // too small.
        final int slotSizeBad2 = -1; // negative.
        final int slotLimit = 100; // no constraint.
        final int objectIndexSizeOk = 64;
        final int objectIndexSizeBad = 63; // must be even.
        final int objectIndexSizeBad2 = 0; // must be positive.
        final int slotChainOk = slotLimit - 1;
        final int slotChainOk2 = 0;
        final int slotChainBad = slotLimit; // too large
        final int slotChainBad2 = -1; // negative
        final int objectIndexOk = slotLimit - 1;
        final int objectIndexOk2 = 0;
        final int objectIndexBad = slotLimit; // too large
        final int objectIndexBad2 = -1; // negative
        final int[] rootIdsOk = new int[RootBlockView.MAX_ROOT_ID];
        final int[] rootIdsBad = null; // null.
        final int[] rootIdsBad2 = new int[RootBlockView.MAX_ROOT_ID-1]; // too small.
        final int[] rootIdsBad3 = new int[RootBlockView.MAX_ROOT_ID+1]; // too large.
        final long commitCounterOk = 0;
        final long commitCounterBad = -1; // negative
        final long commitCounterBad2 = Long.MAX_VALUE; // too large.
        
        // legit.
        new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                objectIndexSizeOk, slotChainOk, objectIndexOk, commitCounterOk,
                rootIdsOk);
        new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                objectIndexSizeOk, slotChainOk2, objectIndexOk,
                commitCounterOk, rootIdsOk);
        new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                objectIndexSizeOk, slotChainOk, objectIndexOk2,
                commitCounterOk, rootIdsOk);

        // bad slot size.
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeBad, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexOk,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad slot size.
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeBad2, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexOk,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad objectIndexSize.
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeBad, slotChainOk, objectIndexOk,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeBad2, slotChainOk, objectIndexOk,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad slot chain.
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainBad, objectIndexOk,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainBad2, objectIndexOk,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad object index.
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexBad,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexBad2,
                    commitCounterOk, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad commit counter
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexOk,
                    commitCounterBad, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexOk,
                    commitCounterBad2, rootIdsOk);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // bad root ids.
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexOk,
                    commitCounterOk, rootIdsBad);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexOk,
                    commitCounterOk, rootIdsBad2);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new RootBlockView(rootBlock0, segmentId, slotSizeOk, slotLimit,
                    objectIndexSizeOk, slotChainOk, objectIndexOk,
                    commitCounterOk, rootIdsBad3);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }

}
