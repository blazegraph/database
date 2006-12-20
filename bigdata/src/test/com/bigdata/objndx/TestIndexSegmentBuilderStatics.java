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
 * Created on Dec 14, 2006
 */

package com.bigdata.objndx;

/**
 * Test suite for efficient post-order rebuild of an index in an external index
 * segment.
 * 
 * @todo verify post-conditions for files (temp file is deleted, perhaps the
 *       index segment is read only).
 * 
 * @todo try building large indices, exporting them into index segments, and
 *       then verifying that the index segments have the correct data. We can
 *       run a variety of index stress tests to build the index, sweep in data
 *       from the file system, etc., and then generate the corresponding index
 *       segment and validate it against the in memory {@link BTree}.
 * 
 * @todo The notion of merging multiple index segments requires a notion of
 *       which index segments are more recent or alternatively which values are
 *       more recent so that we can reconcile values for the same key. this is
 *       linked to how we will handle transactional isolation.
 * 
 * @todo Handle "delete" markers. For full transactional isolation we need to
 *       keep delete markers around until there are no more live transactions
 *       that could read the index entry. This suggests that we probably want to
 *       use the transaction timestamp rather than a version counter. Consider
 *       that a read by tx1 needs to check the index on the journal and then
 *       each index segment in turn in reverse historical order until an entry
 *       (potentially a delete marker) is found that is equal to or less than
 *       the timestamp of the committed state from which tx1 was born. This
 *       means that an index miss must test the journal and all live segments
 *       for that index (hence the use of bloom filters to filter out index
 *       misses). It also suggests that we should keep the timestamp as part of
 *       the key, except in the ground state index on the journal where the
 *       timestamp is the timestamp of the last commit of the journal. This
 *       probably will also address VLR TX that would span a freeze of the
 *       journal. We expunge the isolated index into a segment and do a merge
 *       when the transaction finally commits. We wind up doing the same
 *       validation and merge steps as when the isolation occurs within a more
 *       limited buffer, but in more of a batch fashion. This might work nicely
 *       if we buffer the isolatation index out to a certain size in memory and
 *       then start to spill it onto the journal. If fact, the hard reference
 *       queue already does this so we can just test to see if (a) anything has
 *       been written out from the isolation index; and (b) whether or not the
 *       journal was frozen since the isolation index was created.
 * 
 * Should the merge down should impose the transaction commit timestamp on the
 * items in the index?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentBuilderStatics extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIndexSegmentBuilderStatics() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentBuilderStatics(String name) {
        super(name);
    }

    /**
     * Test {@link IndexSegmentBuilder#getMinimumHeight(int, int)}. This
     * routine is responsible for determining the minimum height of a tree given
     * a branching factor and a #of index entries.
     */
    public void test_minimumHeight() {
        
        assertEquals( 0, IndexSegmentBuilder.getMinimumHeight(3, 1));
        assertEquals( 1, IndexSegmentBuilder.getMinimumHeight(3, 2));
        assertEquals( 1, IndexSegmentBuilder.getMinimumHeight(3, 3));
        assertEquals( 2, IndexSegmentBuilder.getMinimumHeight(3, 4));
        assertEquals( 2, IndexSegmentBuilder.getMinimumHeight(3, 5));
        assertEquals( 2, IndexSegmentBuilder.getMinimumHeight(3, 6));
        assertEquals( 2, IndexSegmentBuilder.getMinimumHeight(3, 7));
        assertEquals( 2, IndexSegmentBuilder.getMinimumHeight(3, 8));
        assertEquals( 2, IndexSegmentBuilder.getMinimumHeight(3, 9));
        assertEquals( 3, IndexSegmentBuilder.getMinimumHeight(3, 10));
        
    }

    /**
     * A series of tests for
     * {@link IndexSegmentBuilder#distributeKeys(int m, int m/2, int nleaves, int nentries)}.
     * 
     * This routine is responsible for deciding how many index entries will go
     * into each leaf of the generated {@link IndexSegment}. In particular, it
     * compensates when there would be an underflow in the last leaf unless we
     * short some of the earlier leaves so that all leaves have at least their
     * minimum capacity.
     * 
     * @see src/architecture/btree.xls, which has the examples from which these
     *      tests are derived.
     */
    public void test_distributeKeys_m3() {
        
        assertEquals(new int[] { 3, 3, 2, 2 }, IndexSegmentBuilder
                .distributeKeys(3, (3 + 1) / 2, 4, 10));

    }
    
    public void test_distributeKeys_m4() {
        
        assertEquals(new int[] { 4, 4, 2 }, IndexSegmentBuilder.distributeKeys(
                4, (4 + 1) / 2, 3, 10));
        
    }
    
    public void test_distributeKeys_m5() {
        
        assertEquals(new int[] { 5,5 }, IndexSegmentBuilder.distributeKeys(
                5, (5+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m6() {
        
        assertEquals(new int[] { 6,4 }, IndexSegmentBuilder.distributeKeys(
                6, (6+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m7() {
        
        assertEquals(new int[] { 6,4 }, IndexSegmentBuilder.distributeKeys(
                7, (7+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m8() {
        
        assertEquals(new int[] { 6,4 }, IndexSegmentBuilder.distributeKeys(
                8, (8+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m9() {
        
        assertEquals(new int[] { 5, 5 }, IndexSegmentBuilder.distributeKeys(9,
                (9 + 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m10() {
        
        assertEquals(new int[] { 10 }, IndexSegmentBuilder.distributeKeys(10,
                (10 + 1) / 2, 1, 10));
        
    }
    
}
