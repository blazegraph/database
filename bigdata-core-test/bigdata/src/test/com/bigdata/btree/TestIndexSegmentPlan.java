/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Dec 14, 2006
 */

package com.bigdata.btree;

/**
 * Test suite for efficient post-order rebuild of an index in an external index
 * segment.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentPlan extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIndexSegmentPlan() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentPlan(String name) {
        super(name);
    }

    /**
     * Test {@link IndexSegmentBuilder#getMinimumHeight(int, int)}. This
     * routine is responsible for determining the minimum height of a tree given
     * a branching factor and a #of index entries.
     */
    public void test_minimumHeight() {
        
        assertEquals( 0, IndexSegmentPlan.getMinimumHeight(3, 1));
        assertEquals( 1, IndexSegmentPlan.getMinimumHeight(3, 2));
        assertEquals( 1, IndexSegmentPlan.getMinimumHeight(3, 3));
        assertEquals( 2, IndexSegmentPlan.getMinimumHeight(3, 4));
        assertEquals( 2, IndexSegmentPlan.getMinimumHeight(3, 5));
        assertEquals( 2, IndexSegmentPlan.getMinimumHeight(3, 6));
        assertEquals( 2, IndexSegmentPlan.getMinimumHeight(3, 7));
        assertEquals( 2, IndexSegmentPlan.getMinimumHeight(3, 8));
        assertEquals( 2, IndexSegmentPlan.getMinimumHeight(3, 9));
        assertEquals( 3, IndexSegmentPlan.getMinimumHeight(3, 10));
        
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
        
        assertEquals(new int[] { 3, 3, 2, 2 }, IndexSegmentPlan.distributeKeys(3, (3 + 1) / 2, 4, 10));

    }
    
    public void test_distributeKeys_m4() {
        
        assertEquals(new int[] { 4, 4, 2 }, IndexSegmentPlan.distributeKeys(
                4, (4 + 1) / 2, 3, 10));
        
    }
    
    public void test_distributeKeys_m5() {
        
        assertEquals(new int[] { 5,5 }, IndexSegmentPlan.distributeKeys(
                5, (5+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m6() {
        
        assertEquals(new int[] { 6,4 }, IndexSegmentPlan.distributeKeys(
                6, (6+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m7() {
        
        assertEquals(new int[] { 6,4 }, IndexSegmentPlan.distributeKeys(
                7, (7+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m8() {
        
        assertEquals(new int[] { 6,4 }, IndexSegmentPlan.distributeKeys(
                8, (8+ 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m9() {
        
        assertEquals(new int[] { 5, 5 }, IndexSegmentPlan.distributeKeys(9,
                (9 + 1) / 2, 2, 10));
        
    }
    
    public void test_distributeKeys_m10() {
        
        assertEquals(new int[] { 10 }, IndexSegmentPlan.distributeKeys(10,
                (10 + 1) / 2, 1, 10));
        
    }

    /**
     * Test where the root leaf has fewer than (m+1)/2 entries.  The root is
     * never under capacity, so this tests that the function to distribute the
     * keys accepts a root leaf under these circumstances.
     */
    public void test_distributeKeys_rootUnderCapacity() {
        
        assertEquals(new int[] { 3 }, IndexSegmentPlan.distributeKeys(10,
                (10 + 1) / 2, 1, 3));
        
    }
    
    /*
     * 
     */

    /**
     * An application of the routine to distribute children among nodes - the
     * logic is identical to distributing keys among leaves except that the
     * result must be interpreted as the #of children NOT the #of keys. An alias
     * is provided to help clarify this distinction.
     * 
     * @see IndexSegmentBuilder#distributeKeys(int, int, int, int)
     * @see IndexSegmentBuilder#distributeChildren(int, int, int, int)
     */
    public void test_distributeChildren01() {

        assertEquals(new int[] { 2, 2, 2, 2, 2 }, IndexSegmentPlan
                .distributeKeys(3, (3 + 1) / 2, 5, 10));

    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=3) and (n=10) entries.
     */
    public void test_plan_m3_n10() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(3,10);

        assertEquals("m",3,plan.m);
        assertEquals("(m+1/2)",2,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",4,plan.nleaves);
        assertEquals("nnodes",3,plan.nnodes);
        assertEquals("height",2,plan.height);
        assertEquals("numInLeaf[]",new int[]{3,3,2,2},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,2,4},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{2},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{2,2},plan.numInNode[1]);
        assertEquals("numInNode[2][]",new int[]{3,3,2,2},plan.numInNode[2]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=4) and (n=10) entries.
     */
    public void test_plan_m4_n10() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(4,10);

        assertEquals("m",4,plan.m);
        assertEquals("(m+1/2)",2,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",3,plan.nleaves);
        assertEquals("nnodes",1,plan.nnodes);
        assertEquals("height",1,plan.height);
        assertEquals("numInLeaf[]",new int[]{4,4,2},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,3},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{3},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{4,4,2},plan.numInNode[1]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=5) and (n=10) entries.
     */
    public void test_plan_m5_n10() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(5,10);

        assertEquals("m",5,plan.m);
        assertEquals("(m+1/2)",3,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",2,plan.nleaves);
        assertEquals("nnodes",1,plan.nnodes);
        assertEquals("height",1,plan.height);
        assertEquals("numInLeaf[]",new int[]{5,5},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,2},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{2},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{5,5},plan.numInNode[1]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=6) and (n=10) entries.
     */
    public void test_plan_m6_n10() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(6,10);

        assertEquals("m",6,plan.m);
        assertEquals("(m+1/2)",3,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",2,plan.nleaves);
        assertEquals("nnodes",1,plan.nnodes);
        assertEquals("height",1,plan.height);
        assertEquals("numInLeaf[]",new int[]{6,4},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,2},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{2},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{6,4},plan.numInNode[1]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=7) and (n=10) entries.
     */
    public void test_plan_m7_n10() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(7,10);

        assertEquals("m",7,plan.m);
        assertEquals("(m+1/2)",4,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",2,plan.nleaves);
        assertEquals("nnodes",1,plan.nnodes);
        assertEquals("height",1,plan.height);
        assertEquals("numInLeaf[]",new int[]{6,4},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,2},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{2},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{6,4},plan.numInNode[1]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=8) and (n=10) entries.
     */
    public void test_plan_m8_n10() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(8,10);

        assertEquals("m",8,plan.m);
        assertEquals("(m+1/2)",4,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",2,plan.nleaves);
        assertEquals("nnodes",1,plan.nnodes);
        assertEquals("height",1,plan.height);
        assertEquals("numInLeaf[]",new int[]{6,4},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,2},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{2},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{6,4},plan.numInNode[1]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=9) and (n=10) entries.
     */
    public void test_plan_m9_n10() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(9,10);

        assertEquals("m",9,plan.m);
        assertEquals("(m+1/2)",5,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",2,plan.nleaves);
        assertEquals("nnodes",1,plan.nnodes);
        assertEquals("height",1,plan.height);
        assertEquals("numInLeaf[]",new int[]{5,5},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,2},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{2},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{5,5},plan.numInNode[1]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=10) and (n=10) entries (everything fits into the root leaf)
     */
    public void test_plan_m10_n10_everythingInTheRootLeaf() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(10,10);

        assertEquals("m",10,plan.m);
        assertEquals("(m+1/2)",5,plan.m2);
        assertEquals("nentries",10,plan.nentries);
        assertEquals("nleaves",1,plan.nleaves);
        assertEquals("nnodes",0,plan.nnodes);
        assertEquals("height",0,plan.height);
        assertEquals("numInLeaf[]",new int[]{10},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{10},plan.numInNode[0]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=3) and (n=0) entries.
     */
    public void test_plan_m3_n0_emptyRootLeaf() {
        
        final IndexSegmentPlan plan = new IndexSegmentPlan(3, 0);

        assertEquals("m",3,plan.m);
        assertEquals("(m+1/2)",2,plan.m2);
        assertEquals("nentries",0,plan.nentries);
        assertEquals("nleaves",1,plan.nleaves);
        assertEquals("nnodes",0,plan.nnodes);
        assertEquals("height",0,plan.height);
        assertEquals("numInLeaf[]",new int[]{0},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{0},plan.numInNode[0]);
        
    }

    /**
     * Tests {@link IndexSegmentPlan} for a tree with a branching factor of
     * (m=3) and (n=20) entries.
     */
    public void test_plan_m3_n20() {
        
        IndexSegmentPlan plan = new IndexSegmentPlan(3,20);

        assertEquals("m",3,plan.m);
        assertEquals("(m+1/2)",2,plan.m2);
        assertEquals("nentries",20,plan.nentries);
        assertEquals("nleaves",7,plan.nleaves);
        assertEquals("nnodes",4,plan.nnodes);
        assertEquals("height",2,plan.height);
        assertEquals("numInLeaf[]",new int[]{3,3,3,3,3,3,2},plan.numInLeaf);
        assertEquals("numInLevel[]",new long[]{1,3,7},plan.numInLevel);
        assertEquals("numInNode[][]",plan.height+1,plan.numInNode.length);
        assertEquals("numInNode[0][]",new int[]{3},plan.numInNode[0]);
        assertEquals("numInNode[1][]",new int[]{3,2,2},plan.numInNode[1]);
        assertEquals("numInNode[2][]",new int[]{3,3,3,3,3,3,2},plan.numInNode[2]);

    }

}
