/*

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
package com.bigdata.btree;

/**
 * Unit tests of non-default B+Tree branching factors.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBTreeBranchingFactors extends AbstractBTreeTestCase {

    public TestBTreeBranchingFactors() {
    }

    public TestBTreeBranchingFactors(String name) {
        super(name);
    }

    public void test_branchingFactor_1000() {

        final int branchingFactor = 1000;

        final BTree btree = getBTree(branchingFactor);

        assertEquals("branchingFactor", branchingFactor,
                btree.getBranchingFactor());

    }

    /**
     * Unit test replicates an error reported in the ticket below. The root
     * cause of the error was a confusion in the code for the "Node Serializer"
     * classes used by the HTree and the BTree.
     * <p>
     * Note: We already have coverage for both classes of different branching
     * factors and addressBits. However, this specific value caused
     * 
     * <pre>
     * 1 &lt;&lt; addressBits
     * </pre>
     * 
     * to wrap to a negative integer and provided the clue that the
     * <code>initialCapacity</code> calculation in the BTree
     * {@link NodeSerializer} was incorrect.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/598">
     *      B+Tree branching factor and HTree addressBits are confused in their
     *      NodeSerializer implementations </a>
     */
    public void test_branchingFactor_1081() {

        final int branchingFactor = 1081;

        final BTree btree = getBTree(branchingFactor);

        assertEquals("branchingFactor", branchingFactor,
                btree.getBranchingFactor());
        
    }

    public void test_branchingFactor_1200() {

        final int branchingFactor = 1200;

        final BTree btree = getBTree(branchingFactor);

        assertEquals("branchingFactor", branchingFactor,
                btree.getBranchingFactor());

    }

}
