/*

Copyright (C) SYSTAP, LLC 2006-2009.  All rights reserved.

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
 * Created on Oct 13, 2009
 */

package com.bigdata.btree;

import java.util.UUID;

import org.apache.log4j.Level;

import junit.framework.TestCase2;

import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLeafSplitShortestSeparatorKey extends TestCase2 {

    /**
     * 
     */
    public TestLeafSplitShortestSeparatorKey() {
    }

    /**
     * @param name
     */
    public TestLeafSplitShortestSeparatorKey(String name) {
        super(name);
    }

    /**
     * On reflection, I suspect that this is an edge case which is simply not
     * covered by the code. I think that I can construct a unit test which will
     * demonstrate the failure and then work out the logic for the edge case.
     * <p>
     * The exception is being thrown when a leaf is full. The leaf is in a
     * temporary overflow condition. In order to split the leaf into a left
     * sibling and a right sibling, the tuples in the leaf are divided into two
     * by choosing the index of the first key which will enter the right
     * sibling. A separator key is chosen which guides search into either the
     * left sibling or the right sibling as appropriate and is then inserted
     * into the parent. The assertion was tripped because the chosen separator
     * key already existed in the parent. The code (Leaf#802) is attempting to
     * choose the shortest separator key because that makes the keys in the
     * nodes short, which reduces their on disk requirements. In fact, since we
     * also use prefix compression (front-coding of the keys) there is less
     * benefit to this than might otherwise be the case. Regardless, I believe
     * that the source of the error is the failure to consider the existing
     * separator key in the parent of the leaf which was directing search into
     * the leaf. In this case, it appears that the shortest separator key when
     * considering the last key in the left sibling and the first key in the
     * right sibling is identical to the pre-existing separator key in the
     * parent for the leaf before it is split. To fix this, I need to create a
     * unit test with data in a tree which replicates the assertion as an
     * existence proof of the problem and then change the logic to consider
     * three keys when determining the shortest separator key -- the existing
     * separator key in the parent node, the last key in the new left sibling,
     * and the first key in the new right sibling.
     * 
     * As a workaround, you can replace line 802:
     * 
     * <pre>
     * final byte[] separatorKey = BytesUtil.getSeparatorKey(//
     *         getKeys().get(splitIndex),//
     *         getKeys().get(splitIndex - 1)//
     *         );
     * </pre>
     * 
     * with
     * 
     * <pre>
     * final byte[] separatorKey = getKeys().get(splitIndex);
     * </pre>
     * 
     * This will use the first key in the new right sibling as the new separator
     * key in the parent between the new left and right sibling. That should be
     * correct, just a longer separator key.
     */
    public void test_shortestSeparatorKey() {

        final int m = 3;

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBranchingFactor(m);

//        metadata.setDeleteMarkers(true);
        
        metadata.setWriteRetentionQueueCapacity(1000);
        
        final BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);

        btree.insert(new byte[] { 1 }, (byte[]) null);
        // System.out.println("----------------------");
        // assert btree.dump(Level.DEBUG,System.out);

        btree.insert(new byte[] { 10 }, (byte[]) null);
        // System.out.println("----------------------");
        // assert btree.dump(Level.DEBUG,System.out);

        btree.insert(new byte[] { 20, 10 }, (byte[]) null);
        // System.out.println("----------------------");
        // assert btree.dump(Level.DEBUG,System.out);

        // causes split.
        btree.insert(new byte[] { 20 }, (byte[]) null);
        System.out.println("----------------------");
        assertTrue( btree.dump(Level.DEBUG,System.out) );
        
        // add to right edge of right sibling
        btree.insert(new byte[] { 20, 20 }, (byte[]) null);
        // remove left edge of right sibling (EQ to separator key).
        btree.remove(new byte[] { 20});
        System.out.println("----------------------");
        assertTrue( btree.dump(Level.DEBUG,System.out));

        // insert deleted key -- causes split for existing separator key.
        btree.insert(new byte[] { 20 }, (byte[]) null);
        System.out.println("----------------------");
        assertTrue( btree.dump(Level.DEBUG,System.out) );
        
    }
    
}
