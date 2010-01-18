/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 10, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Abstract base class for some unit tests that can only be run against a
 * {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTreeCursorTestCase extends AbstractTupleCursorTestCase {

    /**
     * 
     */
    public AbstractBTreeCursorTestCase() {
    }
    
    public AbstractBTreeCursorTestCase(String name) {
        
        super(name);
        
    }
    
    /**
     * Return <code>true</code> if the B+Tree under test is read-only.
     */
    abstract protected boolean isReadOnly();

    public void test_emptyIndex() {

        BTree btree = BTree.create(new SimpleMemoryRawStore(), new IndexMetadata(UUID.randomUUID()));
        
        if(isReadOnly()) {
            
            btree.writeCheckpoint();
            
            btree = btree.asReadOnly();
            
        }
        
        doEmptyIndexTest(btree);
        
    }

    public void test_oneTuple() {

        BTree btree = getOneTupleBTree();
        
        if(isReadOnly()) {
            
            btree.writeCheckpoint();
            
            btree = btree.asReadOnly();
            
        }
 
        doOneTupleTest(btree);

    }

    public void test_baseCase() {
        
        BTree btree = getBaseCaseBTree();

        if(isReadOnly()) {
            
            btree.writeCheckpoint();
            
            btree = btree.asReadOnly();
            
        }
        
        doBaseCaseTest(btree);
        
    }

    /**
     * Unit test verifies that a fromKey and toKey which are out of order will
     * be rejected.
     */
    public void test_keyRange_correctRejection() {

        BTree btree = BTree.create(new SimpleMemoryRawStore(),
                new IndexMetadata(UUID.randomUUID()));

        if(isReadOnly()) {
            
            btree.writeCheckpoint();
            
            btree = btree.asReadOnly();
            
        }
        
        /*
         * These are Ok.
         */
        
        newCursor(btree, IRangeQuery.DEFAULT, null/* fromKey */, null/* toKey */);

        newCursor(btree, IRangeQuery.DEFAULT, new byte[] {}, null/* toKey */);
        
        newCursor(btree, IRangeQuery.DEFAULT, new byte[] {2}, new byte[]{3});

        /*
         * Edge case where [toKey == fromKey] is allowed.
         */

        newCursor(btree, IRangeQuery.DEFAULT, new byte[] {}, new byte[]{});

        newCursor(btree, IRangeQuery.DEFAULT, new byte[] {5}, new byte[]{5});

        /*
         * These are illegal.
         */

        // toKey LT from key
        try {
            newCursor(btree, IRangeQuery.DEFAULT, new byte[] {5}, new byte[]{});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            log.info("Ignoring expected exception: "+ex);
        }

        // toKey LT from key
        try {
            newCursor(btree, IRangeQuery.DEFAULT, new byte[] {5}, new byte[]{3});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            log.info("Ignoring expected exception: "+ex);
        }

        /*
         * @todo variant that tests when the key range is not legal for the
         * underlying index partition.
         */
        
    }
    
}
