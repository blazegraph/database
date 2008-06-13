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
abstract public class AbstractBTreeCursorTestCase extends AbstractCursorTestCase {

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
        
        if (isReadOnly())
            btree.setReadOnly(true);
 
        doEmptyIndexTest(btree);
        
    }

    public void test_oneTuple() {

        BTree btree = getOneTupleBTree();
        
        if (isReadOnly())
            btree.setReadOnly(true);
 
        doOneTupleTest(btree);

    }

    public void test_baseCase() {
        
        BTree btree = getBaseCaseBTree();

        btree.setReadOnly(isReadOnly());
        
        doBaseCaseTest(btree);
        
    }

}
