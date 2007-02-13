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
 * Created on Feb 13, 2007
 */

package com.bigdata.isolation;

import com.bigdata.objndx.AbstractBTreeTestCase;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link UnisolatedBTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test the point and batch apis. all methods must work with {@link Value}s
 * 
 * @todo test the rangeCount and rangeIterator api. the former will over
 *       estimate if there are deleted entries while the latter must not visit
 *       deleted entries (or may it -- we will need to see them for mergeDown()
 *       and validate())?
 * 
 * @todo test entryIterator() - it visits only those that are not deleted.
 */
public class TestUnisolatedBTree extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestUnisolatedBTree() {
    }

    /**
     * @param name
     */
    public TestUnisolatedBTree(String name) {
        super(name);
    }

    public void assertEquals(Value expected, Value actual) {
        
        assertEquals(expected.versionCounter,actual.versionCounter);
        
        assertEquals(expected.deleted,actual.deleted);
        
        assertEquals(expected.value,actual.value);
        
    }
    
    /**
     * test constructors and re-load of attributes set by constructors.
     */
    public void test_ctor() {
    
        final IRawStore store = new SimpleMemoryRawStore();
        final int branchingFactor = 4;
        IConflictResolver conflictResolver = new NoConflictResolver();
        {
            
            UnisolatedBTree btree = new UnisolatedBTree(store,branchingFactor,null);
            
            assertTrue(store==btree.getStore());
            assertEquals(branchingFactor,btree.getBranchingFactor());
            assertNull(btree.getConflictResolver());

            final long addr = btree.write();
            
            btree = new UnisolatedBTree(store,BTreeMetadata.read(store, addr));

            assertTrue(store==btree.getStore());
            assertEquals(branchingFactor,btree.getBranchingFactor());
            assertNull(btree.getConflictResolver());

        }
        {
            
            UnisolatedBTree btree = new UnisolatedBTree(store, branchingFactor,
                    conflictResolver);

            assertTrue(store == btree.getStore());
            assertEquals(branchingFactor, btree.getBranchingFactor());
            assertTrue(conflictResolver == btree.getConflictResolver());

            final long addr = btree.write();

            btree = new UnisolatedBTree(store, BTreeMetadata.read(store, addr));

            assertTrue(store == btree.getStore());
            assertEquals(branchingFactor, btree.getBranchingFactor());
            assertTrue(btree.getConflictResolver() instanceof NoConflictResolver);
            
        }
        
    }
    
    public void test_crud_pointApi() {
        
        final byte[] k3= new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        
        UnisolatedBTree btree = new UnisolatedBTree(new SimpleMemoryRawStore(),
                3, null);

        assertFalse(btree.contains(k3));
        assertEquals(null,btree.insert(k3,k3));
        assertTrue(btree.contains(k3));
        assertEquals(k3,(byte[])btree.lookup(k3));
        assertEquals(new Value((short)1,false,k3),(Value)((BTree)btree).lookup(k3));
        
        assertEquals(k3,btree.insert(k3,k3));
        assertEquals(new Value((short)2,false,k3),(Value)((BTree)btree).lookup(k3));
        
        
        fail("write test");
        
    }

    public void test_crud_batchApi() {
        
        fail("write test");
    }

    public void test_restartSafe() {
        
        fail("write test");
    }
    
    /**
     * Does not resolve any conflicts.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class NoConflictResolver implements IConflictResolver {

        private static final long serialVersionUID = 1L;

        public Value resolveConflict(byte[] key, Value comittedValue, Value txEntry) throws RuntimeException {
            return null;
        }
        
        
    }

}
