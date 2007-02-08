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
 * Created on Feb 7, 2007
 */

package com.bigdata.scaleup;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.SimpleEntry;

/**
 * @todo rather than writing this test suite directly, we mostly want to apply
 * the existing proxy test suites for {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPartitionedStore extends TestCase2 {

    /**
     * 
     */
    public TestPartitionedStore() {
    }

    /**
     * @param name
     */
    public TestPartitionedStore(String name) {
        super(name);
    }

    /**
     * Test the ability to register and use named index, including whether the
     * named index is restart safe.
     */
    public void test_registerAndUse() {

        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
        
        properties.setProperty(Options.SEGMENT, "0");
        
        File file = new File(getName()+".jnl");
        
        assertTrue(!file.exists() || file.delete());
        
        properties.setProperty(Options.FILE,file.toString());
        
        PartitionedJournal journal = new PartitionedJournal(properties);
        
        String name = "abc";
        
        BTree btree = new BTree(journal, 3, SimpleEntry.Serializer.INSTANCE);
        
        assertNull(journal.getIndex(name));
        
        journal.registerIndex(name, btree);
        
        assertTrue(btree==journal.getIndex(name));
        
        final byte[] k0 = new byte[]{0};
        final Object v0 = new SimpleEntry(0);
        
        btree.insert( k0, v0);

        /*
         * commit and close the journal
         */
        journal.commit();
        
        journal.close();
        
        if (journal.isStable()) {

            /*
             * re-open the journal and test restart safety.
             */
            journal = new PartitionedJournal(properties);

            btree = (BTree) journal.getIndex(name);

            assertNotNull("btree", btree);
            assertEquals("entryCount", 1, btree.getEntryCount());
            assertEquals(v0, btree.lookup(k0));

            journal.close();

        }

    }
    
}
