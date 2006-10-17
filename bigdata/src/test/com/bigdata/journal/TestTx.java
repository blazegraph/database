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
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;

/**
 * Test suite for transaction isolation with respect to the underlying journal.
 * The tests in this suite are designed to verify isolation of changes within
 * the scope of the transaction when compared to the last committed state of the
 * journal. This basically amounts to verifying that operations read through the
 * transaction scope object index into the journal scope object index.
 * 
 * @todo This suite does not attempt to verify isolation for concurrent
 *       transactions, but that amounts to pretty much the same thing.
 * 
 * @todo Work through tests of the commit logic and verify the post-conditions
 *       for successful commit vs abort of a transaction.
 * 
 * FIXME Modify the tests to look for transactional isolation? Or write
 * different tests for that. The initial issue is testing transactional
 * isolation for the object index. I am thinking about _also_ supporting
 * operations outside of a transaction (for use as an embedded database), in
 * which case those need to be tested. The tests for transactional isolation
 * need to verify that write/delete operations in a transaction are NOT visible
 * outside of that transaction, whether in the "root" scope or in any other
 * transaction. Beyond that we get into testing with concurrent modifications by
 * different transactions and proving out the validation and commit logic, as
 * well as proving that transaction isolation begins as of the last committed
 * state when the transaction starts and that concurrent commits do NOT bleed
 * into visibility within uncommitted, still running transactions. We also have
 * to prove that abort does not leave anything lying around, both that would
 * break isolation (unlikely) or just junk that lies around unreclaimed on the
 * slots (or in the index nodes themselves).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTx extends ProxyTestCase {
    
    /**
     * 
     */
    public TestTx() {
    }

    public TestTx(String name) {
        super(name);
    }

    /**
     * @todo Verify no object outside of tx scope, no object in side of tx scope.
     * write object inside of tx scope.  verify not visible outside of tx scope.
     * verify visible inside of tx scope. etc, etc.
     */
    public void test_isolation001() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            // Transaction begins before the write.
            Tx tx0 = new Tx(journal,0);

            // Write a random data version for id 0.
            final int id0 = 0;
            final ByteBuffer expected0v0 = getRandomData(journal);
            journal.write(null, id0, expected0v0);
            assertEquals(expected0v0.array(),journal.read(null, id0, null));

            /*
             * Verify that the version does NOT show up in a transaction created
             * before the write. If the version shows up here it most likely
             * means that the transaction is reading from the current object
             * index state, rather than from the object index state at the time
             * that the transaction began.
             */
            assertNotFound(tx0.read(id0, null));

            // Transaction begins after the write.
            Tx tx1 = new Tx(journal,1);

            /*
             * Verify that the version shows up in a transaction created after
             * the write.
             */
            assertEquals(expected0v0.array(),tx1.read(id0, null));

            /*
             * Update the version outside of the transaction.  This change SHOULD
             * NOT be visible to either transaction.
             */

            final ByteBuffer expected0v1 = getRandomData(journal);
            journal.write(null, id0, expected0v1);
            assertEquals(expected0v1.array(),journal.read(null, id0, null));
            assertNotFound(tx0.read(id0, null));
            assertEquals(expected0v0.array(),tx1.read(id0, null));

            /*
             * Delete the version on the journal. This change SHOULD NOT be
             * visible to either transaction.
             */
            journal.delete(null, id0);
            assertDeleted(journal, id0);
            assertNotFound(tx0.read(id0, null));
            assertEquals(expected0v0.array(),tx1.read(id0, null));

            /*
             * @todo Since commit processing is not implemented, we can not go a
             * lot further with this test. (We could write versions on the
             * different transactions and verify that they were mutually
             * isolated and that those versions do not show up in the journal.)
             */
            
            journal.close();
            
        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }
    
}
