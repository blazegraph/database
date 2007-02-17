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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.Properties;

/**
 * Test suite for the integration of the {@link Journal} and the {@link ITx}
 * implementations.
 * 
 * @todo the tests in this suite are stale and need to be reviewed, possibly
 *       revised or replaced, and certainly extended.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTxJournalProtocol extends ProxyTestCase {

    public TestTxJournalProtocol() {
    }

    public TestTxJournalProtocol(String name) {
        super(name);
    }

    /**
     * Test verifies that duplicate transaction identifiers are detected in the
     * case where the first transaction is active.
     */
    public void test_duplicateTransactionIdentifiers01() throws IOException {
        
        final Properties properties = getProperties();

        try {

            Journal journal = new Journal(properties);

            Tx tx0 = new Tx(journal,0);

            try {

                // Try to create another transaction with the same identifier.
                new Tx(journal,0);
                
                fail( "Expecting: "+IllegalStateException.class);
                
            }
            
            catch( IllegalStateException ex ) {
             
                System.err.println("Ignoring expected exception: "+ex);
                
            }
            
            tx0.abort();
            
            journal.close();

        } finally {

            deleteTestJournalFile();

        }       
        
    }
    
    /**
     * Test verifies that duplicate transaction identifiers are detected in the
     * case where the first transaction has already prepared.
     * 
     * @todo The {@link Journal} does not maintain a collection of committed
     *       transaction identifier for transactions that have already
     *       committed. However, it might make sense to maintain a transient
     *       collection that is rebuilt on restart of those transactions that
     *       are waiting for GC. Also, it may be possible to summarily reject
     *       transaction identifiers if they are before a timestamp when a
     *       transaction service has notified the journal that no active
     *       transactions remain before that timestamp.  If those modifications
     *       are made, then add the appropriate tests here.
     */
    public void test_duplicateTransactionIdentifiers02() throws IOException {
        
        final Properties properties = getProperties();

        try {

            Journal journal = new Journal(properties);

            ITx tx0 = new Tx(journal,0);

            tx0.prepare();
            
            try {

                // Try to create another transaction with the same identifier.
                new Tx(journal,0);
                
                fail( "Expecting: "+IllegalStateException.class);
                
            }
            
            catch( IllegalStateException ex ) {
             
                System.err.println("Ignoring expected exception: "+ex);
                
            }

            tx0.abort();
            
            journal.close();

        } finally {

            deleteTestJournalFile();

        }       
        
    }

}
