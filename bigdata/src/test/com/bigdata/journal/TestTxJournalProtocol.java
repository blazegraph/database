/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.io.IOException;

/**
 * Test suite for the integration of the {@link Journal} and the {@link ITx}
 * implementations.
 * 
 * @todo the tests in this suite are stale and need to be reviewed, possibly
 *       revised or replaced, and certainly extended. The main issue is that
 *       they do not test the basic contract for the journal, transactions, and
 *       the transaction manager service but instead test some particulars of
 *       the implementation that might not even be the right way to manage that
 *       contract.
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

        Journal journal = new Journal(getProperties());

        final long startTime = journal.nextTimestamp();

        Tx tx0 = new Tx(journal, journal, startTime, false);

        try {

            // Try to create another transaction with the same identifier.
            new Tx(journal, journal, startTime, false);

            fail("Expecting: " + IllegalStateException.class);

        }

        catch (IllegalStateException ex) {

            System.err.println("Ignoring expected exception: " + ex);

        }

        tx0.abort();

        journal.destroy();

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
     *       transactions remain before that timestamp. If those modifications
     *       are made, then add the appropriate tests here.
     */
    public void test_duplicateTransactionIdentifiers02() throws IOException {

        Journal journal = new Journal(getProperties());

        final long startTime = journal.nextTimestamp();

        ITx tx0 = new Tx(journal, journal, startTime, false/*readOnly*/);
        
        tx0.prepare(0L/*journal.nextTimestamp()*/);

        try {

            // Try to create another transaction with the same start time.
            new Tx(journal, journal, startTime, false);

            fail("Expecting: " + IllegalStateException.class);

        }

        catch (IllegalStateException ex) {

            System.err.println("Ignoring expected exception: " + ex);

        }

        tx0.abort();

        journal.destroy();

    }

}
