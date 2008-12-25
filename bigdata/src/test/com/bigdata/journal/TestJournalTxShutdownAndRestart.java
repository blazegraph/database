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
 * Created on Dec 23, 2008
 */

package com.bigdata.journal;

/**
 * Unit tests for correct shutdown and restart of the journal and its integrated
 * {@link JournalTransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJournalTxShutdownAndRestart extends ProxyTestCase {

    /**
     * 
     */
    public TestJournalTxShutdownAndRestart() {
    }

    /**
     * @param name
     */
    public TestJournalTxShutdownAndRestart(String name) {
        super(name);
    }

    /**
     * @todo test shutdown waits until running read-write transactions commit or
     *       abort.
     * 
     * @todo test shutdown waits until running read-only tx commit or abort.
     * 
     * @todo test shutdownNow() - does it wait or abort running tx?
     */
    public void test_shutdown() {

        fail("write test");

    }

    /**
     * 
     * @todo test various aspects of {@link JournalTransactionService} are
     *       restart safe, including: lastCommitTime (should be read from the
     *       journal), releaseTime (should remain zero since it is not used)
     */
    public void test_restartSafe() {

        fail("write test");

    }

}
