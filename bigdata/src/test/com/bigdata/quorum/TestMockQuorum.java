/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumFixtureClient;

/**
 * Bootstrap unit tests for the {@link AbstractQuorum}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMockQuorum extends TestCase2 {

    /**
     * 
     */
    public TestMockQuorum() {
    }

    /**
     * @param name
     */
    public TestMockQuorum(String name) {
        super(name);
    }

    /**
     * Tests of various illegal constructor calls.
     */
    public void test_ctor_correctRejection() {

        try {
            final int k = 2;
            new MockQuorumFixture(k);
            fail("Expected: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info(ex);
        }        

        try {
            final int k = 0;
            new MockQuorumFixture(k);
            fail("Expected: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info(ex);
        }        

        try {
            final int k = -1;
            new MockQuorumFixture(k);
            fail("Expected: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info(ex);
        }        

    }

    /**
     * Simple start()/terminate() test.
     * 
     * @todo test double-terminate is not an error.
     * @todo test double-start is an error or ignored.
     * @todo test start/terminate/start/terminate is ok.
     */
    public void test_start_terminate() {

        final int k = 1;
        Quorum q = new MockQuorumFixture(k);
        QuorumClient c = new MockQuorumMember(q);
        q.start(c);
        q.terminate();

    }

}
