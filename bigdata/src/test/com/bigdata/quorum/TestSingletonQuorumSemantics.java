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

import com.bigdata.journal.ha.QuorumException;

/**
 * FIXME Test the quorum semantics for a singleton quorum. This test suite
 * allows us to verify that each quorum state change is translated into the
 * appropriate methods against the public API of the quorum client or quorum
 * member.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSingletonQuorumSemantics extends AbstractQuorumTestCase {

    /**
     * 
     */
    public TestSingletonQuorumSemantics() {
    }

    /**
     * @param name
     */
    public TestSingletonQuorumSemantics(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        k = 1;
        super.setUp();
    }

    /**
     * Unit test verifies that the {@link QuorumMember} receives synchronous
     * state change messages when it is added to and removed from a
     * {@link Quorum}.
     */
    public void test_memberAddRemove() {
        
        assertFalse(clients[0].isMember());
        
        fixture.memberAdd(clients[0].getServiceId());
        
        assertTrue(clients[0].isMember());
        
        fixture.memberRemove(clients[0].getServiceId());
        
        assertFalse(clients[0].isMember());

    }

    /**
     * Unit test verifies that the {@link QuorumMember} receives synchronous
     * state change messages when it is added to and removed from a
     * {@link Quorum}'s write pipeline.
     * 
     * @todo Tnis test also verifies that we need to be a member of the quorum
     *       in order to be added to the write pipeline. If that changes, then
     *       update this unit test.
     */
    public void test_pipelineAddRemove() {
        
        assertFalse(clients[0].isPipelineMember());

        try {
            fixture.pipelineAdd(clients[0].getServiceId());
            fail("Expected " + QuorumException.class);
        } catch (QuorumException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        fixture.memberAdd(clients[0].getServiceId());

        fixture.pipelineAdd(clients[0].getServiceId());

        assertTrue(clients[0].isPipelineMember());
        
        fixture.pipelineRemove(clients[0].getServiceId());
        
        assertFalse(clients[0].isPipelineMember());

        fixture.memberRemove(clients[0].getServiceId());

        assertFalse(clients[0].isMember());

    }
    
}
