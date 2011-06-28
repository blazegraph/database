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

import junit.framework.TestCase;

/**
 * Unit tests for {@link RunState}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRunState extends TestCase {

    /**
     * 
     */
    public TestRunState() {
   
    }

    /**
     * @param arg0
     */
    public TestRunState(String arg0) {
        super(arg0);
   
    }

    /**
     * Unit tests for legal and illegal state transitions.
     */
    public void test_stateMachine() {

        assertTrue(RunState.Active.isTransitionAllowed(RunState.Prepared));
        assertTrue(RunState.Active.isTransitionAllowed(RunState.Committed));
        assertTrue(RunState.Active.isTransitionAllowed(RunState.Aborted));
        
        assertFalse(RunState.Prepared.isTransitionAllowed(RunState.Active));
        assertTrue(RunState.Prepared.isTransitionAllowed(RunState.Committed));
        assertTrue(RunState.Prepared.isTransitionAllowed(RunState.Aborted));
        
        assertFalse(RunState.Committed.isTransitionAllowed(RunState.Active));
        assertFalse(RunState.Committed.isTransitionAllowed(RunState.Prepared));
        assertFalse(RunState.Committed.isTransitionAllowed(RunState.Aborted));
        
        assertFalse(RunState.Aborted.isTransitionAllowed(RunState.Active));
        assertFalse(RunState.Aborted.isTransitionAllowed(RunState.Prepared));
        assertFalse(RunState.Aborted.isTransitionAllowed(RunState.Committed));
        
    }

    /**
     * Verify that a NOP state change is allowed (this is used when there are
     * multiple committers for a distributed transaction since (as a
     * convenience) more than one may instruct us to make the same state
     * change).
     */
    public void test_selfTransitionOk() {

        assertTrue(RunState.Active.isTransitionAllowed(RunState.Active));
        assertTrue(RunState.Prepared.isTransitionAllowed(RunState.Prepared));
        assertTrue(RunState.Committed.isTransitionAllowed(RunState.Committed));
        assertTrue(RunState.Aborted.isTransitionAllowed(RunState.Aborted));

    }
    
}
