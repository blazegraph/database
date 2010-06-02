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

/**
 * Test the quorum semantics for a highly available quorum of 3 services. The
 * main points to test here are the particulars of events not observable with a
 * singleton quorum, including a service join which does not trigger a quorum
 * meet, a service leave which does not trigger a quorum break, a leader leave,
 * etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHA3QuorumSemantics extends AbstractQuorumTestCase {

    /**
     * 
     */
    public TestHA3QuorumSemantics() {
    }

    /**
     * @param name
     */
    public TestHA3QuorumSemantics(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        k = 3;
        super.setUp();
    }

    public void test_something() {
        fail("write tests");
    }

}
