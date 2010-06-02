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

    public void test_something() {
        fail("write tests");
    }
    
}
