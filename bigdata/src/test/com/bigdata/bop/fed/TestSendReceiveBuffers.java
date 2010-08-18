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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.fed;

import junit.framework.TestCase2;

/**
 * Unit tests for the low-level NIO operations used to transmit data between
 * services whether mapping binding sets over shards, binding sets over nodes,
 * or shipping elements or bit vectors around.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This capability can be built up based on the work in the HA branch for
 *       low-level replication of the write cache buffers.
 */
public class TestSendReceiveBuffers extends TestCase2 {

    /**
     * 
     */
    public TestSendReceiveBuffers() {
    }

    /**
     * @param name
     */
    public TestSendReceiveBuffers(String name) {
        super(name);
    }
    
    public void test_something() {
        fail("write tests");
    }

}
