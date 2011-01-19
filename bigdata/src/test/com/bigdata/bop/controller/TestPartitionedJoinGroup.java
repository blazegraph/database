/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jan 19, 2011
 */

package com.bigdata.bop.controller;

import junit.framework.TestCase2;

import com.bigdata.bop.IPredicate;

/**
 * Unit tests for {@link PartitionedJoinGroup}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPartitionedJoinGroup extends TestCase2 {

    /**
     * 
     */
    public TestPartitionedJoinGroup() {
    }

    /**
     * @param name
     */
    public TestPartitionedJoinGroup(String name) {
        super(name);
    }

    public void test_ctor_correctRejection() {

        // null source predicate[].
        try {
            new PartitionedJoinGroup(null/* sourcePreds */, null/* constraints */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // empty source predicate[].
        try {
            new PartitionedJoinGroup(new IPredicate[0], null/* constraints */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // null element in the source predicate[].
        try {
            new PartitionedJoinGroup(new IPredicate[1], null/* constraints */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * @todo test with headPlan, tailPlan.
     * @todo test association of constraints to optional joins.
     * @todo test logic to attach constraints to non-optional joins based on a
     *       given join path (not yet written).
     */
    public void test_something() {
        fail("write tests");
    }
    
}
