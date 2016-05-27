/**
 *
 * Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016. All rights reserved.
 *
 * Contact: SYSTAP, LLC DBA Blazegraph 2501 Calvert ST NW #106 Washington, DC
 * 20008 licenses@blazegraph.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */
package com.bigdata.bop.solutions;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:ariazanov@blazegraph.com">Alexandre Riazanov</a>
 * @since Apr 19, 2016
 */
public class TestAll extends TestCase {

    public TestAll() {
    }

    public TestAll(String name) {
        super(name);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite() {

        final TestSuite suite = new TestSuite("BOp solutions");

        /*
         * Data driven tests.
         */

        // Aggregation.
        suite.addTestSuite(TestGroupByOp.class);

        return suite;

    } // suite()
} // class TestAll
