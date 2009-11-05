/*

Copyright (C) SYSTAP, LLC 2006-2009.  All rights reserved.

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
 * Created on Nov 4, 2009
 */

package com.bigdata.counters.linux;

import junit.framework.TestCase2;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKernelVersion extends TestCase2 {

    /**
     * 
     */
    public TestKernelVersion() {
    }

    /**
     * @param name
     */
    public TestKernelVersion(String name) {
        super(name);
    }

    public void test_correctRejection() {

        try {
            new KernelVersion(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        try {
            new KernelVersion("a.b.c");
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    public void test_kernelVersion01() {

        final KernelVersion t = new KernelVersion("2.6.31");
        assertEquals(2, t.version);
        assertEquals(6, t.major);
        assertEquals(31, t.minor);

    }

    /**
     * Observed on a fedora 10 build <code>2.6.31-302-rs</code>.
     */
    public void test_kernelVersion02() {

        final KernelVersion t = new KernelVersion("2.6.31-302-rs");
        assertEquals(2, t.version);
        assertEquals(6, t.major);
        assertEquals(31, t.minor);

    }

}
