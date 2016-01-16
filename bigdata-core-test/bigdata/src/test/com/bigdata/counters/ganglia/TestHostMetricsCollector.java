/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jan 30, 2012
 */

package com.bigdata.counters.ganglia;

import com.bigdata.counters.ganglia.HostMetricsCollector;

import junit.framework.TestCase;

/**
 * Unit tests for {@link HostMetricsCollector}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHostMetricsCollector extends TestCase {

    public TestHostMetricsCollector() {

    }
    
    public TestHostMetricsCollector(final String name) {
        super(name);
    }

    /**
     * Unit test verifies that the filter will only pass the per-host metrics.
     */
    public void test_filter_host_01() {

        doMatchTest("/hostname/CPU/Foo", true);
    }

    public void test_filter_host_02() {

        doMatchTest("/hostname/Memory/Bar", true);

    }

    public void test_filter_host_03() {

        doMatchTest("/192.168.1.10/CPU/% IO Wait", true);

    }

    public void test_filter_host_04() {

        doMatchTest("/192.168.1.10/PhysicalDisk/Bytes Read Per Second", true);

    }

    public void test_filter_service_01() {

        doMatchTest("/hostname/service/Goo", false);

    }

    private void doMatchTest(final String input, final boolean expected) {

        final boolean actual = HostMetricsCollector.filter.matcher(input)
                .matches();

        assertEquals("For input=[" + input + "]", expected, actual);

    }

}
