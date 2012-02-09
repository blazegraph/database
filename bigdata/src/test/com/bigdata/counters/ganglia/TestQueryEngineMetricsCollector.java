/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Jan 30, 2012
 */

package com.bigdata.counters.ganglia;

import junit.framework.TestCase;

/**
 * Unit tests for {@link QueryEngineMetricsCollector}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestQueryEngineMetricsCollector extends TestCase {

    public TestQueryEngineMetricsCollector() {

    }
    
    public TestQueryEngineMetricsCollector(final String name) {
        super(name);
    }

    /**
     * Correct acceptance of query engine counter on a data service.
     */
    public void test_filter_queryEngine_01() {
        doMatchTest(
                "/bigdata10.bigdata.com/service/com.bigdata.service.IDataService/dabb5034-9db0-4218-a6dd-f9032fc05ce6/Query Engine/blue",
                true);
    }

    /**
     * Correct rejection of counter not on a data service.
     */
    public void test_filter_queryEngine_02() {
        doMatchTest(
                "/bigdata10.bigdata.com/service/com.bigdata.service.IMetadataService/dabb5034-9db0-4218-a6dd-f9032fc05ce6/Query Engine/blue",
                false);
    }

    /**
     * Correct rejection of counter on a data service, but whose path is not
     * appropriate for a query engine metric.
     */
    public void test_filter_queryEngine_03() {
        doMatchTest(
                "/bigdata10.bigdata.com/service/com.bigdata.service.IDataService/dabb5034-9db0-4218-a6dd-f9032fc05ce6/Resource Manager/blue",
                false);
    }

    private void doMatchTest(final String input, final boolean expected) {

        final boolean actual = QueryEngineMetricsCollector.filter
                .matcher(input).matches();

        assertEquals("For input=[" + input + "]", expected, actual);

    }

}
