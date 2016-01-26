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

package com.bigdata.rdf.sail.webapp;


import junit.framework.Test;

import com.bigdata.journal.IIndexManager;

/**
 * Proxied test suite.
 * We test the behavior reported in trac 727.
 */
public class TestInsertFilterFalse727<S extends IIndexManager> extends
        AbstractSimpleInsertTest<S> {

	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestInsertFilterFalse727.class,"test.*", TestMode.quads,TestMode.sids,TestMode.triples);
	}
    public TestInsertFilterFalse727() {

    }

	public TestInsertFilterFalse727(final String name) {

		super(name);

	}

	public void testInsertWhereTrue()
            throws Exception
    {
        executeInsert("FILTER ( true )", true);
    }
	public void testInsertWhereFalse()
            throws Exception
    {
        executeInsert("FILTER ( false )", false);
    }
    

    public void testInsertWhereOptionallyTrue()
            throws Exception
    {
        executeInsert("OPTIONAL { FILTER ( true ) }", true);
    }

    public void testInsertWhereOptionallyFalse()
            throws Exception
    {
        executeInsert("OPTIONAL { FILTER ( false ) }", true);
    }

}
