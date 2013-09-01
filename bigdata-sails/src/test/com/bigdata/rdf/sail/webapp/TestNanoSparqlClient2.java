/**
Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class TestNanoSparqlClient2<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {

    public TestNanoSparqlClient2() {

    }

	public TestNanoSparqlClient2(final String name) {

		super(name);

	}
	
	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestNanoSparqlClient2.class,"test.*", TestMode.quads,TestMode.sids,TestMode.triples);
	}

    /**
     * Delete everything matching an access path description.
     */
    public void test_IMPLEMENT_ME() throws Exception {

    	final RemoteRepositoryManager rrm = super.m_repo;
    	
    	// do something here
        
    }
    
}
