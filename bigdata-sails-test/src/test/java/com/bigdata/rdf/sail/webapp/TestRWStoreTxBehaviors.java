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

import java.util.Collections;

import junit.framework.Test;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.RWStrategy;
import com.bigdata.rdf.sail.webapp.client.IPreparedSparqlUpdate;
import com.bigdata.rwstore.RWStore;

/**
 * Tests that are RWStore specific.
 * 
 * @author bryan
 *
 * @param <S>
 */
public class TestRWStoreTxBehaviors<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public TestRWStoreTxBehaviors() {

	}

	public TestRWStoreTxBehaviors(final String name) {

		super(name);

	}

	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestRWStoreTxBehaviors.class,
				"test.*", Collections.singleton(BufferMode.DiskRW), TestMode.triples
				);
	}

   /**
    * Unit test verifies that the native journal transaction counter for the
    * RWStore is properly closed for SPARQL UPDATE. This bug was introduced when
    * addressing <a href="http://trac.blazegraph.com/ticket/1026> SPARQL UPDATE
    * with runtime errors causes problems with lexicon indices </a>.
    * <p>
    * Note: This test will fail if group commit is enabled and the servlet
    * commits the http response before the group commit.
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/1036"> Journal file growth
    *      reported with 1.3.3 </a>
    */
    public void test_SPARQL_UPDATE_Tx_Properly_Closed() throws Exception {
    	
		RWStore rwstore = null;

		if (getIndexManager() instanceof AbstractJournal
				&& ((AbstractJournal) getIndexManager()).getBufferStrategy() instanceof RWStrategy) {

			rwstore = ((RWStrategy) ((AbstractJournal) getIndexManager())
					.getBufferStrategy()).getStore();

		} else {
			
			fail("RWStore is not in use");
			
		}

      final int activeTxBefore = rwstore.getActiveTxCount();
      if (log.isInfoEnabled())
         log.info("activeTxBefore=" + activeTxBefore);

		final IPreparedSparqlUpdate preparedUpdate = m_repo.prepareUpdate("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"//
+"INSERT DATA\n"//
+"{\n"//
+"  <http://example/book1> dc:title \"A new book\" ; \n"//
+"                          dc:creator \"A.N.Other\" .\n"//
+"}\n"//
);
		preparedUpdate.evaluate();
		
      final int activeTxAfter = rwstore.getActiveTxCount();
      if (log.isInfoEnabled())
         log.info("activeTxAfter=" + activeTxAfter);

		/*
		 * This value should be unchanged across the SPARQL UPDATE request (it
		 * will be incremented during the processing of the request and then
		 * decremented again during the commit or abort protocol).
		 */
		assertEquals(activeTxBefore, activeTxAfter);

    }
    
}
