package com.bigdata.rdf.sail.webapp;

import java.util.Collections;

import junit.framework.Test;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.RWStrategy;
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
	 * RWStore is properly closed for SPARQL UPDATE. This bug was introduced
	 * when addressing <a href="http://trac.bigdata.com/ticket/1026> SPARQL
	 * UPDATE with runtime errors causes problems with lexicon indices </a>.
	 * @throws Exception 
	 * 
	 * @see <a href="http://trac.bigdata.com/ticket/1036"> Journal file growth
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

		final int activeTxBefore = rwstore == null ? 0 : rwstore
				.getActiveTxCount();

		m_repo.prepareUpdate("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n"//
+"INSERT DATA\n"//
+"{\n"//
+"  <http://example/book1> dc:title \"A new book\" ; \n"//
+"                          dc:creator \"A.N.Other\" .\n"//
+"}\n"//
).evaluate();
		
		final int activeTxAfter = rwstore == null ? 0 : rwstore
				.getActiveTxCount();

		/*
		 * This value should be unchanged across the SPARQL UPDATE request (it
		 * will be incremented during the processing of the request and then
		 * decremented again during the commit or abort protocol).
		 */
		assertEquals(activeTxBefore, activeTxAfter);

    }
    
}
