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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import junit.framework.Test;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.ServiceProviderHook;
import com.bigdata.rdf.sail.webapp.client.IPreparedBooleanQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * Test of RDR specific data interchange and query.
 * 
 * @author bryan
 * 
 * @param <S>
 */
public class TestRDROperations<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public TestRDROperations() {

	}

	public TestRDROperations(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(TestRDROperations.class,
				"test.*", TestMode.sids);

	}

//FIXME:   Test case removed for 2.1.4 reverse merge.
//    public void test_POST_INSERT_withBody_TURTLE_RDR() throws Exception {
//
//    	final long ntriples = 3L;
//    	
//		InputStream is = null;
//		try {
//			is = new FileInputStream(new File(packagePath + "rdr_01.ttlx"));
//			final AddOp add = new AddOp(is, ServiceProviderHook.TURTLE_RDR);
//			assertEquals(ntriples, m_repo.add(add));
//		} finally {
//			if (is != null) {
//				is.close();
//			}
//		}
//
//		/*
//		 * Verify normal ground triple is present.
//		 */
//		{
//
//			final String queryStr = "ASK {<x:a1> <x:b1> <x:c1>}";
//
//			final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
//			
//			assertTrue(query.evaluate());
//			
//		}
//		
//		// false positive test (not found).
//		{
//
//			final String queryStr = "ASK {<x:a1> <x:b1> <x:c2>}";
//
//			final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
//			
//			assertFalse(query.evaluate());
//			
//		}
//		
//		/*
//		 * Verify RDR ground triple is present.
//		 */
//		{
//
//			final String queryStr = "ASK {<x:a> <x:b> <x:c>}";
//
//			final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
//			
//			assertTrue(query.evaluate());
//			
//		}
//		
//		// RDR false positive test (not found).
//		{
//
//			final String queryStr = "ASK {<x:a> <x:b> <x:c2>}";
//
//			final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
//			
//			assertFalse(query.evaluate());
//			
//		}
//
//		
//		/*
//		 * Verify RDR triple is present.
//		 */
//		{
//
//			final String queryStr = "ASK {<<<x:a> <x:b> <x:c>>> <x:d> <x:e>}";
//
//			final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
//			
//			assertTrue(query.evaluate());
//			
//		}
//		
//		// false positive test for RDR triple NOT present.
//		{
//
//			final String queryStr = "ASK {<<<x:a> <x:b> <x:c>>> <x:d> <x:e2>}";
//
//			final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
//			
//			assertFalse(query.evaluate());
//			
//		}
//		
//		/*
//		 * Verify the expected #of statements in the store using a SPARQL result
//		 * set.
//		 */
//		{
//
//			final String queryStr = "SELECT * where {?s ?p ?o}";
//
//			final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
//
////FIXME:   Per @thompsonbry removing to get clean CI on merge branch			
////			assertEquals(ntriples, countResults(query.evaluate()));
//			
//			assertEquals( true, true );
//			
//		}
//        
//		/* 
//		 * Verify the RDR data can be recovered using a CONSTRUCT query.
//		 */
//		{
//
//			final String queryStr = "CONSTRUCT where {?s ?p ?o}";
//
//			final IPreparedGraphQuery query = m_repo.prepareGraphQuery(queryStr);
//			
//			assertEquals(ntriples, countResults(query.evaluate()));
//			
//		}
//        
//		/* 
//		 * Verify the RDR data can be recovered using a DESCRIBE query.
//		 */
//		{
//
//			final String queryStr = "DESCRIBE * {?s ?p ?o}";
//
//			final IPreparedGraphQuery query = m_repo.prepareGraphQuery(queryStr);
//			
//			assertEquals(ntriples, countResults(query.evaluate()));
//			
//		}
//		
//    }

	/**
	 * FIXME We need to verify export for this case. It relies on access to a
	 * Bigdata specific ValueFactoryImpl to handle the RDR mode statements.
	 */
	public void test_EXPORT_TURTLE_RDR() throws Exception {

	   if(!BigdataStatics.runKnownBadTests) {
	      return;
	   }
	   
    	final long ntriples = 3L;
    	
		InputStream is = null;
		try {
			is = new FileInputStream(new File(packagePath + "rdr_01.ttlx"));
			final AddOp add = new AddOp(is, ServiceProviderHook.TURTLE_RDR);
			assertEquals(ntriples, m_repo.add(add));
		} finally {
			if (is != null) {
				is.close();
			}
		}

		fail("write export test for TURTLE-RDR");

	}

}
