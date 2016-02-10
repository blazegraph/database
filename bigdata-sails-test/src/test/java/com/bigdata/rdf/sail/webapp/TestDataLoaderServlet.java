/*

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
import java.util.Properties;
import java.util.UUID;

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;

import junit.framework.Test;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.properties.PropertiesFormat;
import com.bigdata.rdf.properties.PropertiesParserFactory;
import com.bigdata.rdf.properties.PropertiesParserRegistry;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Proxied test suite for {@link DataLoaderServlet}
 * 
 * @author beebs
 */
public class TestDataLoaderServlet<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	final static String BASE = "com/bigdata/rdf/sail/webapp/";

	public TestDataLoaderServlet() {

	}

	public TestDataLoaderServlet(final String name) {

		super(name);

	}

	static public Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(
				TestDataLoaderServlet.class, "test_load01",
				Collections.singleton(BufferMode.DiskRW), TestMode.quads);
	}

	public void test_load01() throws Exception {

		final String kbPropsURL = this.getClass()
				.getResource("dataloader.props").getFile();
		final String dataURL = this.getClass().getResource("sample-data.ttl")
				.getFile();

		final PropertiesFormat format = PropertiesFormat.XML;
		final PropertiesParserFactory parserFactory = PropertiesParserRegistry
				.getInstance().get(format);

		final Properties loaderProps = parserFactory.getParser().parse(
				this.getClass().getResourceAsStream("dataloader.xml"));

		final String randomNS = "kb" + UUID.randomUUID();

		{ // verify does not exist.
			try {
				m_mgr.getRepositoryProperties(randomNS);
				fail("Should not exist: " + randomNS);
			} catch (HttpException ex) {
				// Expected status code.
				assertEquals(404, ex.getStatusCode());
			}
		}

		// Set the random namespace and the correct resource paths
		loaderProps.setProperty("namespace", randomNS);
		loaderProps.setProperty("quiet", "true");
		loaderProps.setProperty("verbose", "0");
		loaderProps.setProperty("propertyFile", kbPropsURL);
		loaderProps.setProperty("fileOrDirs", dataURL);

		m_mgr.doDataLoader(loaderProps);

		RemoteRepository repo = m_mgr.getRepositoryForNamespace(randomNS);

		{ // verify it was created by the data loader.
			final Properties p = m_mgr.getRepositoryProperties(randomNS);
			assertNotNull(p);

			log.warn("Found properties for namespace " + randomNS);
		}

		final BigdataSailRemoteRepositoryConnection cxn = (BigdataSailRemoteRepositoryConnection) repo
				.getBigdataSailRemoteRepository().getConnection();

		try {
			String queryStr = "select * where { ?s ?p ?o }";
			final org.openrdf.query.TupleQuery tq = cxn.prepareTupleQuery(
					QueryLanguage.SPARQL, queryStr);
			final TupleQueryResult tqr = tq.evaluate();
			try {
				int cnt = 0;
				while (tqr.hasNext()) {
					tqr.next();
					cnt++;
				}
				if (cnt == 0) {
					fail("DataLoaderServlet did not add any statements.");
				}
				assertTrue(cnt > 0);
			} finally {
				tqr.close();
			}
		} finally {
			cxn.close();
		}

	}

}
