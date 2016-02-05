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

import org.openrdf.model.Graph;
import org.openrdf.model.ValueFactory;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class Test_REST_ServiceDescription<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_ServiceDescription() {

	}

	public Test_REST_ServiceDescription(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_ServiceDescription.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

	/**
	 * Request the SPARQL SERVICE DESCRIPTION for the end point.
	 */
	public void test_SERVICE_DESCRIPTION() throws Exception {

		final Graph g = RemoteRepository
				.asGraph(m_repo.getServiceDescription());

		final ValueFactory f = g.getValueFactory();

		// Verify the end point is disclosed.
		assertEquals(
				1,
				countMatches(g, null/* service */, SD.endpoint,
						f.createURI(m_repo.getSparqlEndPoint())));
//		            f.createURI(m_serviceURL + "/sparql")));

		// Verify description includes supported query and update languages.
		assertEquals(
				1,
				countMatches(g, null/* service */, SD.supportedLanguage,
						SD.SPARQL10Query));
		assertEquals(
				1,
				countMatches(g, null/* service */, SD.supportedLanguage,
						SD.SPARQL11Query));
		assertEquals(
				1,
				countMatches(g, null/* service */, SD.supportedLanguage,
						SD.SPARQL11Update));

		// Verify support for Basic Federated Query is disclosed.
		assertEquals(
				1,
				countMatches(g, null/* service */, SD.feature,
						SD.BasicFederatedQuery));

	}

}
