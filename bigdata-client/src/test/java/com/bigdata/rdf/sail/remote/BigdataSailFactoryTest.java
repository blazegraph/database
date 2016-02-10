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

package com.bigdata.rdf.sail.remote;

import junit.framework.TestCase;

import org.junit.Test;

import com.bigdata.util.httpd.Config;

/**
 * Test suite for the {@link BigdataSailFactory}.
 */
public class BigdataSailFactoryTest extends TestCase {

	//The correctly normalized value for the remote repository
	protected static final String remoteRepositoryUrl = Config.DEFAULT_ENDPOINT;
	
	//The correctly normalized value for the remote repository
	protected static final String remoteRepositoryNamespaceUrl = Config.DEFAULT_ENDPOINT
			+ "/namespace/NAMESPACE";

	@Test
	public void testWithSparql() {
		
		String serviceEndpoint = Config.DEFAULT_ENDPOINT + "/sparql";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testWithoutSparql() {
		
		String serviceEndpoint = Config.DEFAULT_ENDPOINT + "/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testWithoutSparqlAndNoTrailingSlash() {
		
		String serviceEndpoint =  Config.DEFAULT_ENDPOINT;
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testHostOnly() {
		
		String serviceEndpoint = "http://" + Config.DEFAULT_HOST + ":" + Config.BLAZEGRAPH_HTTP_PORT + "/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testHostOnlyNoTrailingSlash() {
		
		String serviceEndpoint = "http://" + Config.DEFAULT_HOST + ":" + Config.BLAZEGRAPH_HTTP_PORT;
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testWithNamespaceNoSparql() {
		
		String serviceEndpoint = Config.DEFAULT_ENDPOINT + "/namespace/NAMESPACE";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryNamespaceUrl, normalizedServiceURL);
	}
	
	@Test
	public void testWithNamespaceNoSparqlWithTrailingSlash() {
		
		String serviceEndpoint = Config.DEFAULT_ENDPOINT + "/namespace/NAMESPACE/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryNamespaceUrl, normalizedServiceURL);
	}
	
	@Test
	public void testWithNamespaceSparql() {
		
		String serviceEndpoint = Config.DEFAULT_ENDPOINT + "/namespace/NAMESPACE/sparql";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryNamespaceUrl, normalizedServiceURL);
	}
	
	@Test
	public void testWithNamespaceSparqlTrailingSlash() {
		
		String serviceEndpoint = Config.DEFAULT_ENDPOINT + "/namespace/NAMESPACE/sparql/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryNamespaceUrl, normalizedServiceURL);
	}

}
