/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

package com.bigdata.rdf.sail;

import junit.framework.TestCase;

import org.junit.Test;

public class BigdataSailFactoryTest extends TestCase {

	//The correctly normalized value for the remote repository
	protected static final String remoteRepositoryUrl = "http://localhost:9999/bigdata";
	
	//The correctly normalized value for the remote repository
	protected static final String remoteRepositoryNamespaceUrl = "http://localhost:9999/bigdata/namespace/NAMESPACE";


	@Test
	public void testWithSparql() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/sparql";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testWithoutSparql() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testWithoutSparqlAndNoTrailingSlash() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testHostOnly() {
		
		String serviceEndpoint = "http://localhost:9999/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testHostOnlyNoTrailingSlash() {
		
		String serviceEndpoint = "http://localhost:9999";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryUrl,normalizedServiceURL);
	}
	
	@Test
	public void testWithNamespaceNoSparql() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/namespace/NAMESPACE";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryNamespaceUrl, normalizedServiceURL);
	}
	
	@Test
	public void testWithNamespaceSparql() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/namespace/NAMESPACE/sparql";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assertEquals(remoteRepositoryNamespaceUrl, normalizedServiceURL);
	}
	

}
