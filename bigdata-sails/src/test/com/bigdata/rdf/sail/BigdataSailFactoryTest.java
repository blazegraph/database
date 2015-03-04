package com.bigdata.rdf.sail;

import static org.junit.Assert.*;

import org.junit.Test;

public class BigdataSailFactoryTest {
	
	protected static final String remoteRepositoryUrl = "http://localhost:9999/bigdata";
	protected static final String remoteRepositoryNamespaceUrl = "http://localhost:9999/bigdata/namespace";


	@Test
	public void test() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/sparql";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assert(remoteRepositoryUrl.equals(normalizedServiceURL));
	}
	
	@Test
	public void test1() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assert(remoteRepositoryUrl.equals(normalizedServiceURL));
	}
	
	@Test
	public void test2() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assert(remoteRepositoryUrl.equals(normalizedServiceURL));
	}
	
	@Test
	public void test3() {
		
		String serviceEndpoint = "http://localhost:9999/";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assert(remoteRepositoryUrl.equals(normalizedServiceURL));
	}
	
	@Test
	public void test4() {
		
		String serviceEndpoint = "http://localhost:9999";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assert(remoteRepositoryUrl.equals(normalizedServiceURL));
	}
	
	@Test
	public void testNamespace1() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/namespace";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assert(remoteRepositoryNamespaceUrl.equals(normalizedServiceURL));
	}
	
	@Test
	public void testNamespace2() {
		
		String serviceEndpoint = "http://localhost:9999/bigdata/namespace/sparql";
		String normalizedServiceURL = 
				BigdataSailFactory.testServiceEndpointUrl(serviceEndpoint);

		assert(remoteRepositoryNamespaceUrl.equals(normalizedServiceURL));
	}

}
