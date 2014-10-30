package com.bigdata.util.http;

import com.bigdata.util.http.IHttpResponse;

public class JettyHttpClient implements IHttpClient {
	final org.eclipse.jetty.client.HttpClient m_client;
	
	public JettyHttpClient() throws Exception {
		m_client = new org.eclipse.jetty.client.HttpClient();
		m_client.start();
	}
	
	public IHttpResponse GET(final String url) throws Exception {
		org.eclipse.jetty.client.api.ContentResponse response = m_client.GET(url);
		 
		return new JettyClientResponse(response);
	}
	
	public void destroy() {
		m_client.destroy();
	}
	
	class JettyClientResponse implements IHttpResponse {

		final org.eclipse.jetty.client.api.ContentResponse m_response;
		
		public JettyClientResponse(final org.eclipse.jetty.client.api.ContentResponse response) {
			m_response = response;
		}
		
		public int getStatusCode() {
			return m_response.getStatus();
		}
	}

	@Override
	public IHttpEntity createEntity() {
		throw new UnsupportedOperationException();
	}

	@Override
	public IHttpRequest newRequest(String uri, String method) {
		throw new UnsupportedOperationException();
	}
}
