package com.bigdata.rdf.sail.webapp.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;

public class JettyResponseListener extends InputStreamResponseListener {
	
    private static final transient Logger log = Logger
            .getLogger(JettyResponseListener.class);
    
	Response m_response;
	
	private void ensureResponse() {
		if (m_response == null) {
			try {
				final boolean traceEnabled = log.isTraceEnabled();
				
				final long start = traceEnabled ? System.currentTimeMillis() : 0;
				
				m_response = get(300, TimeUnit.SECONDS); // wait up to 5 minutes!
				
				if (traceEnabled)
					log.trace("Response in " + (System.currentTimeMillis()-start) + "ms");
			} catch (InterruptedException | TimeoutException
					| ExecutionException e) {
				throw new RuntimeException(e);
			}
		}		
	}
	
	public String getContentType() {
		ensureResponse();
		
		final HttpFields headers = m_response.getHeaders();
		
		final String[] contentSpec = headers.get(HttpHeader.CONTENT_TYPE).split(";");
		
		return contentSpec[0];		
	}
	
	public String getContentEncoding() {
		ensureResponse();
				
		final HttpFields headers = m_response.getHeaders();
		
		final String[] contentSpec = headers.get(HttpHeader.CONTENT_TYPE).split(";");
		
		// charset=
		return contentSpec[contentSpec.length-1];		
	}

	public int getStatus() {
		ensureResponse();
		
		return m_response.getStatus();
	}

	public String getReason() {
		ensureResponse();
		
		return null;
	}

	public HttpFields getHeaders() {
		ensureResponse();
		
		return m_response.getHeaders();
	}

}
