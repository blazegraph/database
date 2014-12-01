package com.bigdata.rdf.sail.webapp.client;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * 
 * @author Martyn Cutcher
 *
 */
public class JettyHttpClient extends HttpClient {
	
	final boolean m_autoClose;
	
	JettyHttpClient(final boolean autoclose) {
		super(new SslContextFactory(true)); // Use default SSL factory
		
		m_autoClose = autoclose;
		/*
		 * Ensure that the client follows redirects using a standard policy.
		 * 
		 * Note: This is necessary for tests of the webapp structure since the
		 * container may respond with a redirect (302) to the location of the
		 * webapp when the client requests the root URL.
		 */
		setFollowRedirects(true);

	}
	
	public void close() throws Exception {
		if (m_autoClose) {
			stop();
		}
	}
}
