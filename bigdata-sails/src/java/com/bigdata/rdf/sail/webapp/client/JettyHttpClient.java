package com.bigdata.rdf.sail.webapp.client;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.bigdata.util.StackInfoReport;

/**
 * 
 * @author Martyn Cutcher
 *
 */
public class JettyHttpClient extends HttpClient {
	
    private static final transient Logger log = Logger
            .getLogger(JettyHttpClient.class);
    
	final boolean m_autoClose;
	
	StackInfoReport m_stopped = null;
	
	public JettyHttpClient() {
		this(false/*do NOT auto close by default*/);
	}
	
	public JettyHttpClient(final boolean autoclose) {
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
		
		s_active.incrementAndGet();
	}
	
	/**
	 * Called from JettyRemoteRepositoryManager when it is closed.
	 * <p>
	 * If it was created with autoclose==true then it will be stopped.
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception {
		if (m_autoClose) {
			
			if (isStopped()) {
				throw new AssertionError("Already stopped!, shared=" + (this == JettyRemoteRepository.s_sharedClient), m_stopped);
			}
			
			stop();
			
			s_active.decrementAndGet();
			
			// for debug
			m_stopped = new StackInfoReport();
		}
	}

	final static AtomicInteger s_active = new AtomicInteger();
	public static int activeCount() {
		return s_active.get();
	}
	
}
