/**
Copyright (C) SYSTAP, LLC 2015.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp.client;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.bigdata.util.StackInfoReport;

import cutthecrap.utils.striterators.ICloseable;

/**
 * 
 * @author Martyn Cutcher
 *
 */
public class AutoCloseHttpClient extends HttpClient implements ICloseable {
	
    private static final transient Logger log = Logger
            .getLogger(AutoCloseHttpClient.class);
    
	final boolean m_autoClose;
	
	StackInfoReport m_stopped = null;
	
	public AutoCloseHttpClient() {
		this(false/*do NOT auto close by default*/);
	}
	
	public AutoCloseHttpClient(final String keystorepath) {
		this(false/*do NOT auto close by default*/, new SslContextFactory(keystorepath));
	}
	
	public AutoCloseHttpClient(final boolean autoclose) {
		this(autoclose, new SslContextFactory(true)); // Use default SSL factory
	}
		
	public AutoCloseHttpClient(final boolean autoclose, final String keystorepath) {
		this(autoclose, new SslContextFactory(keystorepath)); // Use default SSL factory
	}
		
	public AutoCloseHttpClient(final boolean autoclose, final SslContextFactory sslFactory) {
		super(sslFactory); // Use default SSL factory
			
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
	 */
	public void close() {
		if (m_autoClose) {
			
			if (isStopped()) {
				throw new AssertionError("Already stopped!", m_stopped);
			}
			
			try {
				stop();
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {			
				s_active.decrementAndGet();
			}
			
			// for debug
			m_stopped = new StackInfoReport();
		}
	}

	final static AtomicInteger s_active = new AtomicInteger();
	public static int activeCount() {
		return s_active.get();
	}
	
}
