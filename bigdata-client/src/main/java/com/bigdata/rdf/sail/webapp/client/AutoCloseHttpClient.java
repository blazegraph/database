/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2015.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp.client;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.bigdata.util.StackInfoReport;

/**
 * A {@link HttpClient} that supports the {@link AutoCloseable} protocol and
 * which will be transparently closed by the
 * {@link RemoteRepositoryManager} if
 * {@link RemoteRepositoryManager#close()} is invoked.
 * <p>
 * Note: Do NOT use an instance of this class if you wish to share the same
 * {@link HttpClient} across multiple {@link RemoteRepositoryManager}
 * instances.
 * 
 * @author Martyn Cutcher
 */
public class AutoCloseHttpClient extends HttpClient implements AutoCloseable {
	
//    private static final transient Logger log = Logger
//            .getLogger(AutoCloseHttpClient.class);
    
    /**
     * The #of running instances of the {@link AutoCloseHttpClient} - debug only.
     */
	private final static AtomicInteger s_active = new AtomicInteger();
	
	/**
	 * The stack trace of the caller when the {@link AutoCloseHttpClient} is
	 * stopped.
	 */
	@SuppressWarnings("unused")
	private volatile StackInfoReport m_stopped = null;
	
	public AutoCloseHttpClient(final SslContextFactory sslFactory) {
		
		super(sslFactory); 

	}

	@Override
	protected void doStart() throws Exception {

		super.doStart();
		
		s_active.incrementAndGet();
		
	}
	
	@Override
	protected void doStop() throws Exception {

		super.doStop();

		// for debug
		m_stopped = new StackInfoReport();

		s_active.decrementAndGet();

	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Called from {@link RemoteRepositoryManager} when it is closed.
	 * 
	 * @throws Exception
	 */
	@Override
	public void close() throws Exception {

		if (isStopping() || isStopped())
			return;

		stop();

	}

}
