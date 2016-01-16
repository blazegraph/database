/**
1Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

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

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * This implementation:
 * <ul>
 * <li>Creates an {@link AutoCloseHttpClient}.</li>
 * <li>Sets up redirect handling.</li>
 * <li>Sets a default SSL context factory. By default the
 * {@link SslContextFactory} will trusts all certificates. This allows encrypted
 * communications with any SSL endpoint, but it does not verify the identity
 * associated with that certificate. You can change the default behavior by
 * specify the {@link Options#SSL_KEYSTORE_PATH} system property.</li>
 * </ul>
 * 
 * @see Options
 */
public final class DefaultHttpClientFactory implements IHttpClientFactory {

	private static final Logger log = Logger.getLogger(DefaultHttpClientFactory.class);
	
	public interface Options {

		/**
		 * This is the name of an optional System property that may be used to
		 * override the {@link IHttpClientFactory} implementation class (default
		 * {@value #DEFAULT_SSL_KEYSTORE_PATH}).
		 */
		final static public String SSL_KEYSTORE_PATH = DefaultHttpClientFactory.class
				.getName() + ".SSLKeystorePath";

		/**
		 * There is no default SSL keystore. When not specified, all SSL end
		 * points will be trusted.
		 */
		final static public String DEFAULT_SSL_KEYSTORE_PATH = null;

		final static public String FOLLOW_REDIRECTS = DefaultHttpClientFactory.class
				.getName() + ".followRedirects";

		final static public String DEFAULT_FOLLOW_REDIRECTS = "true";

	    /**
		 * The name of the system property that may be used to specify the maximum
		 * size (in bytes) for the Jetty response buffer (default @value
		 * {@value #DEFAULT_RESPONSE_BUFFER_SIZE})
		 * 
		 * @see <a href="http://trac.blazegraph.com/ticket/1092"> Set query timeout and
		 *      response buffer length on jetty response listener</a>
		 * @see <a href="http://trac.blazegraph.com/ticket/1096"> Set jetty request
		 *      buffer size</a>
		 */
		static public final String RESPONSE_BUFFER_SIZE = DefaultHttpClientFactory.class
				.getName() + ".responseBufferSize";
	    
		/**
		 * The default maximum size of the jetty response buffer (@value
		 * {@value #DEFAULT_RESPONSE_BUFFER_SIZE}).
		 * <p>
		 * Note: The default value for the jetty platform is 16kb.  
		 */
	    static public final int DEFAULT_RESPONSE_BUFFER_SIZE = 16 * 1024;
	    
		/**
		 * The name of the system property that may be used to specify the maximum
		 * size (in bytes) for the Jetty request buffer (default @value
		 * {@value #DEFAULT_REQUEST_BUFFER_SIZE})
		 * 
		 * @see <a href="http://trac.blazegraph.com/ticket/1096"> Set jetty request
		 *      buffer size</a>
		 */
		public static final String REQUEST_BUFFER_SIZE = DefaultHttpClientFactory.class
				.getName() + ".requestBufferSize";

		/**
		 * The default maximum size of the jetty request buffer (@value
		 * {@value #DEFAULT_REQUEST_BUFFER_SIZE}).
		 * <p>
		 * Note: The default value for the jetty platform is 4kb.
		 */
		public static final int DEFAULT_REQUEST_BUFFER_SIZE = 16 * 1024;

	}

	@Override
	public HttpClient newInstance() {

		final AutoCloseHttpClient cm;
		{
			final String sslKeystorePath = System.getProperty(
					Options.SSL_KEYSTORE_PATH,
					Options.DEFAULT_SSL_KEYSTORE_PATH);

			if (log.isInfoEnabled()) {
				log.info(Options.SSL_KEYSTORE_PATH + "=" + sslKeystorePath);
			}

			final SslContextFactory sslFactory;

			if (sslKeystorePath == null) {

				sslFactory = new SslContextFactory(true/* trustAll */);

			} else {

				sslFactory = new SslContextFactory(sslKeystorePath);

			}

			cm = new AutoCloseHttpClient(sslFactory);

		}

		try {

			final boolean followRedirects = Boolean
					.valueOf(System.getProperty(Options.FOLLOW_REDIRECTS,
							Options.DEFAULT_FOLLOW_REDIRECTS));

			if (log.isInfoEnabled()) {
				log.info(Options.FOLLOW_REDIRECTS + "=" + followRedirects);
			}
			
			if (followRedirects) {

				// Allow redirects.
				cm.setFollowRedirects(true);

			}

			/*
			 * Configure the http request buffer capacity.
			 * 
			 * See #1097.
			 */
			{
				final int bufferSize = Integer.valueOf(System
						.getProperty(Options.REQUEST_BUFFER_SIZE,
								Integer.toString(Options.DEFAULT_REQUEST_BUFFER_SIZE)));

				if (log.isInfoEnabled()) {
					log.info(Options.REQUEST_BUFFER_SIZE+ "=" + bufferSize);
				}

				cm.setRequestBufferSize(bufferSize);
			}
			
			/*
			 * Configure the http response buffer capacity.
			 * 
			 * See #1092.
			 */
			{

				final int bufferSize = Integer
						.parseInt(System.getProperty(
								Options.RESPONSE_BUFFER_SIZE,
								Integer.toString(Options.DEFAULT_RESPONSE_BUFFER_SIZE)));

				if (log.isInfoEnabled()) {
					log.info(Options.RESPONSE_BUFFER_SIZE + "=" + bufferSize);
				}

				cm.setResponseBufferSize(bufferSize);

			}

			// Start the client.
			cm.start();

		} catch (Exception e) {

			throw new RuntimeException("Unable to start HttpClient", e);

		}

		return cm;

	}

}
