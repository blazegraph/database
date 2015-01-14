/**
Copyright (C) SYSTAP, LLC 2014.  All rights reserved.

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

	}

	@Override
	public HttpClient newInstance() {

		final SslContextFactory sslFactory;

		final String sslKeystorePath = System.getProperty(
				Options.SSL_KEYSTORE_PATH, Options.DEFAULT_SSL_KEYSTORE_PATH);

		if (sslKeystorePath == null) {

			sslFactory = new SslContextFactory(true/* trustAll */);

		} else {

			sslFactory = new SslContextFactory(sslKeystorePath);

		}

		final AutoCloseHttpClient cm = new AutoCloseHttpClient(sslFactory);

		try {

			final boolean followRedirects = Boolean
					.valueOf(System.getProperty(Options.FOLLOW_REDIRECTS,
							Options.DEFAULT_FOLLOW_REDIRECTS));

			if (followRedirects) {

				// Allow redirects.
				cm.setFollowRedirects(true);

			}

			// Start the client.
			cm.start();

		} catch (Exception e) {

			throw new RuntimeException("Unable to start HttpClient", e);

		}

		return cm;

	}

}
