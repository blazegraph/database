/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

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
/*
 * Created on Mar 21, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import org.eclipse.jetty.client.HttpClient;

/**
 * Factory for {@link HttpClient}.
 * 
 * @see Options
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HttpClientConfigurator {

	public interface Options {

		/**
		 * This is the name of an optional System property that may be used to
		 * override the {@link IHttpClientFactory} implementation class (default
		 * {@value #DEFAULT_CONNECTION_MANAGER_FACTORY_CLASS}).
		 */
		final static public String CONNECTION_MANAGER_FACTORY_CLASS = HttpClientConfigurator.class
				.getName();

		/**
		 * The name of the default {@link IHttpClientFactory} implementation
		 * class.
		 */
		final static public String DEFAULT_CONNECTION_MANAGER_FACTORY_CLASS = DefaultHttpClientFactory.class
				.getName();

	}
	
	/**
	 * Allow a user configurable factory to allow the override of the
	 * HttpClient(s) it will return.
	 * 
	 * @see #CONNECTION_MANAGER_FACTORY_CLASS
	 */
	public static IHttpClientFactory getInstance() {

		final String configuredFactory = System.getProperty(
				Options.CONNECTION_MANAGER_FACTORY_CLASS,
				Options.DEFAULT_CONNECTION_MANAGER_FACTORY_CLASS);

		try {

			@SuppressWarnings("unchecked")
			final Class<IHttpClientFactory> factoryClass = (Class<IHttpClientFactory>) Class
					.forName(configuredFactory);

			if (!IHttpClientFactory.class.isAssignableFrom(factoryClass)) {

				throw new RuntimeException("Invalid option: "
						+ Options.CONNECTION_MANAGER_FACTORY_CLASS + "="
						+ factoryClass + ":: Class does not extend "
						+ IHttpClientFactory.class);

			}

			final IHttpClientFactory factory = factoryClass.newInstance();

			return factory;

		} catch (Exception e) {

			throw new RuntimeException("Could not create "
					+ IHttpClientFactory.class.getSimpleName() + ": " + e, e);

		}

	}

}
