/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Mar 27, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;

/**
 * Options for the HTTP connection.
 */
public class ConnectOptions extends AbstractConnectOptions {

	/**
	 * Request entity.
	 */
	public HttpEntity entity = null;

	public ConnectOptions(String serviceURL) {
		super(serviceURL);
	}

	/**
	 * Add query params to an {@link IMimeTypes#MIME_APPLICATION_URL_ENCODED}
	 * entity.
	 */
	public static HttpEntity getFormEntity(
			final Map<String, String[]> requestParams) throws Exception {

		final List<NameValuePair> formparams = new ArrayList<NameValuePair>();

		if (requestParams != null) {
			for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
				final String name = e.getKey();
				final String[] vals = e.getValue();

				if (vals == null) {
					formparams.add(new BasicNameValuePair(name, null));
				} else {
					for (String val : vals) {
						formparams.add(new BasicNameValuePair(name, val));
					}
				}
			} // next Map.Entry
		}

		return new UrlEncodedFormEntity(formparams, RemoteRepository.UTF8);

	}

}
