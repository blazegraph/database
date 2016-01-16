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
 * Created on Jan 13, 2015
 */
package com.bigdata.rdf.sail.webapp.client;

import org.eclipse.jetty.client.HttpClient;

/**
 * Factory for {@link HttpClient} objects.
 * 
 * @author bryan
 */
public interface IHttpClientFactory {

	/**
	 * Starts an {@link HttpClient}.
	 * <p>
	 * Note: The caller MUST use {@link HttpClient#stop()} to terminate the
	 * returned {@link HttpClient} when they are done with it. Failure to do so
	 * will leak resources.
	 * 
	 * @return The returned object can (and should) be used for multiple
	 *         connections as long as the same configuration for http
	 *         communications should be used for those connections.
	 */
	HttpClient newInstance();
	
}
