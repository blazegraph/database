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

import org.apache.http.HttpEntity;

/**
 * Options for the HTTP connection.
 */
public class ConnectOptions extends AbstractConnectOptions {

   /**
    * Request entity.
    * 
    * TODO This field is read/written by the {@link RemoteRepository}. We should
    * try to encapsulate this within the scope of the logic that manages its
    * value and pass it through function calls rather than by side effect on
    * this object.
    */
	public HttpEntity entity = null;

	public ConnectOptions(String serviceURL) {
		super(serviceURL);
	}

}
