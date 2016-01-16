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

	private volatile String requestURL;

   /**
    * Return the effective request URL
    * 
    * @param contextPath
    *           The context path of the web application.
    * @param useLBS
    *           When <code>true</code> the load balancer aware paths will be
    *           used.
    * 
    * @return The effective request URL (cached).
    */
	public String getRequestURL(final String contextPath, final boolean useLBS) {
	   
      if (contextPath == null)
         throw new IllegalArgumentException();
	   
      if (requestURL == null) {

         if (useLBS) {
            /*
             * Use the HA load balancer.
             */
            // Index of the WebApp ContextPath in the serviceURL.
            final int startContextPath = serviceURL.indexOf(contextPath);
            // Index of the last character in the context path.
            final int endContextPath = startContextPath + contextPath.length();
            // The base URL (up to and including the context path).
            final String baseURL = serviceURL.substring(0, endContextPath);
            // Everything after that baseURL.
            final String rest = serviceURL.substring(endContextPath);
            if (update) {
               // Request should be proxied to the leader.
               requestURL = baseURL + "/LBS/leader" + rest;
            } else {
               // Request should be load balanced over the services.
               requestURL = baseURL + "/LBS/read" + rest;
            }
         } else {
            // Use the URL as given.
            requestURL = serviceURL;
         }
      }
	   
	   return requestURL;
	   
	}

   /**
    * Return the best URL for error reporting purposes. This is the
    * <code>serviceURL</code> provided to the constructor unless the
    * {@link #getRequestURL(String, boolean)} has been set, in which case that
    * is returned instead.
    */
	public String getBestRequestURL() {
	   
	   String url = requestURL;
	   
	   if(url == null) {
	      
	      url = serviceURL;
	      
	   }
	   
	   return url;
	   
	}
	
	public ConnectOptions(final String serviceURL) {

	   super(serviceURL);
	   
	}

}
